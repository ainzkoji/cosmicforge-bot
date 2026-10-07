"""Durable reduce-only CLOSE, with broker read-back before any restart action.

Bounded retry (audit: the fail-safe close used to be single-shot -- one failed
POST left the position open forever behind CLOSE_SUBMIT_OUTCOME_UNKNOWN):

* Every attempt is a ``reduceOnly`` MARKET order for the position as the broker
  reports it now. A reduce-only order can only shrink the position and is
  refused by the venue once it is flat, so a duplicate cannot open or increase
  exposure. That -- and only that -- is why a close may be re-attempted while
  an ENTRY never is.
* The durable row is written BEFORE the POST and carries every attempt. A new
  attempt (new deterministic client id ``<base>-r<n>``) starts only after the
  previous one is PROVEN not to be working at the broker:
    - the venue refused it (HTTP 4xx with a definitive error code), or
    - the venue reports it terminal without having flattened the position, or
    - the venue authoritatively reports the id as non-existent AND
      ``ABSENT_RESOLUTION_MS`` has passed since it was sent.
  A timeout / 5xx / connection error alone never starts a new attempt.
* At most ``MAX_CLOSE_ATTEMPTS`` attempts. After that the existing fail-closed
  error is raised and an operator alert is recorded.
* Two budgets. A venue answer that proves the REQUEST was not processed for a
  reason unrelated to the order (clock skew -1021, request / order rate limits
  -1003 / -1015, or an HTTP 429 / 418 send later proven absent) is "transient":
  such attempts are spaced out (``TRANSIENT_RETRY_BACKOFF_MS``) and counted
  against ``MAX_TRANSIENT_CLOSE_ATTEMPTS`` instead of the cap above, so three
  bad cycles in a row cannot exhaust the intent. Every other failed attempt
  counts against ``MAX_CLOSE_ATTEMPTS`` exactly as before. Both counters are
  derived from the attempt list, which is persisted before each POST.
"""
import hashlib
import json
import logging
import time

logger = logging.getLogger(__name__)

#: Close orders that may ever be sent for one (account, lineage, symbol), not
#: counting "transient" attempts (below).
MAX_CLOSE_ATTEMPTS = 3
#: Separate, larger cap for attempts the venue did not process for a transient
#: reason (see the module docstring). At most MAX_CLOSE_ATTEMPTS - 1 +
#: MAX_TRANSIENT_CLOSE_ATTEMPTS orders can therefore ever be sent for one
#: intent, so the ``-r<n>`` suffix stays a single digit (36-character limit).
MAX_TRANSIENT_CLOSE_ATTEMPTS = 6
#: Minimum time between a transient attempt and the next one, by how many
#: transient attempts the intent has had (the last value repeats).
TRANSIENT_RETRY_BACKOFF_MS = (30_000, 60_000, 120_000)
#: How long after a CREATE was sent its absence at the broker is believed.
#: Signed requests carry recvWindow=5000 ms: the venue refuses one that reaches
#: it later than that, so an order that does not exist 60 s (two runtime
#: cycles) after it was sent cannot appear afterwards.
ABSENT_RESOLUTION_MS = 60_000
NAKED_POSITION_OPERATOR_REQUIRED = 'NAKED_POSITION_OPERATOR_REQUIRED'
#: A close order the venue reports FILLED while the position is still open is
#: alerted once it has been in that state this long (a position read can lag a
#: fill for a moment; a whole absent-resolution window is not a lag).
FILLED_NOT_FLAT_ALERT_MS = ABSENT_RESOLUTION_MS
#: A reduce-only MARKET close that is still working (NEW / partially filled)
#: this long after it was sent is alerted: nothing else will be sent while it
#: may still be live, so a human has to look.
WORKING_ALERT_MS = 300_000
#: Identity prefix of an operator emergency flatten (no trade-plan lineage).
EMERGENCY_IDENTITY_PREFIX = 'EMERGENCY|'
#: Attempt outcomes that prove the attempt's order is not working at the broker.
FAILED_OUTCOMES = ('REJECTED', 'ABSENT', 'TERMINAL_UNFILLED')
_TERMINAL_UNFILLED = ('CANCELED', 'CANCELLED', 'EXPIRED', 'EXPIRED_IN_MATCH', 'REJECTED')


def close_client_id(account, identity, symbol):
    """The durable row key (and attempt-0 client order id) of one close intent."""
    return 'CFCLOSE' + hashlib.sha256((account+identity+symbol).encode()).hexdigest()[:24]


def attempt_client_id(base, attempt):
    """Deterministic per-attempt id. Attempt 0 keeps the historical id, so rows
    written before retries existed are read back unchanged. 31 + 3 characters,
    inside the venue's 36-character ``[.A-Z:/a-z0-9_-]`` client id limit."""
    return base if not attempt else f'{base}-r{int(attempt)}'


def operator_alert(db, account, symbol, key, detail, summary='automatic close/protection exhausted'):
    """Record one CRITICAL operator alert on the existing alert store. The
    store de-duplicates on the key while the alert is unacknowledged, so a
    condition that persists every cycle cannot flood it. Never raises."""
    logger.critical('[%s] account=%s symbol=%s %s', NAKED_POSITION_OPERATOR_REQUIRED, account, symbol, detail)
    try:
        from app.ops.multi_asset_alerts import MultiAssetAlert, emit
        emit(db, [MultiAssetAlert(NAKED_POSITION_OPERATOR_REQUIRED, 'CRITICAL', NAKED_POSITION_OPERATOR_REQUIRED,
            f'naked:{account}:{key}', f'{symbol}: {summary}; manual action required',
            broker_account_id=account, symbol=symbol, details=dict(detail))])
    except Exception:  # alerting must never mask the fail-closed error
        logger.exception('[%s] alert could not be recorded', NAKED_POSITION_OPERATOR_REQUIRED)


def _attempts(document, cid, first_seen_at=None):
    """``(attempts, legacy)``. A row written before retries existed carries no
    attempt list: its single order is attempt 0 (same client id).

    ``first_seen_at`` (rollout safety): the first time such a legacy row is
    seen, attempt 0 is timed from NOW, not from the row's old timestamp.
    Otherwise every legacy PENDING row whose order is absent would look
    "proven absent long ago" on the first cycle after deploy and immediately
    fire a reduce-only market close of whatever is open on that symbol. Timed
    from now, the normal read-back and the full ``ABSENT_RESOLUTION_MS`` window
    apply from deploy time before anything can be sent."""
    attempts = document.get('attempts')
    if isinstance(attempts, list) and attempts:
        return attempts, False
    attempts = [{'attempt': 0, 'client_order_id': cid, 'request': document.get('request'),
                 'requested_at': document.get('requested_at') if first_seen_at is None else first_seen_at,
                 'outcome': 'UNKNOWN'}]
    if first_seen_at is not None:
        attempts[0].update(legacy=True, legacy_requested_at=document.get('requested_at'))
    document['attempts'] = attempts
    return attempts, True


def _transient(attempt):
    """True for an attempt PROVEN not to exist that the venue did not process
    for a transient reason (it then counts against the transient budget)."""
    return bool(attempt.get('transient')) and attempt.get('outcome') in ('REJECTED', 'ABSENT')


def _save(db, account, cid, document, status, expected=None):
    """Persist the row. With ``expected`` (the document text that was read) it
    is a compare-and-swap, so two processes can never both claim an attempt."""
    with db.connect() as c:
        if expected is None:
            return c.execute('UPDATE cati_production_closes SET status=?,document=? WHERE account_id=? AND client_id=?',
                             (status, json.dumps(document), account, cid)).rowcount
        return c.execute('UPDATE cati_production_closes SET status=?,document=? WHERE account_id=? AND client_id=? '
                         'AND document=?', (status, json.dumps(document), account, cid, expected)).rowcount


def close_position(client, symbol, trace=None):
    """Close ``symbol`` under the client's current intent identity (see the
    module docstring). Returns the confirmed close order, or
    ``{'status': 'no_position'}``; raises while the close is not confirmed.

    ``trace`` (optional dict) receives what THIS call did, for a caller that
    must report it precisely (the operator flatten):

    * ``posted``        a close order was handed to the transport by this call
    * ``acknowledged``  ...and the venue acknowledged it (the POST returned)
    * ``dead``          ...but the venue reports that order terminal, unfilled
    * ``not_working``   this call sent nothing AND the intent's latest attempt is
                        PROVEN not to be working at the venue (refused, absent,
                        or terminal) -- never set while it may still be live
    * ``state``         a precise reason for the failure
    """
    from shared_lib.core.production import order_submission_gate
    from app.trading_intelligence.integration.residual_prospective import owner_current
    from .demo_transport_smoke import lookup
    trace = {} if trace is None else trace
    db = client._production_db
    account = client._production_account_id
    identity = getattr(client,'_production_intent_identity',None)
    if not identity or not owner_current(db):
        raise ValueError('PRODUCTION_CLOSE_AUTHORITY_REQUIRED')
    if not order_submission_gate(client.broker_environment)['enabled']:
        raise ValueError(order_submission_gate(client.broker_environment)['reason'])
    from .fill_resolution import RATE_LIMIT_STATUSES, TRANSIENT_NOT_PROCESSED_CODES, definitive_rejection, venue_error
    cid = close_client_id(account,identity,symbol)
    with db.connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS cati_production_closes (
            account_id TEXT,client_id TEXT,symbol TEXT,identity TEXT,status TEXT,
            document TEXT,PRIMARY KEY(account_id,client_id))''')
        prior = c.execute('SELECT * FROM cati_production_closes WHERE account_id=? AND client_id=?',(account,cid)).fetchone()
    # Taken BEFORE the reads below: every "has enough time passed" test in this
    # function therefore errs on the side of waiting longer.
    now = int(time.time()*1000)
    order = None
    stored = prior['document'] if prior else None
    document = json.loads(stored) if prior else {}
    attempts = []
    if prior:
        closed = prior['status'] == 'CLOSED'
        attempts, legacy = _attempts(document, cid, None if closed else now)
        if legacy and not closed:
            # First sighting of a row from before attempts were recorded: persist
            # the attempt metadata, timed from now (see _attempts). Nothing can be
            # sent for it before a full absent-resolution window from here.
            logger.warning('[PRODUCTION_CLOSE] legacy close row adopted: account=%s client_id=%s symbol=%s identity=%s '
                           'status=%s -- its absent-resolution window starts now; nothing is sent before read-back',
                           account, cid, symbol, identity, prior['status'])
            if not _save(db,account,cid,document,prior['status'],expected=stored):
                raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
            stored = json.dumps(document)
    if prior and attempts[-1].get('outcome') not in FAILED_OUTCOMES:
        # Read the outstanding attempt back before anything else.
        current = attempts[-1]
        order = lookup(client,symbol,current['client_order_id'])
        if not order:
            # The venue says this id does not exist. Believed only once the
            # request can no longer arrive; until then the outcome is unknown.
            # A row already confirmed CLOSED is never reopened by a failed read.
            sent = current.get('requested_at')
            if prior['status'] == 'CLOSED' or sent is None or now - int(sent) < ABSENT_RESOLUTION_MS:
                trace['state'] = 'PRIOR_ATTEMPT_OUTCOME_UNKNOWN'
                raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
            current.update(outcome='ABSENT',resolved_at=now)
            if not _save(db,account,cid,document,prior['status'],expected=stored):
                raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
            stored = json.dumps(document)
            order = None
        elif str(order.get('status')).upper() in _TERMINAL_UNFILLED and prior['status'] != 'CLOSED':
            # Terminal at the broker without closing the position: this order
            # is finished, whatever remains needs a new reduce-only attempt.
            current.update(outcome='TERMINAL_UNFILLED',order=order,resolved_at=now)
            if not _save(db,account,cid,document,prior['status'],expected=stored):
                raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
            stored = json.dumps(document)
            order = None
    if order is None:
        # Reached only when no attempt can be working at the broker: there is
        # none yet, or the latest one is PROVEN refused / absent / terminal.
        amount = float(client.get_position_amt(symbol))
        if not amount:
            if prior and prior['status'] != 'CLOSED':
                # No attempt is working at the broker and the position is flat
                # (closed by its native protection or by the operator). The
                # intent is retired; it carries no close order, so lineage
                # closure still needs real exit fills (see reconcile_executions).
                document.update(retired_flat_at=now,final_position=0)
                _save(db,account,cid,document,'CLOSED',expected=stored)
            return {'status':'no_position','symbol':symbol}
        transient = sum(1 for a in attempts if _transient(a))
        if len(attempts) - transient >= MAX_CLOSE_ATTEMPTS or transient >= MAX_TRANSIENT_CLOSE_ATTEMPTS:
            # Fail closed as before, and tell a human: this position is open
            # and the engine has no automatic way left to close it.
            operator_alert(db,account,symbol,cid,{'identity':identity,'attempts':len(attempts),
                'transient_attempts':transient,'position_amt':amount,'outcomes':[a.get('outcome') for a in attempts],
                'venue_codes':[a.get('venue_code') for a in attempts]})
            trace.update(state='CLOSE_ATTEMPTS_EXHAUSTED',not_working=True)
            raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
        if attempts and _transient(attempts[-1]):
            # The venue did not process the previous request (clock skew / rate
            # limit). Sending again is safe -- that order does not exist -- but
            # not in a tight loop: wait 30 s, 60 s, then 120 s between tries.
            wait = TRANSIENT_RETRY_BACKOFF_MS[min(transient,len(TRANSIENT_RETRY_BACKOFF_MS))-1]
            since = attempts[-1].get('requested_at') or attempts[-1].get('resolved_at') or now
            if now - int(since) < wait:
                trace.update(state='CLOSE_RETRY_BACKOFF',not_working=True)
                raise ValueError('CLOSE_RETRY_BACKOFF')
        attempt_cid = attempt_client_id(cid,len(attempts))
        # reduceOnly: this order can only shrink the position, never open one.
        request = {'symbol':symbol,'side':'SELL' if amount > 0 else 'BUY','type':'MARKET',
                   'quantity':abs(amount),'reduceOnly':'true','newClientOrderId':attempt_cid}
        # Stamped here -- after every pre-POST read (each can wait on a rate
        # limit), immediately before the durable claim and the POST -- so the
        # absent-resolution window is measured from the real send time.
        sent_at = int(time.time()*1000)
        current = {'attempt':len(attempts),'client_order_id':attempt_cid,'request':dict(request),
                   'requested_at':sent_at,'outcome':'UNKNOWN'}
        attempts.append(current)
        document.update(request=dict(request),requested_at=sent_at,attempts=attempts)
        # The durable claim precedes the POST. INSERT OR IGNORE / compare-and-swap
        # guarantee a single claimant per attempt.
        if prior:
            claimed = _save(db,account,cid,document,'PENDING',expected=stored)
        else:
            with db.connect() as c:
                claimed = c.execute("INSERT OR IGNORE INTO cati_production_closes VALUES(?,?,?,?,'PENDING',?)",
                    (account,cid,symbol,identity,json.dumps(document))).rowcount
        if not claimed:
            raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
        stored = json.dumps(document)
        # Never cancel working native protection before a confirmed flat close.
        trace['posted'] = True
        try:
            response = client._signed_post('/fapi/v1/order',params=request)
        except Exception as exc:
            # Only THIS exception (and its explicit causes) is classified: the
            # close often runs while another error is being handled.
            code = definitive_rejection(exc)
            if code is not None:
                # The venue refused the order: it does not exist. Recording that
                # is what lets the NEXT call start a new attempt. Anything else
                # (timeout, 5xx, dropped connection) stays UNKNOWN.
                current.update(outcome='REJECTED',venue_code=code,resolved_at=int(time.time()*1000))
                if code in TRANSIENT_NOT_PROCESSED_CODES:
                    current['transient'] = True
                _save(db,account,cid,document,'PENDING',expected=stored)
                trace['state'] = 'CLOSE_ORDER_REJECTED_BY_VENUE'
            else:
                status, venue_code = venue_error(exc)
                if status in RATE_LIMIT_STATUSES:
                    # Rate limited (HTTP 429 / 418). The outcome stays UNKNOWN
                    # and is resolved by read-back like any other; the mark only
                    # decides which budget the attempt counts against IF it is
                    # later proven absent.
                    current.update(transient=True,venue_status=status,venue_code=venue_code)
                    _save(db,account,cid,document,'PENDING',expected=stored)
                trace['state'] = 'CLOSE_SUBMIT_OUTCOME_UNKNOWN'
            raise
        # The venue answered the POST without an error (the transport's -4130
        # "duplicate" marker is not an acknowledgement of this order).
        trace['acknowledged'] = isinstance(response,dict) and response.get('orderId') != 'DUPLICATE_4130'
        order = lookup(client,symbol,attempt_cid)
        if not order:
            trace['state'] = 'CLOSE_ORDER_NOT_YET_VISIBLE'
            raise ValueError('CLOSE_SUBMIT_OUTCOME_UNKNOWN')
        if str(order.get('status')).upper() in _TERMINAL_UNFILLED:
            current.update(outcome='TERMINAL_UNFILLED',order=order,resolved_at=int(time.time()*1000))
            _save(db,account,cid,document,'PENDING',expected=stored)
            trace.update(state='CLOSE_ORDER_TERMINAL_UNFILLED',dead=True)
            raise ValueError('CLOSE_FILL_OR_FLAT_UNCONFIRMED')
    filled = order.get('status') == 'FILLED'
    if not filled or float(client.get_position_amt(symbol)) != 0:
        latest = attempts[-1]
        if not trace.get('posted'):
            # Read-back of an earlier attempt. A FILLED order is terminal: it
            # can do nothing more, whatever is still open is not being closed.
            trace.update(state='PRIOR_CLOSE_FILLED_POSITION_NOT_FLAT' if filled else 'PRIOR_CLOSE_ORDER_STILL_WORKING',
                         not_working=filled)
        else:
            trace['state'] = 'CLOSE_FILLED_FLAT_CONFIRMATION_PENDING' if filled else 'CLOSE_ORDER_WORKING'
        age = int(time.time()*1000) - int(latest.get('requested_at') or time.time()*1000)
        if (prior is None or prior['status'] != 'CLOSED') and age >= (FILLED_NOT_FLAT_ALERT_MS if filled else WORKING_ALERT_MS):
            # Nothing automatic will happen from here: a filled close cannot
            # close more, and no new close is sent while one may be working.
            reason = 'CLOSE_FILLED_POSITION_NOT_FLAT' if filled else 'CLOSE_ORDER_STILL_WORKING'
            operator_alert(db,account,symbol,cid,{'identity':identity,'reason':reason,'attempts':len(attempts),
                'client_order_id':latest.get('client_order_id'),'order_status':str(order.get('status')),'age_ms':age},
                summary='close order filled but the position is still open' if filled
                else 'close order still unfilled at the venue')
        raise ValueError('CLOSE_FILL_OR_FLAT_UNCONFIRMED')
    attempts[-1].update(outcome='FILLED')
    document.update(order=order,confirmed_at=int(time.time()*1000),final_position=0)
    with db.connect() as c:
        c.execute("UPDATE cati_production_closes SET status='CLOSED',document=? WHERE account_id=? AND client_id=?",
                  (json.dumps(document),account,cid))
        plan = None if identity.startswith(EMERGENCY_IDENTITY_PREFIX) else c.execute(
            'SELECT bot_instance_id,side FROM cati_trade_plans WHERE broker_account_id=? AND trade_plan_id=?',
            (account,identity.split('|')[0])).fetchone()
    if plan:
        from .entry_protection import get_entry_protection
        get_entry_protection(db).mark_closed(plan['bot_instance_id'],symbol,plan['side'])
    return order
