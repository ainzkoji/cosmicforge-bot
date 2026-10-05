"""Read-only execution occupancy over the existing account-scoped authorities.

No research decision/lifecycle can reserve execution capacity. Reservations,
attempts and executor intents retain their own durable lifecycle; this is a
projection, not a second portfolio store.
"""


def reconcile_confirmed_intents(db, account, snapshot):
    """Reconcile acknowledged executor locks, including stopped account bots.

    Use the existing multi-read/age lifecycle against a complete broker snapshot.
    Unsubmitted/unknown CREATE intents are never released by flatness here.
    """
    from app.execution.entry_protection import get_entry_protection
    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute("""SELECT e.* FROM pending_entries e
            JOIN bot_instances b ON b.id=e.bot_id WHERE b.broker_account_id=?
            AND e.state='OPEN_CONFIRMED' AND e.submit_state='SUBMIT_CONFIRMED'""", (account['id'],))]
    if not rows:
        return []
    protection = get_entry_protection(db)
    result = []
    for r in rows:
        positions = [p for p in snapshot['positions'] if p['symbol'] == r['symbol']]
        # Sum is insufficient in hedge mode: opposing positions never prove flat.
        position = next((float(p['positionAmt']) for p in positions if float(p['positionAmt'])), 0.)
        orders = [o for o in snapshot['orders'] if o.get('symbol') == r['symbol']]
        state = protection.reconcile_entry(r['bot_id'], r['symbol'], r['side'], position,
                                                   order_evidence='OPEN' if orders else None)
        result.append({'intent_id':r['id'], 'state':state})
    return result


def durable_occupancy(conn, account_id, now):
    unknown, active = [], []
    rows = conn.execute("""SELECT r.* FROM cati_portfolio_reservations r
        WHERE r.broker_account_id=? AND ((r.status='RESERVED' AND r.expires_at>?)
        OR r.status='RESOLUTION_PENDING') AND (r.mode='PRODUCTION' OR EXISTS
        (SELECT 1 FROM cati_trade_plans p WHERE p.reservation_id=r.reservation_id
         AND p.broker_account_id=r.broker_account_id AND p.mode='LIVE'))""", (account_id, now)).fetchall()
    for r in rows:
        (unknown if r['status'] == 'RESOLUTION_PENDING' else active).append(r['reservation_id'])
    latest = {}
    for r in conn.execute("SELECT * FROM cati_execution_attempts WHERE broker_account_id=? ORDER BY sequence", (account_id,)):
        latest[r['execution_attempt_id']] = r
    for r in latest.values():
        if r['status'] in {'PENDING_SUBMIT', 'SUBMIT_UNKNOWN'}:
            unknown.append(r['execution_attempt_id'])
    for r in conn.execute("""SELECT e.id,e.state,e.submit_state FROM pending_entries e
        JOIN bot_instances b ON b.id=e.bot_id WHERE b.broker_account_id=? AND e.state!='OPEN_FAILED'""", (account_id,)):
        (unknown if r['submit_state'] == 'SUBMIT_UNKNOWN' else active).append(r['id'])
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_protection'").fetchone():
        unknown.extend(r[0] for r in conn.execute("SELECT client_id FROM cati_production_protection WHERE account_id=? AND response IS NULL", (account_id,)))
    return {'active': bool(unknown or active), 'state': 'UNKNOWN' if unknown else 'RESERVED' if active else 'AVAILABLE',
            'reason': 'ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED' if unknown else 'EXECUTION_PORTFOLIO_OVERLAP' if active else None,
            'durable_ids': unknown + active}


def execution_portfolio(db, account, snapshot, execution_history, now):
    with db.connect() as conn:
        result = durable_occupancy(conn, account['id'], now)
    positions = [p for p in snapshot['positions'] if abs(float(p['positionAmt'])) > 0]
    entries = [o for o in snapshot['orders'] if not o.get('reduceOnly') and not o.get('closePosition')]
    protective = [o for o in snapshot['orders'] if o not in entries]
    # Durable lineage is resolved by fresh broker order + position read-back.
    # An unanswered historical CREATE is ambiguous even when this symbol looks flat.
    unanswered = [h['trade_plan_id'] for h in execution_history
                  if not h['order']['answered'] or not h['position']['answered']]
    open_orders = [h for h in execution_history if h['order'].get('status') in {'NEW','PARTIALLY_FILLED'}]
    if unanswered:
        result.update(active=True, state='UNKNOWN', reason='ACCOUNT_SUBMIT_OUTCOME_UNRESOLVED')
    elif positions or entries or open_orders or any(h['position']['quantity'] > 0 for h in execution_history):
        has_position = bool(positions or any(h['position']['quantity'] > 0 for h in execution_history))
        result.update(active=True, state='POSITION_OPEN' if has_position else 'ORDER_OPEN', reason='ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE')
    elif protective:
        result.update(active=True, state='UNKNOWN', reason='ACCOUNT_WIDE_ORPHAN_ORDER_ACTIVE')
    result.update(broker_positions=len(positions), entry_orders=len(entries), protection_orders=len(protective),
                  active_lineage=[h['cati_decision_id'] for h in execution_history
                                  if not h['position']['answered'] or h['position']['quantity'] > 0 or h in open_orders])
    return result
