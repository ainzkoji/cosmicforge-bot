"""Production account authority: DEMO consent, LIVE promotion, shared kill switch."""
from shared_lib.broker.environment import normalize_environment
from shared_lib.broker.auto_trading import authorization
from shared_lib.core.production import order_submission_gate
from .promotion import GovernanceAuthority


class AccountExecutionAuthority(GovernanceAuthority):
    def __init__(self, db, account_scope):
        super().__init__(db)
        self.db, self.account_scope = db, tuple(account_scope)

    def authorize_entry(self, plan):
        if (plan.user_id, plan.broker_account_id) != self.account_scope:
            return False, "EXECUTION_ACCOUNT_SCOPE_MISMATCH"
        if self.gov.kill_switch_on(scope=plan.broker_account_id):
            return False, "CATI_NEW_ENTRY_KILL_SWITCH"
        with self.db.connect() as c:
            row = c.execute("SELECT * FROM broker_accounts WHERE id=? AND user_id=?",
                            (plan.broker_account_id, plan.user_id)).fetchone()
            if not row:
                return False, "BROKER_ACCOUNT_OWNERSHIP_MISMATCH"
            account = dict(row)
            bots = [dict(r) for r in c.execute("SELECT * FROM bot_instances WHERE broker_account_id=? AND status='active'",
                                              (plan.broker_account_id,))]
            if len(bots) != 1 or bots[0]['id'] != plan.bot_instance_id or bots[0]['user_id'] != plan.user_id:
                return False, "ACCOUNT_EXECUTION_OWNER_AMBIGUOUS"
            if normalize_environment(account['environment']).value.upper() != plan.environment:
                return False, "BROKER_ENVIRONMENT_MISMATCH"
            if str(account['status']).lower() not in {'connected', 'active'}:
                return False, "BROKER_ACCOUNT_NOT_CONNECTED"
            consent = authorization(c, account, bots)
        if not consent['enabled']:
            return False, consent['reason']
        gate = order_submission_gate(plan.environment)
        if not gate['enabled']:
            return False, gate['reason']
        from app.trading_intelligence.integration.residual_prospective import owner_current
        if not owner_current(self.db):
            return False, "CANONICAL_RUNTIME_LEASE_REQUIRED"
        if plan.environment == 'DEMO':
            # Only the connected DEMO account bypasses real-capital commissioning.
            # Signal, capabilities, risk, reservation and idempotency remain at
            # the common execution boundary, including its final pre-CREATE check.
            return True, "ACCOUNT_AUTHORIZED_DEMO_EXECUTION"
        return super().authorize_entry(plan)
