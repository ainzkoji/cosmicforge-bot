"""User authorization is scoped to one account, environment and bot owner."""
from shared_lib.broker.environment import normalize_environment


def ensure(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS broker_auto_trading(account_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,bot_instance_id TEXT NOT NULL,environment TEXT NOT NULL,enabled INTEGER NOT NULL,authorized_at TEXT NOT NULL)")


def set_authorization(conn, *, account_id, user_id, bot_instance_id, enabled, now):
    ensure(conn)
    account = conn.execute("SELECT user_id,environment FROM broker_accounts WHERE id=?", (account_id,)).fetchone()
    bot = conn.execute("SELECT user_id,broker_account_id FROM bot_instances WHERE id=?", (bot_instance_id,)).fetchone()
    if account is None or bot is None or account["user_id"] != user_id or bot["user_id"] != user_id or bot["broker_account_id"] != account_id:
        raise ValueError("BROKER_ACCOUNT_ACCESS_DENIED")
    conn.execute("INSERT OR REPLACE INTO broker_auto_trading VALUES(?,?,?,?,?,?)",
        (account_id, user_id, bot_instance_id, normalize_environment(account["environment"]).value, int(enabled), now))


def authorization(conn, account, bots):
    ensure(conn)
    if len(bots) != 1:
        return {"enabled": False, "state": "AUTO_TRADING_DISABLED", "reason":
                "ACCOUNT_EXECUTION_OWNER_AMBIGUOUS" if bots else "AUTO_TRADING_DISABLED"}
    if bots[0]["user_id"] != account["user_id"]:
        return {"enabled": False, "state": "AUTO_TRADING_DISABLED", "reason": "BROKER_ACCOUNT_OWNERSHIP_MISMATCH"}
    row = conn.execute("SELECT * FROM broker_auto_trading WHERE account_id=?", (account["id"],)).fetchone()
    environment = normalize_environment(account["environment"]).value
    if row is not None:
        enabled = bool(row["enabled"] and (row["user_id"], row["bot_instance_id"], row["environment"]) ==
                       (account["user_id"], bots[0]["id"], environment))
    else:
        # Current proving phase permits existing active DEMO assignments only.
        enabled = environment == "demo"
    return {"enabled": enabled, "state": "AUTO_TRADING_ENABLED" if enabled else "AUTO_TRADING_DISABLED",
            "reason": None if enabled else "USER_AUTHORIZATION_REQUIRED" if environment == "live" else "AUTO_TRADING_DISABLED"}
