from unittest.mock import patch, MagicMock
from app.core.broker_service import submit_broker_credentials, create_broker_account_draft, validate_broker_account

def test_mt_broker_flow():
    user_id = "test_user"
    broker_id = "mt4"
    market_type = "forex"
    
    # 1. Draft Creation
    with patch("app.core.broker_service.DB") as mock_db:
        mock_conn = MagicMock()
        mock_db.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock validation queries
        mock_conn.execute.side_effect = [
            MagicMock(fetchone=lambda: None), # check existing draft
            MagicMock(fetchone=lambda: [0]),  # check count
            MagicMock()                       # insert
        ]
        
        # Mock subscription
        with patch("app.core.billing_service.get_user_subscription", return_value={"entitlements": {"max_brokers": 5}}):
            account_id = create_broker_account_draft(user_id, broker_id, market_type)
            assert account_id.startswith("brk_")

    # 2. Submit Credentials
    creds = {
        "bridge_url": "https://test-bridge.com",
        "bridge_token": "secret_token",
        "environment": "live"
    }
    
    with patch("app.core.broker_service.DB") as mock_db, \
         patch("app.core.broker_service.encrypt_credentials", return_value=b"encrypted"), \
         patch("app.core.broker_service.mask_credentials", return_value="******"), \
         patch("app.core.broker_service._log_audit_event"):
         
        mock_conn = MagicMock()
        mock_db.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock ownership check logic
        mock_conn.execute.side_effect = [
            MagicMock(fetchone=lambda: {"broker_id": "mt4"}), # check broker_id
            MagicMock(fetchone=lambda: {"id": account_id}),   # verify ownership
            MagicMock(), # insert creds
            MagicMock()  # update account
        ]

        result = submit_broker_credentials(user_id, account_id, creds)
        assert result is True

    # 3. Validate Connection (Proxy Test) -- credentials come from the canonical resolver
    #    (shared_lib.broker.resolver.resolve_broker_auth, v2 with v1 fallback), not a local decrypt.
    from types import SimpleNamespace

    auth = SimpleNamespace(extra={"bridge_url": creds["bridge_url"], "bridge_token": creds["bridge_token"]},
                           api_key=None, api_secret=None, base_url=None,
                           environment=SimpleNamespace(value="live"), credential_version=1)
    permission = {"decision": "ACCEPTED", "message": "", "evidence": {"permissions": {"TRADE": True}}}
    with patch("app.core.broker_service.get_db") as mock_get_db,          patch("shared_lib.broker.resolver.resolve_broker_auth", return_value=auth),          patch("app.core.broker_service._evaluate_key_permissions", return_value=permission),          patch("app.core.broker_service._test_broker_connection") as mock_test_conn,          patch("app.core.broker_service._log_audit_event"):

        mock_conn = MagicMock()
        mock_get_db.return_value.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = {"broker_id": "mt4"}
        mock_test_conn.return_value = {"success": True}

        res = validate_broker_account(user_id, account_id)
        assert res["success"] is True and res["status"] == "connected"
        expected = {**auth.extra, "api_key": None, "api_secret": None, "base_url": None}
        mock_test_conn.assert_called_with("mt4", expected, "live")
