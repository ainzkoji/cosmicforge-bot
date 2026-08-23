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

    # 3. Validate Connection (Proxy Test)
    with patch("app.core.broker_service.DB") as mock_db, \
         patch("app.core.broker_service.decrypt_credentials", return_value=creds), \
         patch("app.core.broker_service._test_broker_connection") as mock_test_conn, \
         patch("app.core.broker_service._log_audit_event"):
         
        mock_conn = MagicMock()
        mock_db.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock retrieval for validation
        mock_conn.execute.side_effect = [
            MagicMock(fetchone=lambda: {"broker_id": "mt4", "environment": "live"}), 
            MagicMock(fetchone=lambda: {"encrypted_blob": b"blob"}),
            MagicMock() # update status
        ]
        
        mock_test_conn.return_value = {"success": True}
        
        res = validate_broker_account(user_id, account_id)
        assert res["success"] is True
        mock_test_conn.assert_called_with("mt4", creds, "live")
