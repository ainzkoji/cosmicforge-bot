# Manual Verification for MT4/MT5 Bridge

This guide explains how to manually verify the MT4/MT5 connection flow using the provided mock bridge script.

## Prerequisities

- Python 3.8+ installed
- `fastapi`, `uvicorn` installed (`pip install fastapi uvicorn`)

## Steps

1. **Start the Mock Bridge**
   Run the following command in the project root:
   ```bash
   python mock_bridge.py
   ```
   This will start a mock server at `http://0.0.0.0:8443` simulating an MT5 bridge.

2. **Open the Frontend**
   Navigation to the broker connection page in your browser (e.g., `http://localhost:5173/connect`).

3. **Select Broker**
   - Click on **MetaTrader 5** (or MetaTrader 4).
   - Verify that the **Bridge Requirement** info box appears with the warning about VPS/Port Forwarding.

4. **Enter Credentials**
   - **Bridge URL**: `http://127.0.0.1:8443`
   - **API Token**: `valid-token` (any string will work with the mock, but must not be empty)
   - **Account Label**: `Test MT5`

5. **Test Connection**
   - Click the **Test Connection** button.
   - You should see a success alert: "Connection successful! You can now proceed to save."
   - The mock bridge console should show a request to `/health`.

6. **Save & Connect**
   - Click **Save & Connect**.
   - The mock bridge console should show requests to `/instrument` and `/balance`.
   - The frontend should redirect to the success or permissions step.

## Troubleshooting

- If "Test Connection" fails with "Network Error" or similar, ensure the frontend can reach the mock bridge.
- If using Docker or a VM, you might need to use the host IP instead of `127.0.0.1`.
- Check the console logs of `mock_bridge.py` to see if requests are reaching it.
