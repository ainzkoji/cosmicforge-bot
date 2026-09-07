# Add these methods to BinanceFuturesClient

def place_order(self, req):
    """
    Unified interface for placing orders.
    Maps OrderRequest to Binance API.
    """
    from app.models.unified_trading import UnifiedOrder, OrderStatus, Side
    
    # Set leverage if specified
    if req.leverage:
        self.set_leverage(req.symbol, int(req.leverage))
    
    # Build params
    params = {
        "symbol": req.symbol,
        "side": req.side.value.upper(),
        "type": req.type.upper(),
        "quantity": float(req.qty),
    }
    
    if req.reduce_only:
        params["reduceOnly"] = "true"
    
    # Execute
    response = self._request("POST", "/fapi/v1/order", params=params, signed=True)
    
    # Map to UnifiedOrder
    return UnifiedOrder(
        client_order_id=str(response.get("clientOrderId", "")),
        broker_order_id=str(response.get("orderId", "")),
        symbol=req.symbol,
        side=req.side,
        type=req.type,
        qty_ordered=req.qty,
        qty_filled=Decimal(response.get("executedQty", "0")),
        avg_fill_price=Decimal(response.get("avgPrice") or response.get("price", "0")),
        status=OrderStatus.FILLED if response.get("status") == "FILLED" else OrderStatus.NEW,
        timestamp=int(response.get("updateTime", response.get("transactTime", 0))),
        reduce_only=req.reduce_only
    )

def place_protection(self, req):
    """
    Place stop-loss and take-profit orders.
    """
    from app.models.unified_trading import ProtectionResult, Side
    
    result = ProtectionResult(status="initiated")
    exit_side = "SELL" if req.position_side == Side.BUY else "BUY"
    
    try:
        if req.sl_price:
            sl_params = {
                "symbol": req.symbol,
                "side": exit_side,
                "type": "STOP_MARKET",
                "stopPrice": float(req.sl_price),
                "closePosition": "true"
            }
            sl_response = self._request("POST", "/fapi/v1/order", params=sl_params, signed=True)
            result.sl_order_id = str(sl_response["orderId"])
        
        if req.tp_price:
            tp_params = {
                "symbol": req.symbol,
                "side": exit_side,
                "type": "TAKE_PROFIT_MARKET",
                "stopPrice": float(req.tp_price),
                "closePosition": "true"
            }
            tp_response = self._request("POST", "/fapi/v1/order", params=tp_params, signed=True)
            result.tp_order_id = str(tp_response["orderId"])
        
        result.status = "success"
    except Exception as e:
        result.status = "failed"
        result.error = str(e)
    
    return result

def get_positions(self):
    """
    Get all open positions.
    """
    from app.models.unified_trading import UnifiedPosition, Side, PositionMode
    
    raw = self.position_risk()
    positions = []
    
    for p in raw:
        amt = Decimal(p["positionAmt"])
        if amt == 0:
            continue
        
        side = Side.BUY if amt > 0 else Side.SELL
        qty = abs(amt)
        
        positions.append(UnifiedPosition(
            symbol=p["symbol"],
            broker_id="binance",
            side=side,
            quantity=qty,
            entry_price=Decimal(p["entryPrice"]),
            current_price=Decimal(p.get("markPrice", p["entryPrice"])),
            unrealized_pnl=Decimal(p["unRealizedProfit"]),
            realized_pnl=Decimal("0"),
            margin_used=Decimal(p.get("initialMargin", "0")),
            leverage=Decimal(p["leverage"]),
            mode=PositionMode.ONE_WAY,
            timestamp=int(time.time() * 1000)
        ))
    
    return positions
