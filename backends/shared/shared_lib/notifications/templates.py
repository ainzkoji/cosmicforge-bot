from typing import Dict, Any, Tuple
import json

class TemplateRegistry:
    """
    Simple template registry for notifications.
    Renders Subject, Body (Text), and Body (HTML).
    """

    @staticmethod
    def render(template_id: str, payload: Dict[str, Any]) -> Tuple[str, str, str]:
        """
        Returns (subject, body_text, body_html)
        """
        renderer = getattr(TemplateRegistry, f"_render_{template_id}", None)
        if not renderer:
            return TemplateRegistry._render_default(template_id, payload)
        
        return renderer(payload)

    @staticmethod
    def _render_default(template_id: str, payload: Dict[str, Any]) -> Tuple[str, str, str]:
        subject = f"Notification: {template_id}"
        text = f"Event: {template_id}\n\nDetails:\n{json.dumps(payload, indent=2)}"
        html = f"<h3>{template_id}</h3><pre>{json.dumps(payload, indent=2)}</pre>"
        return subject, text, html

    @staticmethod
    def _render_TRADE_FILLED(payload: Dict[str, Any]) -> Tuple[str, str, str]:
        details = payload.get("details", {})
        symbol = payload.get("symbol") or details.get("symbol") or "Unknown"
        side = details.get("side", "TRADE")
        qty = details.get("qty", "?")
        price = details.get("price", "?")
        pnl = details.get("realized_pnl")

        subject = f"✅ Trade Filled: {side} {symbol}"
        
        text = (
            f"Trade Executed for {symbol}\n"
            f"Side: {side}\n"
            f"Qty: {qty}\n"
            f"Price: {price}\n"
        )
        if pnl is not None:
             text += f"Realized PnL: {pnl}\n"

        html = f"""
        <h2>Trade Executed</h2>
        <p><strong>{symbol}</strong></p>
        <ul>
            <li>Side: {side}</li>
            <li>Qty: {qty}</li>
            <li>Price: {price}</li>
        """
        if pnl is not None:
            html += f"<li>Realized PnL: <strong>{pnl}</strong></li>"
        html += "</ul>"
        
        return subject, text, html

    @staticmethod
    def _render_ORDER_FAILED(payload: Dict[str, Any]) -> Tuple[str, str, str]:
        details = payload.get("details", {})
        symbol = payload.get("symbol") or "Unknown"
        reason = details.get("error") or details.get("reason") or "Unknown Error"

        subject = f"❌ Order Failed: {symbol}"
        
        text = (
            f"Order Failed for {symbol}\n"
            f"Reason: {reason}\n"
        )

        html = f"""
        <h2 style="color: red;">Order Failed</h2>
        <p><strong>{symbol}</strong></p>
        <p>Reason: {reason}</p>
        """
        return subject, text, html

    @staticmethod
    def _render_RISK_ALERT(payload: Dict[str, Any]) -> Tuple[str, str, str]:
        details = payload.get("details", {})
        msg = details.get("message") or str(payload)
        
        subject = "⚠️ Risk Alert"
        text = f"Risk Warning:\n{msg}"
        html = f"<h2 style='color: orange;'>Risk Warning</h2><p>{msg}</p>"
        return subject, text, html

    @staticmethod
    def _render_SYSTEM_ERROR(payload: Dict[str, Any]) -> Tuple[str, str, str]:
        details = payload.get("details", {})
        err = details.get("error") or "Internal System Error"
        
        subject = "🚨 System Error"
        text = f"Critical Error Detected:\n{err}"
        html = f"<h2 style='color: red;'>System Error</h2><p>{err}</p>"
        return subject, text, html
