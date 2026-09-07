//+------------------------------------------------------------------+
//|                                                     MTBridge.mq4 |
//|                                      MetaTrader 4 Bridge for Bot |
//+------------------------------------------------------------------+
#property copyright "CosmicForge"
#property link      ""
#property version   "1.00"
#property strict

// ZeroMQ library - requires mql-zmq
#include <Zmq/Zmq.mqh>

// Global ZMQ context and socket
Context context("mt-bridge");
Socket socket(context, ZMQ_REP);

// Configuration
extern string ZMQ_PORT = "5555";
extern string API_SECRET = "";

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   Print("MTBridge EA starting...");
   
   string endpoint = "tcp://127.0.0.1:" + ZMQ_PORT;
   if(!socket.bind(endpoint))
   {
      Print("ERROR: Failed to bind ZMQ socket on ", endpoint);
      return(INIT_FAILED);
   }
   
   Print("MTBridge initialized. Listening on ", endpoint);
   Print("Account: ", AccountNumber());
   Print("Server: ", AccountServer());
   
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   Print("MTBridge EA shutting down...");
   socket.unbind("tcp://127.0.0.1:" + ZMQ_PORT);
   socket.disconnect("tcp://127.0.0.1:" + ZMQ_PORT);
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
{
   ZmqMsg request;
   
   if(socket.recv(request, true))
   {
      string requestStr = request.getData();
      Print("Received request: ", requestStr);
      
      string response = HandleRequest(requestStr);
      
      ZmqMsg reply(response);
      socket.send(reply);
      
      Print("Sent response: ", StringSubstr(response, 0, 100), "...");
   }
}

//+------------------------------------------------------------------+
//| Main request handler                                             |
//+------------------------------------------------------------------+
string HandleRequest(string jsonRequest)
{
   string action = ExtractJsonField(jsonRequest, "action");
   
   if(action == "")
      return CreateErrorResponse("Missing 'action' field");
   
   if(action == "health")
      return HandleHealth(jsonRequest);
   else if(action == "instruments")
      return HandleInstruments(jsonRequest);
   else if(action == "prices")
      return HandlePrices(jsonRequest);
   else if(action == "klines")
      return HandleKlines(jsonRequest);
   else if(action == "order")
      return HandleOrder(jsonRequest);
   else if(action == "cancel_order")
      return HandleCancelOrder(jsonRequest);
   else if(action == "get_order")
      return HandleGetOrder(jsonRequest);
   else if(action == "positions")
      return HandlePositions(jsonRequest);
   else if(action == "balance")
      return HandleBalance(jsonRequest);
   else
      return CreateErrorResponse("Unknown action: " + action);
}

//+------------------------------------------------------------------+
//| Health check handler                                             |
//+------------------------------------------------------------------+
string HandleHealth(string jsonRequest)
{
   string json = "{";
   json += "\"status\":\"ok\",";
   json += "\"platform\":\"mt4\",";
   json += "\"account\":" + IntegerToString(AccountNumber()) + ",";
   json += "\"server\":\"" + AccountServer() + "\",";
   json += "\"time\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\",";
   json += "\"connected\":" + (IsConnected() ? "true" : "false");
   json += "}";
   
   return json;
}

//+------------------------------------------------------------------+
//| Instruments handler                                              |
//+------------------------------------------------------------------+
string HandleInstruments(string jsonRequest)
{
   string json = "{\"symbols\":[";
   
   int total = SymbolsTotal(true);
   for(int i = 0; i < total; i++)
   {
      string symbol = SymbolName(i, true);
      
      if(i > 0) json += ",";
      
      json += "{";
      json += "\"symbol\":\"" + symbol + "\",";
      json += "\"description\":\"" + symbol + "\",";
      json += "\"digits\":" + IntegerToString(MarketInfo(symbol, MODE_DIGITS)) + ",";
      json += "\"contract_size\":" + DoubleToStr(MarketInfo(symbol, MODE_LOTSIZE), 2) + ",";
      json += "\"min_lot\":" + DoubleToStr(MarketInfo(symbol, MODE_MINLOT), 2) + ",";
      json += "\"max_lot\":" + DoubleToStr(MarketInfo(symbol, MODE_MAXLOT), 2) + ",";
      json += "\"lot_step\":" + DoubleToStr(MarketInfo(symbol, MODE_LOTSTEP), 2) + ",";
      json += "\"tick_size\":" + DoubleToStr(MarketInfo(symbol, MODE_TICKSIZE), 8) + ",";
      json += "\"tick_value\":" + DoubleToStr(MarketInfo(symbol, MODE_TICKVALUE), 8);
      json += "}";
   }
   
   json += "]}";
   return json;
}

//+------------------------------------------------------------------+
//| Prices handler                                                   |
//+------------------------------------------------------------------+
string HandlePrices(string jsonRequest)
{
   string symbolsStr = ExtractJsonField(jsonRequest, "symbols");
   string symbols[];
   ParseSymbolsArray(symbolsStr, symbols);
   
   string json = "{\"prices\":[";
   
   for(int i = 0; i < ArraySize(symbols); i++)
   {
      string symbol = symbols[i];
      
      if(i > 0) json += ",";
      
      double bid = MarketInfo(symbol, MODE_BID);
      double ask = MarketInfo(symbol, MODE_ASK);
      
      json += "{";
      json += "\"symbol\":\"" + symbol + "\",";
      json += "\"bid\":" + DoubleToStr(bid, MarketInfo(symbol, MODE_DIGITS)) + ",";
      json += "\"ask\":" + DoubleToStr(ask, MarketInfo(symbol, MODE_DIGITS)) + ",";
      json += "\"time\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\"";
      json += "}";
   }
   
   json += "]}";
   return json;
}

//+------------------------------------------------------------------+
//| Klines handler                                                   |
//+------------------------------------------------------------------+
string HandleKlines(string jsonRequest)
{
   string symbol = ExtractJsonField(jsonRequest, "symbol");
   string interval = ExtractJsonField(jsonRequest, "interval");
   int limit = StrToInteger(ExtractJsonField(jsonRequest, "limit"));
   
   if(limit <= 0) limit = 100;
   
   int tf = ParseTimeframe(interval);
   
   string json = "{\"klines\":[";
   
   for(int i = limit - 1; i >= 0; i--)
   {
      int index = i;
      
      if(limit - 1 - i > 0) json += ",";
      
      json += "{";
      json += "\"time\":\"" + TimeToString(iTime(symbol, tf, index), TIME_DATE|TIME_SECONDS) + "\",";
      json += "\"open\":" + DoubleToStr(iOpen(symbol, tf, index), MarketInfo(symbol, MODE_DIGITS)) + ",";
      json += "\"high\":" + DoubleToStr(iHigh(symbol, tf, index), MarketInfo(symbol, MODE_DIGITS)) + ",";
      json += "\"low\":" + DoubleToStr(iLow(symbol, tf, index), MarketInfo(symbol, MODE_DIGITS)) + ",";
      json += "\"close\":" + DoubleToStr(iClose(symbol, tf, index), MarketInfo(symbol, MODE_DIGITS)) + ",";
      json += "\"volume\":" + IntegerToString(iVolume(symbol, tf, index));
      json += "}";
   }
   
   json += "]}";
   return json;
}

//+------------------------------------------------------------------+
//| Order placement handler                                          |
//+------------------------------------------------------------------+
string HandleOrder(string jsonRequest)
{
   string symbol = ExtractJsonField(jsonRequest, "symbol");
   string sideStr = ExtractJsonField(jsonRequest, "side");
   string typeStr = ExtractJsonField(jsonRequest, "type");
   double quantity = StrToDouble(ExtractJsonField(jsonRequest, "quantity"));
   double price = StrToDouble(ExtractJsonField(jsonRequest, "price"));
   double sl = StrToDouble(ExtractJsonField(jsonRequest, "sl"));
   double tp = StrToDouble(ExtractJsonField(jsonRequest, "tp"));
   string clientOrderId = ExtractJsonField(jsonRequest, "client_order_id");
   
   int cmd;
   double orderPrice;
   
   if(typeStr == "market")
   {
      cmd = (sideStr == "buy") ? OP_BUY : OP_SELL;
      orderPrice = (sideStr == "buy") ? Ask : Bid;
   }
   else if(typeStr == "limit")
   {
      cmd = (sideStr == "buy") ? OP_BUYLIMIT : OP_SELLLIMIT;
      orderPrice = price;
   }
   else if(typeStr == "stop")
   {
      cmd = (sideStr == "buy") ? OP_BUYSTOP : OP_SELLSTOP;
      orderPrice = price;
   }
   else
   {
      return CreateErrorResponse("Invalid order type: " + typeStr);
   }
   
   int ticket = OrderSend(symbol, cmd, quantity, orderPrice, 10, sl, tp, clientOrderId, 12345, 0, clrNONE);
   
   if(ticket < 0)
   {
      return CreateErrorResponse("Order failed: " + IntegerToString(GetLastError()));
   }
   
   string json = "{";
   json += "\"order_id\":\"" + IntegerToString(ticket) + "\",";
   json += "\"ticket\":" + IntegerToString(ticket) + ",";
   json += "\"price\":" + DoubleToStr(orderPrice, MarketInfo(symbol, MODE_DIGITS)) + ",";
   json += "\"volume\":" + DoubleToStr(quantity, 2) + ",";
   json += "\"status\":\"filled\"";
   json += "}";
   
   return json;
}

//+------------------------------------------------------------------+
//| Cancel order handler                                             |
//+------------------------------------------------------------------+
string HandleCancelOrder(string jsonRequest)
{
   string orderIdStr = ExtractJsonField(jsonRequest, "order_id");
   int ticket = StrToInteger(orderIdStr);
   
   if(!OrderSelect(ticket, SELECT_BY_TICKET))
   {
      return CreateErrorResponse("Order not found: " + orderIdStr);
   }
   
   if(!OrderDelete(ticket))
   {
      return CreateErrorResponse("Cancel failed: " + IntegerToString(GetLastError()));
   }
   
   return "{\"success\":true,\"order_id\":\"" + orderIdStr + "\"}";
}

//+------------------------------------------------------------------+
//| Get order status handler                                         |
//+------------------------------------------------------------------+
string HandleGetOrder(string jsonRequest)
{
   string orderIdStr = ExtractJsonField(jsonRequest, "order_id");
   int ticket = StrToInteger(orderIdStr);
   
   if(!OrderSelect(ticket, SELECT_BY_TICKET))
   {
      return CreateErrorResponse("Order not found: " + orderIdStr);
   }
   
   string status = "pending";
   if(OrderType() == OP_BUY || OrderType() == OP_SELL)
      status = "filled";
   
   string json = "{";
   json += "\"order_id\":\"" + orderIdStr + "\",";
   json += "\"status\":\"" + status + "\",";
   json += "\"symbol\":\"" + OrderSymbol() + "\",";
   json += "\"side\":\"" + (OrderType() == OP_BUY || OrderType() == OP_BUYLIMIT || OrderType() == OP_BUYSTOP ? "buy" : "sell") + "\",";
   json += "\"quantity\":" + DoubleToStr(OrderLots(), 2) + ",";
   json += "\"price\":" + DoubleToStr(OrderOpenPrice(), 8);
   json += "}";
   
   return json;
}

//+------------------------------------------------------------------+
//| Positions handler                                                |
//+------------------------------------------------------------------+
string HandlePositions(string jsonRequest)
{
   string json = "{\"positions\":[";
   
   int total = OrdersTotal();
   bool first = true;
   
   for(int i = 0; i < total; i++)
   {
      if(!OrderSelect(i, SELECT_BY_POS)) continue;
      
      if(OrderType() != OP_BUY && OrderType() != OP_SELL) continue;
      
      if(!first) json += ",";
      first = false;
      
      json += "{";
      json += "\"ticket\":" + IntegerToString(OrderTicket()) + ",";
      json += "\"symbol\":\"" + OrderSymbol() + "\",";
      json += "\"side\":\"" + (OrderType() == OP_BUY ? "buy" : "sell") + "\",";
      json += "\"quantity\":" + DoubleToStr(OrderLots(), 2) + ",";
      json += "\"price_open\":" + DoubleToStr(OrderOpenPrice(), MarketInfo(OrderSymbol(), MODE_DIGITS)) + ",";
      json += "\"price_current\":" + DoubleToStr(OrderClosePrice(), MarketInfo(OrderSymbol(), MODE_DIGITS)) + ",";
      json += "\"profit\":" + DoubleToStr(OrderProfit(), 2) + ",";
      json += "\"sl\":" + DoubleToStr(OrderStopLoss(), MarketInfo(OrderSymbol(), MODE_DIGITS)) + ",";
      json += "\"tp\":" + DoubleToStr(OrderTakeProfit(), MarketInfo(OrderSymbol(), MODE_DIGITS));
      json += "}";
   }
   
   json += "]}";
   return json;
}

//+------------------------------------------------------------------+
//| Balance handler                                                  |
//+------------------------------------------------------------------+
string HandleBalance(string jsonRequest)
{
   string json = "{";
   json += "\"balance\":" + DoubleToStr(AccountBalance(), 2) + ",";
   json += "\"equity\":" + DoubleToStr(AccountEquity(), 2) + ",";
   json += "\"margin\":" + DoubleToStr(AccountMargin(), 2) + ",";
   json += "\"free_margin\":" + DoubleToStr(AccountFreeMargin(), 2) + ",";
   json += "\"margin_level\":" + DoubleToStr(AccountFreeMargin() / AccountMargin() * 100, 2) + ",";
   json += "\"currency\":\"" + AccountCurrency() + "\"";
   json += "}";
   
   return json;
}

//+------------------------------------------------------------------+
//| Utility functions                                                |
//+------------------------------------------------------------------+
string ExtractJsonField(string json, string fieldName)
{
   string search = "\"" + fieldName + "\":";
   int start = StringFind(json, search);
   if(start < 0) return "";
   
   start += StringLen(search);
   
   while(start < StringLen(json) && (StringGetChar(json, start) == ' ' || StringGetChar(json, start) == '"'))
      start++;
   
   int end = start;
   bool inQuotes = false;
   
   while(end < StringLen(json))
   {
      int ch = StringGetChar(json, end);
      if(ch == '"') inQuotes = !inQuotes;
      if(!inQuotes && (ch == ',' || ch == '}' || ch == ']')) break;
      end++;
   }
   
   string value = StringSubstr(json, start, end - start);
   
   if(StringGetChar(value, StringLen(value)-1) == '"')
      value = StringSubstr(value, 0, StringLen(value)-1);
   
   return value;
}

void ParseSymbolsArray(string arrayStr, string &symbols[])
{
   ArrayResize(symbols, 0);
   
   int start = StringFind(arrayStr, "[");
   int end = StringFind(arrayStr, "]");
   
   if(start < 0 || end < 0) return;
   
   string content = StringSubstr(arrayStr, start + 1, end - start - 1);
   
   string items[];
   int count = StringSplit(content, ',', items);
   
   ArrayResize(symbols, count);
   for(int i = 0; i < count; i++)
   {
      string item = items[i];
      StringReplace(item, "\"", "");
      StringReplace(item, " ", "");
      symbols[i] = item;
   }
}

int ParseTimeframe(string interval)
{
   if(interval == "M1" || interval == "1m") return PERIOD_M1;
   if(interval == "M5" || interval == "5m") return PERIOD_M5;
   if(interval == "M15" || interval == "15m") return PERIOD_M15;
   if(interval == "M30" || interval == "30m") return PERIOD_M30;
   if(interval == "H1" || interval == "1h") return PERIOD_H1;
   if(interval == "H4" || interval == "4h") return PERIOD_H4;
   if(interval == "D1" || interval == "1d") return PERIOD_D1;
   if(interval == "W1" || interval == "1w") return PERIOD_W1;
   if(interval == "MN1" || interval == "1M") return PERIOD_MN1;
   
   return PERIOD_H1;
}

string CreateErrorResponse(string errorMsg)
{
   return "{\"error\":\"" + errorMsg + "\"}";
}
//+------------------------------------------------------------------+
