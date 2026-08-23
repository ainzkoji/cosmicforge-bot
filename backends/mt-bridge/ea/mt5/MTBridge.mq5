//+------------------------------------------------------------------+
//|                                                     MTBridge.mq5 |
//|                                      MetaTrader 5 Bridge for Bot |
//|                                                                   |
//+------------------------------------------------------------------+
#property copyright "CosmicForge"
#property link      ""
#property version   "1.00"
#property strict

// ZeroMQ library - requires mqzmq.dll in Libraries folder
#include <Zmq/Zmq.mqh>

// Global ZMQ context and socket
Context context("mt-bridge");
Socket socket(context, ZMQ_REP);  // Reply socket

// Configuration
input string ZMQ_PORT = "5555";
input string API_SECRET = "";  // Optional: for additional validation

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   Print("MTBridge EA starting...");
   
   // Bind ZMQ socket (localhost only for security)
   string endpoint = "tcp://127.0.0.1:" + ZMQ_PORT;
   if(!socket.bind(endpoint))
   {
      Print("ERROR: Failed to bind ZMQ socket on ", endpoint);
      return(INIT_FAILED);
   }
   
   Print("MTBridge initialized. Listening on ", endpoint);
   Print("Account: ", AccountInfoInteger(ACCOUNT_LOGIN));
   Print("Server: ", AccountInfoString(ACCOUNT_SERVER));
   
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
   // Check for incoming ZMQ messages (non-blocking)
   ZmqMsg request;
   
   if(socket.recv(request, true))  // Non-blocking receive
   {
      string requestStr = request.getData();
      Print("Received request: ", requestStr);
      
      // Parse JSON and route to handler
      string response = HandleRequest(requestStr);
      
      // Send response
      ZmqMsg reply(response);
      socket.send(reply);
      
      Print("Sent response: ", StringSubstr(response, 0, 100), "...");
   }
}

//+------------------------------------------------------------------+
//| Main request handler - routes based on action                    |
//+------------------------------------------------------------------+
string HandleRequest(string jsonRequest)
{
   // Parse JSON manually (MQL5 has limited JSON support)
   string action = ExtractJsonField(jsonRequest, "action");
   
   if(action == "")
   {
      return CreateErrorResponse("Missing 'action' field");
   }
   
   // Route to appropriate handler
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
   json += "\"platform\":\"mt5\",";
   json += "\"account\":" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + ",";
   json += "\"server\":\"" + AccountInfoString(ACCOUNT_SERVER) + "\",";
   json += "\"time\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\",";
   json += "\"connected\":" + (TerminalInfoInteger(TERMINAL_CONNECTED) ? "true" : "false");
   json += "}";
   
   return json;
}

//+------------------------------------------------------------------+
//| Instruments handler - returns available symbols                  |
//+------------------------------------------------------------------+
string HandleInstruments(string jsonRequest)
{
   string json = "{\"symbols\":[";
   
   int total = SymbolsTotal(true);  // Only selected in MarketWatch
   for(int i = 0; i < total; i++)
   {
      string symbol = SymbolName(i, true);
      
      if(i > 0) json += ",";
      
      json += "{";
      json += "\"symbol\":\"" + symbol + "\",";
      json += "\"description\":\"" + SymbolInfoString(symbol, SYMBOL_DESCRIPTION) + "\",";
      json += "\"digits\":" + IntegerToString(SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"contract_size\":" + DoubleToString(SymbolInfoDouble(symbol, SYMBOL_TRADE_CONTRACT_SIZE), 2) + ",";
      json += "\"min_lot\":" + DoubleToString(SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN), 2) + ",";
      json += "\"max_lot\":" + DoubleToString(SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX), 2) + ",";
      json += "\"lot_step\":" + DoubleToString(SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP), 2) + ",";
      json += "\"tick_size\":" + DoubleToString(SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE), 8) + ",";
      json += "\"tick_value\":" + DoubleToString(SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE), 8);
      json += "}";
   }
   
   json += "]}";
   return json;
}

//+------------------------------------------------------------------+
//| Prices handler - returns current bid/ask                         |
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
      
      double bid = SymbolInfoDouble(symbol, SYMBOL_BID);
      double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
      
      json += "{";
      json += "\"symbol\":\"" + symbol + "\",";
      json += "\"bid\":" + DoubleToString(bid, SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"ask\":" + DoubleToString(ask, SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"time\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\"";
      json += "}";
   }
   
   json += "]}";
   return json;
}

//+------------------------------------------------------------------+
//| Klines handler - returns historical candles                      |
//+------------------------------------------------------------------+
string HandleKlines(string jsonRequest)
{
   string symbol = ExtractJsonField(jsonRequest, "symbol");
   string interval = ExtractJsonField(jsonRequest, "interval");
   int limit = (int)StringToInteger(ExtractJsonField(jsonRequest, "limit"));
   
   if(limit <= 0) limit = 100;
   
   ENUM_TIMEFRAMES tf = ParseTimeframe(interval);
   
   MqlRates rates[];
   int copied = CopyRates(symbol, tf, 0, limit, rates);
   
   if(copied <= 0)
   {
      return CreateErrorResponse("Failed to get candles for " + symbol);
   }
   
   string json = "{\"klines\":[";
   
   for(int i = 0; i < copied; i++)
   {
      if(i > 0) json += ",";
      
      json += "{";
      json += "\"time\":\"" + TimeToString(rates[i].time, TIME_DATE|TIME_SECONDS) + "\",";
      json += "\"open\":" + DoubleToString(rates[i].open, SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"high\":" + DoubleToString(rates[i].high, SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"low\":" + DoubleToString(rates[i].low, SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"close\":" + DoubleToString(rates[i].close, SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"volume\":" + IntegerToString(rates[i].tick_volume);
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
   double quantity = StringToDouble(ExtractJsonField(jsonRequest, "quantity"));
   double price = StringToDouble(ExtractJsonField(jsonRequest, "price"));
   double sl = StringToDouble(ExtractJsonField(jsonRequest, "sl"));
   double tp = StringToDouble(ExtractJsonField(jsonRequest, "tp"));
   string clientOrderId = ExtractJsonField(jsonRequest, "client_order_id");
   
   // Determine order type
   ENUM_ORDER_TYPE orderType;
   if(typeStr == "market")
   {
      orderType = (sideStr == "buy") ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   }
   else if(typeStr == "limit")
   {
      orderType = (sideStr == "buy") ? ORDER_TYPE_BUY_LIMIT : ORDER_TYPE_SELL_LIMIT;
   }
   else if(typeStr == "stop")
   {
      orderType = (sideStr == "buy") ? ORDER_TYPE_BUY_STOP : ORDER_TYPE_SELL_STOP;
   }
   else
   {
      return CreateErrorResponse("Invalid order type: " + typeStr);
   }
   
   // Prepare trade request
   MqlTradeRequest request;
   MqlTradeResult result;
   
   ZeroMemory(request);
   ZeroMemory(result);
   
   request.action = TRADE_ACTION_DEAL;
   request.symbol = symbol;
   request.volume = quantity;
   request.type = orderType;
   request.price = (typeStr == "market") ? 0 : price;  // Market orders use 0
   request.sl = sl;
   request.tp = tp;
   request.deviation = 10;
   request.magic = 12345;
   request.comment = clientOrderId;
   request.type_filling = ORDER_FILLING_IOC;
   
   // Send order
   bool success = OrderSend(request, result);
   
   if(!success || result.retcode != TRADE_RETCODE_DONE)
   {
      return CreateErrorResponse("Order failed: " + IntegerToString(result.retcode) + " - " + result.comment);
   }
   
   // Build response
   string json = "{";
   json += "\"order_id\":\"" + IntegerToString(result.order) + "\",";
   json += "\"ticket\":" + IntegerToString(result.order) + ",";
   json += "\"price\":" + DoubleToString(result.price, SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
   json += "\"volume\":" + DoubleToString(result.volume, 2) + ",";
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
   ulong ticket = (ulong)StringToInteger(orderIdStr);
   
   MqlTradeRequest request;
   MqlTradeResult result;
   
   ZeroMemory(request);
   ZeroMemory(result);
   
   request.action = TRADE_ACTION_REMOVE;
   request.order = ticket;
   
   bool success = OrderSend(request, result);
   
   if(!success || result.retcode != TRADE_RETCODE_DONE)
   {
      return CreateErrorResponse("Cancel failed: " + IntegerToString(result.retcode));
   }
   
   return "{\"success\":true,\"order_id\":\"" + orderIdStr + "\"}";
}

//+------------------------------------------------------------------+
//| Get order status handler                                         |
//+------------------------------------------------------------------+
string HandleGetOrder(string jsonRequest)
{
   string orderIdStr = ExtractJsonField(jsonRequest, "order_id");
   ulong ticket = (ulong)StringToInteger(orderIdStr);
   
   // Check if position exists with this ticket
   if(PositionSelectByTicket(ticket))
   {
      string json = "{";
      json += "\"order_id\":\"" + orderIdStr + "\",";
      json += "\"status\":\"filled\",";
      json += "\"symbol\":\"" + PositionGetString(POSITION_SYMBOL) + "\",";
      json += "\"side\":\"" + (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY ? "buy" : "sell") + "\",";
      json += "\"quantity\":" + DoubleToString(PositionGetDouble(POSITION_VOLUME), 2) + ",";
      json += "\"price\":" + DoubleToString(PositionGetDouble(POSITION_PRICE_OPEN), 8);
      json += "}";
      return json;
   }
   
   // Check if pending order
   if(OrderSelect(ticket))
   {
      string json = "{";
      json += "\"order_id\":\"" + orderIdStr + "\",";
      json += "\"status\":\"pending\",";
      json += "\"symbol\":\"" + OrderGetString(ORDER_SYMBOL) + "\",";
      json += "\"quantity\":" + DoubleToString(OrderGetDouble(ORDER_VOLUME_CURRENT), 2);
      json += "}";
      return json;
   }
   
   return CreateErrorResponse("Order not found: " + orderIdStr);
}

//+------------------------------------------------------------------+
//| Positions handler - returns all open positions                   |
//+------------------------------------------------------------------+
string HandlePositions(string jsonRequest)
{
   string json = "{\"positions\":[";
   
   int total = PositionsTotal();
   for(int i = 0; i < total; i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket <= 0) continue;
      
      if(i > 0) json += ",";
      
      string symbol = PositionGetString(POSITION_SYMBOL);
      
      json += "{";
      json += "\"ticket\":" + IntegerToString(ticket) + ",";
      json += "\"symbol\":\"" + symbol + "\",";
      json += "\"side\":\"" + (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY ? "buy" : "sell") + "\",";
      json += "\"quantity\":" + DoubleToString(PositionGetDouble(POSITION_VOLUME), 2) + ",";
      json += "\"price_open\":" + DoubleToString(PositionGetDouble(POSITION_PRICE_OPEN), SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"price_current\":" + DoubleToString(PositionGetDouble(POSITION_PRICE_CURRENT), SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"profit\":" + DoubleToString(PositionGetDouble(POSITION_PROFIT), 2) + ",";
      json += "\"sl\":" + DoubleToString(PositionGetDouble(POSITION_SL), SymbolInfoInteger(symbol, SYMBOL_DIGITS)) + ",";
      json += "\"tp\":" + DoubleToString(PositionGetDouble(POSITION_TP), SymbolInfoInteger(symbol, SYMBOL_DIGITS));
      json += "}";
   }
   
   json += "]}";
   return json;
}

//+------------------------------------------------------------------+
//| Balance handler - returns account balance and equity             |
//+------------------------------------------------------------------+
string HandleBalance(string jsonRequest)
{
   string json = "{";
   json += "\"balance\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ",";
   json += "\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ",";
   json += "\"margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN), 2) + ",";
   json += "\"free_margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) + ",";
   json += "\"margin_level\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_LEVEL), 2) + ",";
   json += "\"currency\":\"" + AccountInfoString(ACCOUNT_CURRENCY) + "\"";
   json += "}";
   
   return json;
}

//+------------------------------------------------------------------+
//| Utility: Extract JSON field value (simplified parser)            |
//+------------------------------------------------------------------+
string ExtractJsonField(string json, string fieldName)
{
   string search = "\"" + fieldName + "\":";
   int start = StringFind(json, search);
   if(start < 0) return "";
   
   start += StringLen(search);
   
   // Skip whitespace and quotes
   while(start < StringLen(json) && (StringGetCharacter(json, start) == ' ' || StringGetCharacter(json, start) == '"'))
      start++;
   
   int end = start;
   bool inQuotes = false;
   
   // Find end of value
   while(end < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, end);
      if(ch == '"') inQuotes = !inQuotes;
      if(!inQuotes && (ch == ',' || ch == '}' || ch == ']')) break;
      end++;
   }
   
   string value = StringSubstr(json, start, end - start);
   
   // Remove trailing quote if present
   if(StringGetCharacter(value, StringLen(value)-1) == '"')
      value = StringSubstr(value, 0, StringLen(value)-1);
   
   return value;
}

//+------------------------------------------------------------------+
//| Utility: Parse symbols array from JSON                           |
//+------------------------------------------------------------------+
void ParseSymbolsArray(string arrayStr, string &symbols[])
{
   // Simple parser for ["SYM1","SYM2","SYM3"]
   ArrayResize(symbols, 0);
   
   int start = StringFind(arrayStr, "[");
   int end = StringFind(arrayStr, "]");
   
   if(start < 0 || end < 0) return;
   
   string content = StringSubstr(arrayStr, start + 1, end - start - 1);
   
   // Split by comma
   string items[];
   int count = StringSplit(content, ',', items);
   
   ArrayResize(symbols, count);
   for(int i = 0; i < count; i++)
   {
      // Remove quotes and whitespace
      string item = items[i];
      StringReplace(item, "\"", "");
      StringReplace(item, " ", "");
      symbols[i] = item;
   }
}

//+------------------------------------------------------------------+
//| Utility: Parse timeframe string to ENUM_TIMEFRAMES               |
//+------------------------------------------------------------------+
ENUM_TIMEFRAMES ParseTimeframe(string interval)
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
   
   return PERIOD_H1;  // Default
}

//+------------------------------------------------------------------+
//| Utility: Create error response JSON                              |
//+------------------------------------------------------------------+
string CreateErrorResponse(string errorMsg)
{
   return "{\"error\":\"" + errorMsg + "\"}";
}
//+------------------------------------------------------------------+
