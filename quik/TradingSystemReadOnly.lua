-- Copy next to QuikSharp.lua from pinned quik_python commit 0eae024.
-- Start THIS script, not QuikSharp.lua. No order-writing commands are exposed.
-- Use a separate folder/ports and change PROFILE for a separate live terminal.
local PROFILE = "demo-quik"
assert(_VERSION == "Lua 5.4", "TradingSystem requires Lua 5.4; actual runtime: " .. tostring(_VERSION))

dofile(getScriptPath() .. "\\QuikSharp.lua")
local qf = require("qsfunctions")
local dispatch = qf.dispatch_and_process
local forward_callback = sendCallback

local empty = {tsRuntimeInfo=true, isConnected=true, getClassesList=true, getTradeAccounts=true,
    get_orders=true, get_trades=true, getFuturesClientHoldings=true, getFuturesClientLimits=true}
local callbacks = {OnOrder=true, OnTrade=true, OnTransReply=true, OnConnected=true, OnDisconnected=true,
    OnFuturesClientHolding=true, OnFuturesLimitChange=true, OnFuturesLimitDelete=true,
    OnClose=true, OnStop=true, lua_error=true}
local parameters = {SEC_PRICE_STEP=true, STEPPRICE=true, CURSTEPPRICE=true, BUYDEPO=true, SELLDEPO=true}

local function identifier(value)
    return type(value) == "string" and #value > 0 and #value <= 64 and value:match("^[%w_.:%-]+$") ~= nil
end

local function allowed(command, data)
    if type(command) ~= "string" or type(data) ~= "string" then return false end
    if empty[command] and data == "" then return true end
    if command == "ping" and data == "Ping" then return true end
    if command == "getInfoParam" then
        return data == "VERSION" or data == "TRADEDATE" or data == "SERVERTIME"
    end
    if command == "getClassSecurities" then return identifier(data) end
    if command == "getSecurityInfo" then
        local class_code, sec_code = data:match("^([^|]+)|([^|]+)$")
        return identifier(class_code) and identifier(sec_code)
    end
    if command == "getParamEx" then
        local class_code, sec_code, parameter = data:match("^([^|]+)|([^|]+)|([^|]+)$")
        return class_code == "SPBFUT" and identifier(sec_code) and parameters[parameter] == true
    end
    return false
end

function qf.dispatch_and_process(msg)
    if type(msg) ~= "table" then
        return {cmd="lua_error", data="", lua_error="read-only request rejected"}
    end
    if type(msg.id) ~= "number" or math.type(msg.id) ~= "integer" or msg.id < 1 or msg.id > 2147483647
            or not allowed(msg.cmd, msg.data) then
        msg.cmd = "lua_error"
        msg.data = ""
        msg.lua_error = "read-only request rejected"
        return msg
    end
    if msg.cmd == "tsRuntimeInfo" then
        msg.data = {protocol=1, mode="read_only", profile=PROFILE,
                    quik_version=getInfoParam("VERSION"), lua_version=_VERSION,
                    capabilities={"instrument_parameters_v1"}}
        return msg
    end
    return dispatch(msg)
end

-- Diagnostics only needs financial/lifecycle callbacks, not the market tape.
function sendCallback(msg)
    if type(msg) == "table" and callbacks[msg.cmd] then
        return forward_callback(msg)
    end
    return true
end

local function port(value)
    return type(value) == "number" and math.type(value) == "integer" and value >= 1 and value <= 65535
end

function main()
    local config = paramsFromConfig("TradingSystemReadOnly")
    assert(config and config[1] == "127.0.0.1" and config[3] == "127.0.0.1",
           "Read-only bridge requires explicit loopback endpoints in config.json")
    assert(port(config[2]) and port(config[4]) and config[2] ~= config[4], "Invalid read-only ports")
    assert(setup("TradingSystemReadOnly"), "Read-only bridge setup failed")
    run()
end
