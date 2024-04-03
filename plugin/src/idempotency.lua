local core = require("apisix.core")
local plugin = require("apisix.plugin")
local redis = require("resty.redis")
local cjson = require "cjson"

redis.register_module_prefix("json")

local plugin_name = "idempotency"

local _M = {
    version = 1.0,
    priority = 1500,
    schema = {},
    name = plugin_name,
}

local attr_schema = {
    type = "object",
    properties = {
        host = {
            type = "string",
            description = "Redis host",
            default = "localhost",
        },
        port = {
            type = "integer",
            description = "Redis port",
            default = 6379,
        },
    },
}

local red

function _M.init()
    local attr = plugin.plugin_attr(plugin_name) or {}
    local ok, err = core.schema.check(attr_schema, attr)
    if not ok then
        core.log.error("Failed to check the plugin_attr[", plugin_name, "]", ": ", err)
        return false, err
    end

    red = redis:new()
    red:set_timeout(1000)
    local ok, err = red:connect(attr.host, attr.port)
    if not ok then
        core.log.error("Failed to connect to Redis: ", err)
        return false, err
    end
end

local function hash_request(request, ctx)
    local request = {
        method = core.request.get_method(),
        uri = ctx.var.request_uri,
        headers = core.request.headers(),
        body = core.request.get_body()
    }
    local json = core.json.stably_encode(request)
    return ngx.encode_base64(json)
end

function _M.access(conf, ctx)
    local idempotency_key = core.request.header(ctx, "Idempotency-Key")
    if not idempotency_key then
        core.response.exit(400, "This operation is idempotent and it requires correct usage of Idempotency Key")
    end
    local resp, err = red:json():get(idempotency_key, "$")
    if not resp then
        core.log.error("Failed to get data in Redis: ", err)
        return
    end
    local hash = hash_request(core.request, ctx)
    if resp == ngx.null then
        core.log.warn("No key found in Redis for Idempotency-Key, set it: ", idempotency_key)
        local resp, err = red:json():set(idempotency_key, "$", '{ "request": "' .. hash .. '" }')
        if not resp then
            core.log.error("Failed to set data in Redis: ", err)
            return
        end
    else
        core.log.warn("Found cached response for Idempotency key, returning it: ", idempotency_key)
        local data = cjson.decode(resp)
        local stored_hash = data[1]["request"]
        if hash ~= stored_hash then
            core.response.exit(422, "This operation is idempotent and it requires correct usage of Idempotency Key. Idempotency Key MUST not be reused across different payloads of this operation.")
        end
        if not data[1].response then
            core.response.exit(409, " request with the same Idempotency-Key for the same operation is being processed or is outstanding.")
        end
        local body = data[1]["response"]["body"]
        local status_code = data[1]["response"]["status"]
        local headers = data[1]["response"]["headers"]
        for k, v in pairs(headers) do
            core.response.set_header(k, v)
        end
        core.response.exit(status_code, body)
    end
end

function _M.body_filter(conf, ctx)
    local idempotency_key = core.request.header(ctx, "Idempotency-Key")
    if core.response then
        local data = {
            response = {
                status = ngx.status,
                body = core.response.hold_body_chunk(ctx, true),
                headers = ngx.resp.get_headers()
            }
        }
        local resp, err = red:json():merge(idempotency_key, "$", cjson.encode(data))
        if not resp then
            core.log.error("Failed to set data in Redis: ", err)
            return
        end
    end
end

return _M
