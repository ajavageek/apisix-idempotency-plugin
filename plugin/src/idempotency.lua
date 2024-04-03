local core = require("apisix.core")
local plugin = require("apisix.plugin")
local redis_new = require("resty.redis").new

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

-- Converting Redis HGETALL results to key-value pairs
-- ["request", "base64_encoded_text", "ba", "la"] => {request = "base64_encoded_text", ba = "la"}
local function normalize_hgetall_result(data)
    local res = {}
    for index, value in ipairs(data) do
        if index % 2 == 1 then
            res[value] = data[index + 1]
        end
    end
    return res
end

local redis

function _M.init()
    local attr = plugin.plugin_attr(plugin_name) or {}
    local ok, err = core.schema.check(attr_schema, attr)
    if not ok then
        core.log.error("Failed to check the plugin_attr[", plugin_name, "]", ": ", err)
        return false, err
    end
    redis = redis_new()
    redis:set_timeout(1000)
    local ok, err = redis:connect(attr.host, attr.port)
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
        return core.response.exit(400, "This operation is idempotent and it requires correct usage of Idempotency Key")
    end
    local redis_key = "idempotency#" .. idempotency_key
    local resp, err = redis:hgetall(idempotency_key)
    if not resp then
        core.log.error("Failed to get data in Redis: ", err)
        return
    end
    local hash = hash_request(core.request, ctx)
    if next(resp) == nil then
        core.log.warn("No key found in Redis for Idempotency-Key, set it: ", redis_key)
        local resp, err = redis:hset(redis_key, "request", hash)
        if not resp then
            core.log.error("Failed to set data in Redis: ", err)
            return
        end
    else
        core.log.warn("Found cached response for Idempotency key, returning it: ", redis_key)
        local data = normalize_hgetall_result(resp)
        local stored_hash = data["request"]
        if hash ~= stored_hash then
            return core.response.exit(422, "This operation is idempotent and it requires correct usage of Idempotency Key. Idempotency Key MUST not be reused across different payloads of this operation.")
        end
        local response = core.json.decode(data["response"])
        local body = response["body"]
        local status_code = response["status"]
        local headers = response["headers"]
        for k, v in pairs(headers) do
            core.response.set_header(k, v)
        end
        return core.response.exit(status_code, body)
    end
end

function _M.body_filter(conf, ctx)
    local idempotency_key = core.request.header(ctx, "Idempotency-Key")
    local redis_key = "idempotency#" .. idempotency_key
    if core.response then
        local response = {
            status = ngx.status,
            body = core.response.hold_body_chunk(ctx, true),
            headers = ngx.resp.get_headers()
        }
        local resp, err = redis:hset(redis_key, "response", core.json.encode(response))
        if not resp then
            core.log.error("Failed to set data in Redis: ", err)
            return
        end
    end
end

return _M
