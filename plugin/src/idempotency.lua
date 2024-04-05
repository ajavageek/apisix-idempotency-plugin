local str_format      = string.format
local redis_new       = require("resty.redis").new
local cjson           = require("cjson")

local core            = require("apisix.core")
local bp_manager_mod  = require("apisix.utils.batch-processor-manager")
local log_util        = require("apisix.utils.log-util")

local plugin_name = "idempotency"
local batch_processor_manager = bp_manager_mod.new(plugin_name)

-- This is the configuration used by the batch processor.
-- 
-- It configures batch_max_size to 1, i.e. we expect that every
-- new response received immediately puts it into the execution queue, 
-- which should be executed immediately, unlike other logging plugins, 
-- which send it after the batch condition or time condition is met.
-- 
-- Depending on the retry configuration, if a write to redis fails, 
-- it will print some logs and retry after 1 second, up to 3 times,
-- and if it never succeeds, this write operation will be discarded.
--
-- And please ignore the default values of buffer_duration and 
-- inactive_timeout, since the maximum batch is always 1, time-based 
-- wait conditions will be skipped.
local batch_processor_manager_conf = {
  name = "idempotency response buffer",
  max_retry_count = 3,
  retry_delay = 1,
  buffer_duration = 60,
  inactive_timeout = 5,
  batch_max_size = 1,
}

local schema = {
  type = "object",
  properties = {
    redis = {
      type = "object",
      properties = {
        host = {
          type = "string",
          default = "localhost",
        },
        port = {
          type = "integer",
          default = 6379,
          minimum = 1,
          maximum = 65535,
        },
        username = {
          type = "string",
        },
        password = {
          type = "string",
        },
        database = {
          type = "integer",
          minimum = 0,
          default = 0,
        },
        timeout = {
          type = "integer",
          minimum = 1,
          default = 1000,
        },
        ssl = {
          type = "boolean",
          default = false,
        },
        ssl_verify = {
          type = "boolean",
          default = false,
        },
      },
    },
  },
  required = {"redis"}
}

local _M = {
    version = 1.0,
    priority = 1500,
    schema = schema,
    name = plugin_name,
}

function _M.check_schema(conf)
  return core.schema.check(schema, conf)
end

-- New a redis client instance
-- copy from apisix/plugins/limit-count/limit-count-redis.lua
--
-- It is used in every access phase and in the batch processor,
-- the OpenResty will take care of maintaining the connection
-- pool and we can use it directly without needing to relate any
-- TCP related issues.
local function redis_cli(conf)
  local redis_conf = conf.redis
  local red = redis_new()
  local timeout = redis_conf.timeout or 1000    -- 1sec

  red:set_timeouts(timeout, timeout, timeout)

  local sock_opts = {
    ssl = redis_conf.ssl,
    ssl_verify = redis_conf.ssl_verify
  }

  local ok, err = red:connect(redis_conf.host, redis_conf.port or 6379, sock_opts)
  if not ok then
    return false, err
  end

  local count
  count, err = red:get_reused_times()
  if 0 == count then
    if redis_conf.password and redis_conf.password ~= '' then
        local ok, err
        if redis_conf.username then
            ok, err = red:auth(redis_conf.username, redis_conf.password)
        else
            ok, err = red:auth(redis_conf.password)
        end
        if not ok then
            return nil, err
        end
    end

    -- select db
    if redis_conf.database ~= 0 then
        local ok, err = red:select(redis_conf.database)
        if not ok then
            return false, "failed to change redis db, err: " .. err
        end
    end
  elseif err then
    -- core.log.info(" err: ", err)
    return nil, err
  end
  return red, nil
end


-- Save response data to redis
local function send_resp_data(conf, data)
  local redis_key = "idempotency#"..data.idempotency_key

  local red, err = redis_cli(conf)
  if not red then
    return false, str_format("failed to create redis client when send_resp_data: ", err)
  end

  local res, err = red:hset(redis_key, "response", core.json.encode(data.response))
  if not res and err then
    return false, str_format("failed to save response to redis when send_resp_data: %s", err)
  end

  return true
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

local function json_response(status, message)
  core.response.set_header("Content-Type", "application/json")

  -- To return a response to the client you can use the following formats, including JSON.
  return status, { message = message }
end

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


function _M.access(conf, ctx)
  local idempotency_key = core.request.header(ctx, "Idempotency-Key")
  if not idempotency_key then
    return json_response(400, "This operation is idempotent and it requires correct usage of Idempotency Key")
  end

  -- Get an available redis client instance
  local red, err = redis_cli(conf)
  if not red then
    core.log.error("failed to create redis client: ", err)
    return json_response(500, "Internal Server Error")
  end

  local redis_key = "idempotency#"..idempotency_key

  -- Check if the key exists in redis
  local exist, err = red:exists(redis_key)
  if not exist and err then
    core.log.error("failed to get idempotency key in redis: ", err)
    return json_response(500, "Internal Server Error")
  end

  local hash = hash_request(core.request, ctx)
  if exist == 0 then -- not exist, save to redis
    core.log.warn("No key found in Redis for Idempotency-Key, set it: ", redis_key)
    local res, err = red:hset(redis_key, "request", hash)
    if not res and err then
      core.log.error("failed to set idempotency key in redis: ", err)
      return json_response(500, "Internal Server Error")
    end
    ctx.idempotency_first_request = true
  elseif exist == 1 then -- already exist, return to client
    core.log.warn("Found cached response for Idempotency key, return it: ", redis_key)
    local res, err = red:hgetall(redis_key)
    if not res and err then
      core.log.error("failed to get idempotency key info in redis: ", err)
      return json_response(500, "Internal Server Error")
    end

    -- Normailize the array result, refer: https://redis.io/commands/hgetall/
    res = normalize_hgetall_result(res)
    if res.request ~= hash then
      return json_response(422, "This operation is idempotent and it requires correct usage of Idempotency Key. Idempotency Key MUST not be reused across different payloads of this operation.")
    end
    if not res.response then
      return json_response(409, "The request with the same Idempotency-Key for the same operation is being processed or is outstanding.")
    end

    -- Send response
    local previous_resp = core.json.decode(res.response)
    for k, v in pairs(previous_resp.headers) do
      core.response.set_header(k, v)
    end
    return previous_resp.status, previous_resp.body
  end
end


-- The scenario for the body_filter function is to modify the response
-- body, not to do anything else, so refer to the other logger plugin,
-- which does not use body_filter to write directly to redis.
--
-- In fact, even if we wanted to write to redis here it wouldn't be
-- possible, the body_filter stage can't perform blocking IO syscalls
-- (SOCKET CERTAINLY ARE)
function _M.body_filter(conf, ctx)
  -- Ensure that only the response body of the first request will be recorded
  if not ctx.idempotency_first_request then
    return
  end

  -- Referring to other logger plugins, the response body is collected 
  -- with the help of tool functions and stored in the ctx.
  log_util.collect_body({include_resp_body = true}, ctx)
end

-- As with body_filter, the log phase is also unable to performblocking IO
-- syscalls, so we will use the APISIX batch processor, which allows
-- blocking IO calls to be performed asynchronously.
-- It uses a fixed batch processor configuration in the header of that file,
-- where it is explained more.
function _M.log(conf, ctx)
  -- Ensure that only the response of the first request will save to redis
  if not ctx.idempotency_first_request then
    return
  end

  local idempotency_key = core.request.header(ctx, "Idempotency-Key")
  local data = {
    idempotency_key = idempotency_key,
    response = {
      status = ngx.status,
      body = ctx.resp_body,
      headers = ngx.resp.get_headers()
    }
  }

  -- Adding an entry to the batch processor
  if batch_processor_manager:add_entry(batch_processor_manager_conf, data) then
    return
  end

  local func = function(entries)
    return send_resp_data(conf, entries[1])
  end
  -- Initialize the batch processor and add an entry to the batch processor
  batch_processor_manager:add_entry_to_new_processor(batch_processor_manager_conf, data, ctx, func)
end

return _M
