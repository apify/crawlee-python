local in_progress_key = KEYS[1]
local unique_key = ARGV[1]
local client_id = ARGV[2]
local current_time = tonumber(ARGV[3])
local delta = tonumber(ARGV[4])

local in_progress_data = redis.call('hget', in_progress_key, unique_key)
if not in_progress_data then
    return 0
end

local data = cjson.decode(in_progress_data)

-- Only a live lock held by this client can be extended
if data.client_id ~= client_id or current_time > data.blocked_until_timestamp then
    return 0
end

data.blocked_until_timestamp = data.blocked_until_timestamp + delta
redis.call('hset', in_progress_key, unique_key, cjson.encode(data))

return 1
