local in_progress_key = KEYS[1]
local queue_key = KEYS[2]
local data_key = KEYS[3]
local metadata_key = KEYS[4]

local unique_key = ARGV[1]
local request_data = ARGV[2]
local now = ARGV[3]
local forefront = ARGV[4] == '1'
local client_id = ARGV[5]
local blocked_until_timestamp = tonumber(ARGV[6])

-- A request no longer in progress may already be back in the queue, put there by an earlier attempt whose reply was
-- lost or by a stale reclaim
if redis.call('hexists', in_progress_key, unique_key) == 0 then
    return 0
end

redis.call('hset', data_key, unique_key, request_data)
redis.call('json.set', metadata_key, '$.accessed_at', now, 'XX')
redis.call('json.set', metadata_key, '$.modified_at', now, 'XX')

if forefront then
    -- The client serves a forefront request from its local cache, so the request stays blocked for other clients
    local in_progress_data = cjson.encode({
        client_id = client_id,
        blocked_until_timestamp = blocked_until_timestamp
    })
    redis.call('hset', in_progress_key, unique_key, in_progress_data)
else
    -- Redis doesn't roll back a script that fails midway, so the request leaves `in_progress` only after the push
    redis.call('rpush', queue_key, unique_key)
    redis.call('hdel', in_progress_key, unique_key)
end

return 1
