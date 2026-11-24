local in_progress_key = KEYS[1]
local data_key = KEYS[2]
local metadata_key = KEYS[3]
local handled_key = KEYS[4]
local pending_set_key = KEYS[5]

local unique_key = ARGV[1]
local request_data = ARGV[2]
local now = ARGV[3]
local dedup_strategy = ARGV[4]

-- Only a request still in progress gets marked, so a retry after a lost reply doesn't count it twice
if redis.call('hexists', in_progress_key, unique_key) == 0 then
    return 0
end

if dedup_strategy == 'bloom' then
    redis.call('bf.add', handled_key, unique_key)
else
    redis.call('sadd', handled_key, unique_key)
    redis.call('srem', pending_set_key, unique_key)
end

redis.call('hset', data_key, unique_key, request_data)

redis.call('json.numincrby', metadata_key, '$.handled_request_count', 1)
redis.call('json.numincrby', metadata_key, '$.pending_request_count', -1)
redis.call('json.set', metadata_key, '$.accessed_at', now, 'XX')
redis.call('json.set', metadata_key, '$.modified_at', now, 'XX')

-- Redis doesn't roll back a script that fails midway, so the request leaves `in_progress` only after the other writes
redis.call('hdel', in_progress_key, unique_key)

return 1
