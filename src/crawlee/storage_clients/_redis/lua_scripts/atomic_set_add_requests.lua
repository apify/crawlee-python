local queue_key = KEYS[1]
local data_key = KEYS[2]
local metadata_key = KEYS[3]
local pending_set_key = KEYS[4]
local handled_set_key = KEYS[5]

local forefront = ARGV[1] == '1'
local unique_keys = cjson.decode(ARGV[2])
local requests_data = cjson.decode(ARGV[3])

-- Add and check which unique keys are actually new using Redis set
local actually_added = {}
local hset_args = {}

-- Process each unique key
for _, unique_key in ipairs(unique_keys) do
    -- A request handled after the caller checked the sets isn't queued again
    if redis.call('sismember', handled_set_key, unique_key) == 0 then
        -- Try to add the key to the set, returns 1 if added, 0 if already existed
        local set_result = redis.call('sadd', pending_set_key, unique_key)

        if set_result == 1 then
            -- This key was added by us (did not exist before)
            table.insert(hset_args, unique_key)
            table.insert(hset_args, requests_data[unique_key])
            table.insert(actually_added, unique_key)
        end
    end
end

-- Add only those that are actually new
if #actually_added > 0 then
    redis.call('hset', data_key, unpack(hset_args))

    if forefront then
        redis.call('lpush', queue_key, unpack(actually_added))
    else
        redis.call('rpush', queue_key, unpack(actually_added))
    end

    -- Counted in the same script, so a lost reply doesn't leave the queued requests uncounted
    redis.call('json.numincrby', metadata_key, '$.pending_request_count', #actually_added)
    redis.call('json.numincrby', metadata_key, '$.total_request_count', #actually_added)
end

return cjson.encode(actually_added)
