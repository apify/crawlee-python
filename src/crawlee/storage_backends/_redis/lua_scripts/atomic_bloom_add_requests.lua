local added_filter_key = KEYS[1]
local queue_key = KEYS[2]
local data_key = KEYS[3]

local forefront = ARGV[1] == '1'
local unique_keys = cjson.decode(ARGV[2])
local requests_data = cjson.decode(ARGV[3])

-- Add and check which unique keys are actually new using Bloom filter
local bf_results = redis.call('bf.madd', added_filter_key, unpack(unique_keys))

local actually_added = {}
local hset_args = {}

-- Process the results
for i, unique_key in ipairs(unique_keys) do
    if bf_results[i] == 1 then
        -- This key was added by us (did not exist before)
        table.insert(hset_args, unique_key)
        table.insert(hset_args, requests_data[unique_key])
        table.insert(actually_added, unique_key)
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
end

return cjson.encode(actually_added)
