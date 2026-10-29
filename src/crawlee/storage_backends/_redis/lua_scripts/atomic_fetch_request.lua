local queue_key = KEYS[1]
local in_progress_key = KEYS[2]
local data_key = KEYS[3]
local client_id = ARGV[1]
local blocked_until_timestamp = ARGV[2]
local batch_size = tonumber(ARGV[3])

-- Pop batch unique_key from queue
local batch_result = redis.call('LMPOP', 1, queue_key, 'LEFT', 'COUNT', batch_size)
if not batch_result then
    return nil
end
local unique_keys = batch_result[2]

-- Get requests data
local requests_data = redis.call('HMGET', data_key, unpack(unique_keys))
if not requests_data then
    -- Data missing, skip this request
    return nil
end

-- Prepare results and update in_progress
local final_result = {}
local in_progress_hmset = {}
local pending_decrement = 0
local in_progress_data = cjson.encode({
    client_id = client_id,
    blocked_until_timestamp = tonumber(blocked_until_timestamp)
})
for i = 1, #unique_keys do
    local unique_key = unique_keys[i]
    local request_data = requests_data[i]

    if request_data then
        -- Add to in_progress hash
        table.insert(in_progress_hmset, unique_key)
        table.insert(in_progress_hmset, in_progress_data)

        table.insert(final_result, request_data)
    end
end

-- Update in_progress hash
if #in_progress_hmset > 0 then
    redis.call('HMSET', in_progress_key, unpack(in_progress_hmset))
end

-- Return result with requests data
return final_result
