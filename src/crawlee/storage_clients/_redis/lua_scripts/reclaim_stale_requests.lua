local in_progress_key = KEYS[1]
local queue_key = KEYS[2]
local data_key = KEYS[3]
local current_time = tonumber(ARGV[1])

local max_reclaim = 1000

local cursor = "0"
local count = 0

repeat
    local result = redis.call('hscan', in_progress_key, cursor, 'COUNT', 100)
    cursor = result[1]
    local entries = result[2]

    for i = 1, #entries, 2 do
        if count >= max_reclaim then
            break
        end

        local unique_key = entries[i]
        local data = cjson.decode(entries[i + 1])

        -- Check if timed out
        if current_time > data.blocked_until_timestamp then
            -- Atomically remove from in_progress and add back to queue
            redis.call('hdel', in_progress_key, unique_key)
            redis.call('rpush', queue_key, unique_key)
            count = count + 1
        end
    end
until cursor == "0" or count >= max_reclaim

return count
