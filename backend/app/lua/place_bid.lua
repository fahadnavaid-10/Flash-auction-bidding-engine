--[[
  place_bid.lua
  ATOMIC HOT PATH BIDDING ENGINE

  Keys:
    KEYS[1] = auction:{auction_id}               (Hash: auction state)
    KEYS[2] = auction:{auction_id}:bids_seen      (Set/Hash: idempotency tracking)
    KEYS[3] = auction_stream                     (Stream: durability log for DB worker)
    KEYS[4] = auction_channel:{auction_id}       (Pub/Sub: fan-out channel)

  Args:
    ARGV[1] = auction_id
    ARGV[2] = user_id
    ARGV[3] = bid_id (Client-generated UUID for idempotency)
    ARGV[4] = bid_amount (string representation of float)
    ARGV[5] = anti_snipe_window_sec (e.g. 5.0 seconds)
    ARGV[6] = anti_snipe_extension_sec (e.g. 10.0 seconds)

  Return format:
    Array: [status_code, message_or_price, highest_bidder, server_timestamp, end_time, extended_flag]
    status_code:
      1 = ACCEPTED (new winning bid)
      2 = IDEMPOTENT_DUPLICATE (already processed, no state change)
      0 = REJECTED (error/rejection reason in message)
--]]

local auction_key    = KEYS[1]
local seen_key       = KEYS[2]
local stream_key     = KEYS[3]
local pubsub_channel = KEYS[4]

local auction_id              = ARGV[1]
local user_id                 = ARGV[2]
local bid_id                  = ARGV[3]
local new_amount              = tonumber(ARGV[4])
local anti_snipe_window       = tonumber(ARGV[5]) or 5.0
local anti_snipe_extension    = tonumber(ARGV[6]) or 10.0

-- 1. Check existence of auction in Redis
local exists = redis.call('EXISTS', auction_key)
if exists == 0 then
    return {0, "AUCTION_NOT_FOUND", "", "0", "0", "0"}
end

-- 2. Fetch server-side authoritative time from Redis core
--    Redis TIME returns [seconds, microseconds]. We combine them into a high-precision float.
--    This completely eliminates reliance on untrusted or drifted client timestamps.
local time_arr = redis.call('TIME')
local server_time = tonumber(time_arr[1]) + (tonumber(time_arr[2]) / 1000000.0)

-- 3. Check Idempotency via Client Bid ID
--    If the client retried a network request, we return the cached confirmation without
--    incrementing the price twice or corrupting the sequence.
local already_seen = redis.call('HEXISTS', seen_key, bid_id)
if already_seen == 1 then
    local cached_amount = redis.call('HGET', seen_key, bid_id)
    return {2, "IDEMPOTENT_DUPLICATE", user_id, tostring(server_time), cached_amount, "0"}
end

-- 4. Load current auction parameters in a single round-trip HMGET
local auction_data = redis.call(
    'HMGET', auction_key, 
    'status', 'end_time', 'stock', 'highest_bid', 'min_increment', 'highest_bidder'
)

local status          = auction_data[1] or 'INACTIVE'
local end_time        = tonumber(auction_data[2]) or 0.0
local stock           = tonumber(auction_data[3]) or 0
local current_highest = tonumber(auction_data[4]) or 0.0
local min_increment   = tonumber(auction_data[5]) or 1.0
local highest_bidder  = auction_data[6] or ''

-- 5. Validate Auction Status & Closing Time
if status ~= 'ACTIVE' then
    return {0, "AUCTION_NOT_ACTIVE", highest_bidder, tostring(server_time), tostring(end_time), "0"}
end

if server_time > end_time then
    -- Auction has officially expired. Mark status as CLOSED in Redis.
    redis.call('HSET', auction_key, 'status', 'CLOSED')
    return {0, "AUCTION_CLOSED", highest_bidder, tostring(server_time), tostring(end_time), "0"}
end

-- 6. Validate Stock Safety (No Overselling)
if stock <= 0 then
    return {0, "OUT_OF_STOCK", highest_bidder, tostring(server_time), tostring(end_time), "0"}
end

-- 7. Validate Strict Monotonicity & Minimum Increment
local min_required = current_highest + min_increment
if new_amount < min_required then
    return {0, "BID_TOO_LOW", tostring(min_required), tostring(server_time), tostring(end_time), "0"}
end

-- 8. Apply Anti-Sniping Soft-Close Extension
--    If a bid arrives in the final N seconds (e.g., 5 seconds before end_time),
--    extend the auction deadline by M seconds to allow counter-bids.
local extended_flag = "0"
local time_remaining = end_time - server_time
if time_remaining <= anti_snipe_window then
    end_time = end_time + anti_snipe_extension
    redis.call('HSET', auction_key, 'end_time', tostring(end_time))
    extended_flag = "1"
end

-- 9. ATOMIC MUTATION: Update Redis Hash
redis.call('HSET', auction_key,
    'highest_bid', tostring(new_amount),
    'highest_bidder', user_id,
    'last_bid_id', bid_id,
    'last_bid_time', tostring(server_time)
)
redis.call('HINCRBY', auction_key, 'bid_count', 1)

-- Store bid_id for idempotency with its accepted price
redis.call('HSET', seen_key, bid_id, tostring(new_amount))

-- 10. WRITE-AHEAD STREAM BUFFER for Asynchronous PostgreSQL Persistence
--     This is non-blocking to disk because it writes to Redis in-memory Stream.
redis.call('XADD', stream_key, '*',
    'auction_id', auction_id,
    'user_id', user_id,
    'bid_id', bid_id,
    'amount', tostring(new_amount),
    'server_time', tostring(server_time)
)

-- 11. REAL-TIME BROADCAST: Publish to Redis Pub/Sub
--     All connected FastAPI gateway instances listening to this channel receive the update
--     and fan it out to their local WebSocket clients.
local broadcast_payload = string.format(
    '{"type":"BID_ACCEPTED","auction_id":"%s","highest_bid":%.2f,"highest_bidder":"%s","server_time":%.4f,"end_time":%.4f,"extended":%s}',
    auction_id, new_amount, user_id, server_time, end_time, (extended_flag == "1" and "true" or "false")
)
redis.call('PUBLISH', pubsub_channel, broadcast_payload)

-- Return success tuple
return {1, tostring(new_amount), user_id, tostring(server_time), tostring(end_time), extended_flag}
