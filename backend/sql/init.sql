-- Schema for Flash Auction Relational Durability
CREATE TABLE IF NOT EXISTS auctions (
    id VARCHAR(64) PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,
    start_time DOUBLE PRECISION NOT NULL,
    end_time DOUBLE PRECISION NOT NULL,
    initial_price NUMERIC(12, 2) NOT NULL,
    min_increment NUMERIC(12, 2) NOT NULL,
    current_highest_bid NUMERIC(12, 2) NOT NULL,
    highest_bidder VARCHAR(64) NOT NULL,
    stock INT NOT NULL DEFAULT 1,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bids (
    bid_id VARCHAR(64) PRIMARY KEY,
    auction_id VARCHAR(64) NOT NULL REFERENCES auctions(id),
    user_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    server_timestamp DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bids_auction_amount ON bids(auction_id, amount DESC);
CREATE INDEX IF NOT EXISTS idx_bids_auction_timestamp ON bids(auction_id, server_timestamp ASC);

-- Seed initial test auction
INSERT INTO auctions (
    id, item_name, start_time, end_time, initial_price, min_increment, current_highest_bid, highest_bidder, stock, status
) VALUES (
    'auction_alpha', 
    'Vintage Rolex Daytona 1968', 
    EXTRACT(EPOCH FROM NOW()), 
    EXTRACT(EPOCH FROM NOW()) + 300, 
    100.00, 
    5.00, 
    100.00, 
    'reserve_bot', 
    1, 
    'ACTIVE'
) ON CONFLICT (id) DO NOTHING;
