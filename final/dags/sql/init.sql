CREATE SCHEMA IF NOT EXISTS marketplace;

CREATE TABLE IF NOT EXISTS marketplace.user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    device TEXT
);

CREATE TABLE IF NOT EXISTS marketplace.user_session_pages (
    session_id TEXT,
    page TEXT
);

CREATE TABLE IF NOT EXISTS marketplace.user_session_actions (
    session_id TEXT,
    action TEXT
);

CREATE TABLE IF NOT EXISTS marketplace.event_logs (
    event_id TEXT PRIMARY KEY,
    timestamp TIMESTAMP,
    event_type TEXT
);

CREATE TABLE IF NOT EXISTS marketplace.event_log_details (
    event_id TEXT,
    page TEXT,
    product_id TEXT
);

CREATE TABLE IF NOT EXISTS marketplace.support_tickets (
    ticket_id TEXT PRIMARY KEY,
    user_id TEXT,
    status TEXT,
    issue_type TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marketplace.support_ticket_messages (
    ticket_id TEXT,
    sender TEXT,
    message TEXT,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marketplace.user_recommendations (
    user_id TEXT,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marketplace.user_recommendation_products (
    user_id TEXT,
    product_id TEXT
);

CREATE TABLE IF NOT EXISTS marketplace.moderation_queue (
    review_id TEXT PRIMARY KEY,
    user_id TEXT,
    product_id TEXT,
    review_text TEXT,
    rating INT,
    moderation_status TEXT,
    submitted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marketplace.moderation_flags (
    review_id TEXT,
    flag TEXT
);