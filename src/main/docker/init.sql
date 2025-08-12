DROP TABLE IF EXISTS payment_summary;

CREATE TABLE payment_summary (
    correlationId UUID PRIMARY KEY,
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMP NOT NULL,
    payment_strategy INTEGER NOT NULL
);

CREATE INDEX payments_requested_at ON payment_summary (requested_at);
