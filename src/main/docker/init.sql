DROP TABLE IF EXISTS payment_summary;

CREATE TABLE payment_summary (
    id SERIAL PRIMARY KEY,
    correlationId UUID UNIQUE NOT NULL,
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMP NOT NULL,
    payment_strategy INTEGER NOT NULL
);

CREATE INDEX payments_requested_at ON payment_summary (requested_at);
CREATE INDEX correlation_id_at ON payment_summary (correlationId);

