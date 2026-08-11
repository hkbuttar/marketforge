BEGIN TRANSACTION;

CREATE SCHEMA IF NOT EXISTS canonical;

CREATE TABLE IF NOT EXISTS canonical.sector (
    sector_id UUID PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS canonical.industry (
    industry_id UUID PRIMARY KEY,
    sector_id UUID NOT NULL REFERENCES canonical.sector(sector_id),
    name VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (sector_id, name)
);

CREATE TABLE IF NOT EXISTS canonical.security (
    security_id UUID PRIMARY KEY,
    security_type VARCHAR NOT NULL,
    exchange_code VARCHAR,
    name VARCHAR NOT NULL,
    currency VARCHAR NOT NULL DEFAULT 'USD',
    sector_id UUID REFERENCES canonical.sector(sector_id),
    industry_id UUID REFERENCES canonical.industry(industry_id),
    active_from DATE,
    active_to DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    CHECK (active_to IS NULL OR active_from IS NULL OR active_to > active_from)
);

CREATE TABLE IF NOT EXISTS canonical.security_identifier (
    security_identifier_id UUID PRIMARY KEY,
    security_id UUID NOT NULL REFERENCES canonical.security(security_id),
    identifier_type VARCHAR NOT NULL,
    identifier_value VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    ingested_at TIMESTAMPTZ NOT NULL,
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    UNIQUE (identifier_type, identifier_value, source, valid_from),
    UNIQUE (security_id, identifier_type, source, valid_from)
);

CREATE TABLE IF NOT EXISTS canonical.trading_day (
    exchange_code VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    session_open TIMESTAMPTZ,
    session_close TIMESTAMPTZ,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange_code, trade_date),
    UNIQUE (source, source_record_id),
    CHECK (session_close IS NULL OR session_open IS NULL OR session_close > session_open)
);

CREATE TABLE IF NOT EXISTS canonical.price_bar (
    price_bar_id UUID PRIMARY KEY,
    security_id UUID NOT NULL REFERENCES canonical.security(security_id),
    trade_date DATE NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume BIGINT NOT NULL,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source, source_record_id),
    UNIQUE (security_id, trade_date, source),
    CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0),
    CHECK (volume >= 0),
    CHECK (high >= low AND high >= open AND high >= close),
    CHECK (low <= open AND low <= close)
);

CREATE TABLE IF NOT EXISTS canonical.fundamental_observation (
    fundamental_observation_id UUID PRIMARY KEY,
    security_id UUID NOT NULL REFERENCES canonical.security(security_id),
    metric_name VARCHAR NOT NULL,
    period_start DATE,
    period_end DATE NOT NULL,
    period_type VARCHAR NOT NULL,
    filed_at TIMESTAMPTZ,
    value DOUBLE NOT NULL,
    unit VARCHAR NOT NULL,
    currency VARCHAR,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source, source_record_id),
    CHECK (period_start IS NULL OR period_end >= period_start),
    CHECK (filed_at IS NULL OR filed_at <= ingested_at)
);

CREATE TABLE IF NOT EXISTS canonical.earnings_event (
    earnings_event_id UUID PRIMARY KEY,
    security_id UUID NOT NULL REFERENCES canonical.security(security_id),
    event_timestamp TIMESTAMPTZ NOT NULL,
    fiscal_period_end DATE,
    event_status VARCHAR NOT NULL,
    eps_estimate DOUBLE,
    eps_actual DOUBLE,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source, source_record_id)
);

CREATE TABLE IF NOT EXISTS canonical.macro_observation (
    macro_observation_id UUID PRIMARY KEY,
    series_id VARCHAR NOT NULL,
    observation_date DATE NOT NULL,
    released_at TIMESTAMPTZ,
    value DOUBLE NOT NULL,
    unit VARCHAR NOT NULL,
    frequency VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source, source_record_id),
    CHECK (released_at IS NULL OR released_at <= ingested_at)
);

CREATE TABLE IF NOT EXISTS canonical.news_event (
    news_event_id UUID PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    headline VARCHAR NOT NULL,
    url VARCHAR,
    publisher VARCHAR,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source, source_record_id)
);

CREATE TABLE IF NOT EXISTS canonical.news_event_security (
    news_event_id UUID NOT NULL REFERENCES canonical.news_event(news_event_id),
    security_id UUID NOT NULL REFERENCES canonical.security(security_id),
    relevance_score DOUBLE,
    PRIMARY KEY (news_event_id, security_id),
    CHECK (relevance_score IS NULL OR relevance_score BETWEEN 0 AND 1)
);

COMMIT;
