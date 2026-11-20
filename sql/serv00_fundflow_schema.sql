CREATE TABLE IF NOT EXISTS sector_master (
    source text NOT NULL,
    sector_code text NOT NULL,
    sector_type text NOT NULL,
    sector_name text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, sector_code)
);

CREATE TABLE IF NOT EXISTS sector_flow_1m (
    minute_ts timestamptz NOT NULL,
    trade_date date NOT NULL,
    source text NOT NULL,
    market_phase text NOT NULL,
    sector_code text NOT NULL,
    sector_type text NOT NULL,
    sector_name text NOT NULL,
    avg_price double precision,
    pct_change double precision,
    turnover double precision,
    in_amount numeric(22,4),
    out_amount numeric(22,4),
    net_amount numeric(22,4),
    net_ratio double precision,
    leader_code text,
    leader_name text,
    leader_price double precision,
    leader_pct_change double precision,
    leader_net_ratio double precision,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, sector_code, minute_ts)
);

CREATE INDEX IF NOT EXISTS idx_flow_1m_sector_time ON sector_flow_1m(source, sector_code, minute_ts DESC);
CREATE INDEX IF NOT EXISTS idx_flow_1m_time ON sector_flow_1m(minute_ts DESC);
CREATE INDEX IF NOT EXISTS idx_flow_1m_trade_date ON sector_flow_1m(trade_date);

CREATE TABLE IF NOT EXISTS sector_latest (
    source text NOT NULL,
    sector_code text NOT NULL,
    sector_type text NOT NULL,
    sector_name text NOT NULL,
    minute_ts timestamptz NOT NULL,
    market_phase text NOT NULL,
    avg_price double precision,
    pct_change double precision,
    turnover double precision,
    in_amount numeric(22,4),
    out_amount numeric(22,4),
    net_amount numeric(22,4),
    net_ratio double precision,
    leader_code text,
    leader_name text,
    leader_price double precision,
    leader_pct_change double precision,
    leader_net_ratio double precision,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, sector_code)
);
