CREATE TABLE IF NOT EXISTS sector_master (
 source text NOT NULL, sector_code text NOT NULL, sector_type text NOT NULL, sector_name text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(source,sector_code));
CREATE TABLE IF NOT EXISTS sector_flow_1m (
 snapshot_time timestamptz NOT NULL, trade_date date NOT NULL, source text NOT NULL, sector_code text NOT NULL, sector_type text NOT NULL, avg_price double precision, pct_change double precision, turnover double precision, in_amount numeric(20,4), out_amount numeric(20,4), net_amount numeric(20,4), net_ratio double precision, PRIMARY KEY(source,sector_code,snapshot_time));
CREATE INDEX IF NOT EXISTS idx_sector_flow_1m_sector_time ON sector_flow_1m(source,sector_code,snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_sector_flow_1m_time ON sector_flow_1m(snapshot_time DESC);
CREATE TABLE IF NOT EXISTS sector_latest (LIKE sector_flow_1m INCLUDING DEFAULTS);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sector_latest ON sector_latest(source,sector_code);
