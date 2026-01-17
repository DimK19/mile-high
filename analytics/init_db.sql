CREATE TABLE IF NOT EXISTS window_stats_5m (
  window_start_utc TIMESTAMPTZ NOT NULL,
  window_end_utc   TIMESTAMPTZ NOT NULL,
  direction        TEXT        NOT NULL,
  vehicle_count    INTEGER     NOT NULL,
  avg_speed_kmh    DOUBLE PRECISION,
  created_utc      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (window_start_utc, direction)
);
