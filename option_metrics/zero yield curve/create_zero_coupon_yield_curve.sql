CREATE DATABASE IF NOT EXISTS option_metrics;
DROP TABLE IF EXISTS option_metrics.zero_coupon_yield_curve;

CREATE TABLE option_metrics.zero_coupon_yield_curve (
    date    Date32,
    days    UInt16,
    rate    Float64
)
ENGINE = MergeTree()
ORDER BY (date, days);
