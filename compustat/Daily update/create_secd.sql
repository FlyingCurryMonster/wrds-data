-- Compustat Daily Security Data (secd)
-- Source: WRDS comp_na_daily_all.secd joined with comp_na_daily_all.company
-- Date range: 2020-01-01 to 2024-12-31

CREATE DATABASE IF NOT EXISTS compustat;

DROP TABLE IF EXISTS compustat.secd;

CREATE TABLE compustat.secd
(
    -- Security identifiers (from secd)
    `tic`             Nullable(String),
    `datadate`        Date32,
    `conm`            Nullable(String),
    `cusip`           Nullable(String),
    `cik`             Nullable(Int64),
    `exchg`           Nullable(Int64),
    `secstat`         Nullable(String),
    `fic`             Nullable(String),
    `tpci`            Nullable(String),

    -- Company identifiers (from company)
    `add1`            Nullable(String),
    `add2`            Nullable(String),
    `add3`            Nullable(String),
    `add4`            Nullable(String),
    `addzip`          Nullable(String),
    `busdesc`         Nullable(String),
    `city`            Nullable(String),
    `conml`           Nullable(String),
    `costat`          Nullable(String),
    `county`          Nullable(String),
    `dldte`           Nullable(Date32),
    `dlrsn`           Nullable(String),
    `ein`             Nullable(String),
    `fax`             Nullable(String),
    `fyrc`            Nullable(Int64),
    `ggroup`          Nullable(Int64),
    `gind`            Nullable(Int64),
    `gsector`         Nullable(Int64),
    `gsubind`         Nullable(Int64),
    `idbflag`         Nullable(String),
    `incorp`          Nullable(String),
    `ipodate`         Nullable(Date32),
    `loc`             Nullable(String),
    `naics`           Nullable(Int64),
    `phone`           Nullable(String),
    `prican`          Nullable(String),
    `prirow`          Nullable(String),
    `priusa`          Nullable(String),
    `sic`             Nullable(Int64),
    `spcindcd`        Nullable(Int64),
    `spcseccd`        Nullable(Int64),
    `spcsrc`          Nullable(String),
    `state`           Nullable(String),
    `stko`            Nullable(Int64),
    `weburl`          Nullable(String),

    -- Daily security data (from secd)
    `adrrc`           Nullable(Float64),
    `ajexdi`          Nullable(Float64),
    `anncdate`        Nullable(Date32),
    `capgn`           Nullable(Float64),
    `capgnpaydate`    Nullable(Date32),
    `cheqv`           Nullable(Float64),
    `cheqvpaydate`    Nullable(Date32),
    `cshoc`           Nullable(Int64),
    `cshtrd`          Nullable(Float64),
    `curcdd`          Nullable(String),
    `curcddv`         Nullable(String),
    `div`             Nullable(Float64),
    `divd`            Nullable(Float64),
    `divdpaydate`     Nullable(Date32),
    `divdpaydateind`  Nullable(String),
    `divsp`           Nullable(Float64),
    `divsppaydate`    Nullable(Date32),
    `dvi`             Nullable(Float64),
    `dvrated`         Nullable(Float64),
    `eps`             Nullable(Float64),
    `epsmo`           Nullable(Int64),
    `iid`             Nullable(String),
    `paydate`         Nullable(Date32),
    `paydateind`      Nullable(String),
    `prccd`           Nullable(Float64),
    `prchd`           Nullable(Float64),
    `prcld`           Nullable(Float64),
    `prcod`           Nullable(Float64),
    `prcstd`          Nullable(Int64),
    `recorddate`      Nullable(Date32),
    `trfd`            Nullable(Float64),
    `gvkey`           String
)
ENGINE = MergeTree
ORDER BY (gvkey, datadate)
SETTINGS index_granularity = 8192;
