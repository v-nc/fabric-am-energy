-- Warehouse wh_am_energy: day-ahead market prices and the energy-cost mart.
-- Run in the Warehouse SQL editor, one block (between GO lines) at a time, or with any T-SQL client. Safe to re-run.
-- ref.day_ahead_price is created and loaded by the Dataflow Gen2 df_day_ahead_prices (dataflow/day_ahead_price.pq).

IF SCHEMA_ID('ref') IS NULL EXEC('CREATE SCHEMA ref');
IF SCHEMA_ID('mart') IS NULL EXEC('CREATE SCHEMA mart');
GO

-- One row per 3D printer version and UTC hour. The fixed time-of-use cost is all-in (energy, grid, levies); the
-- day-ahead price is the energy component only, so the two are compared in shape, not added up.
IF OBJECT_ID('mart.energy_cost_hourly') IS NULL
CREATE TABLE mart.energy_cost_hourly (
    machine_sk     VARCHAR(32)  NOT NULL,  -- dim_machine version valid in that hour (SCD type 2)
    hour_start_utc DATETIME2(6) NOT NULL,
    date_key       INT          NOT NULL,  -- local date as yyyymmdd, the key of gold.dim_date
    hour_local     INT          NOT NULL,  -- 0..23, Europe/Zurich
    kwh            FLOAT        NOT NULL,
    cost_tou_eur   FLOAT        NOT NULL,  -- fixed time-of-use tariff, from gold
    spot_eur_mwh   FLOAT        NULL,      -- day-ahead price of the hour, bidding zone CH
    cost_spot_eur  FLOAT        NULL       -- the hour's energy at the day-ahead price
);
GO

-- Rebuilds the mart from gold (cross-database query to the Lakehouse SQL analytics endpoint) and the prices, in one
-- transaction: readers see the old or the new mart, never a half-built one. Full rebuild, like gold: about 90,000
-- rows a year.
CREATE OR ALTER PROCEDURE mart.usp_refresh_energy_cost
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    DELETE FROM mart.energy_cost_hourly;

    INSERT INTO mart.energy_cost_hourly
        (machine_sk, hour_start_utc, date_key, hour_local, kwh, cost_tou_eur, spot_eur_mwh, cost_spot_eur)
    SELECT h.machine_sk, h.hour_start_utc, h.date_key, h.hour_local, h.kwh, h.cost_tou_eur,
           p.price_eur_mwh,
           h.kwh * p.price_eur_mwh / 1000.0
    FROM (
        SELECT f.machine_sk,
               DATEADD(hour, DATEPART(hour, f.bucket_start), CAST(CAST(f.bucket_start AS DATE) AS DATETIME2(6)))
                   AS hour_start_utc,
               MIN(f.date_key) AS date_key,
               MIN(DATEPART(hour, f.bucket_start_local)) AS hour_local,
               SUM(f.kwh) AS kwh,
               SUM(f.cost) AS cost_tou_eur
        FROM lh_energy.gold.fact_energy_15min AS f
        WHERE f.machine_sk IS NOT NULL
        GROUP BY f.machine_sk,
                 DATEADD(hour, DATEPART(hour, f.bucket_start), CAST(CAST(f.bucket_start AS DATE) AS DATETIME2(6)))
    ) AS h
    LEFT JOIN ref.day_ahead_price AS p
           ON p.price_start_utc = h.hour_start_utc
          AND p.bidding_zone = 'CH';

    COMMIT TRANSACTION;
END
GO

-- For SQL users: the park's load-weighted day-ahead price against the market's time-weighted average, by month.
CREATE OR ALTER VIEW mart.v_price_by_month
AS
SELECT d.yyyymm,
       SUM(d.kwh)                                           AS kwh,
       SUM(d.cost_spot_eur)                                 AS cost_spot_eur,
       SUM(d.cost_spot_eur) * 1000.0 / NULLIF(SUM(d.kwh), 0) AS load_weighted_eur_mwh,
       AVG(d.spot_eur_mwh)                                  AS time_weighted_eur_mwh
FROM (
    SELECT hour_start_utc,
           date_key / 100           AS yyyymm,
           SUM(kwh)                 AS kwh,
           SUM(cost_spot_eur)       AS cost_spot_eur,
           MAX(spot_eur_mwh)        AS spot_eur_mwh
    FROM mart.energy_cost_hourly
    GROUP BY hour_start_utc, date_key / 100
) AS d
GROUP BY d.yyyymm;
GO
