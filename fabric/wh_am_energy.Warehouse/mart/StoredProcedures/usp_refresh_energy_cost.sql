-- Rebuilds the mart from gold (cross-database query to the Lakehouse SQL analytics endpoint) and the prices, in one
-- transaction: readers see the old or the new mart, never a half-built one. Full rebuild, like gold: about 90,000
-- rows a year.
CREATE   PROCEDURE mart.usp_refresh_energy_cost
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