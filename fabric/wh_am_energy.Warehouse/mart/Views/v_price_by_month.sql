-- For SQL users: the park's load-weighted day-ahead price against the market's time-weighted average, by month.
CREATE   VIEW mart.v_price_by_month
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