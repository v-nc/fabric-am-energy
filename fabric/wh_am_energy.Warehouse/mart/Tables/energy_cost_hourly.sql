CREATE TABLE [mart].[energy_cost_hourly] (
    [machine_sk]     VARCHAR (32)  NOT NULL,
    [hour_start_utc] DATETIME2 (6) NOT NULL,
    [date_key]       INT           NOT NULL,
    [hour_local]     INT           NOT NULL,
    [kwh]            FLOAT (53)    NOT NULL,
    [cost_tou_eur]   FLOAT (53)    NOT NULL,
    [spot_eur_mwh]   FLOAT (53)    NULL,
    [cost_spot_eur]  FLOAT (53)    NULL
);


GO