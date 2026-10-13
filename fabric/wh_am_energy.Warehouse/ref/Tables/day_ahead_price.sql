CREATE TABLE [ref].[day_ahead_price] (
    [price_start_utc] DATETIME2 (6)  NULL,
    [bidding_zone]    VARCHAR (8000) NULL,
    [price_eur_mwh]   FLOAT (53)     NULL,
    [loaded_at_utc]   DATETIME2 (6)  NULL
);


GO