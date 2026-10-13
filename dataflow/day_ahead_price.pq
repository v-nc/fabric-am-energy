// Query day_ahead_price of the Dataflow Gen2 df_day_ahead_prices (paste into Power Query: Advanced editor).
// Hourly day-ahead prices of the Swiss bidding zone (CH), EUR/MWh, from the Energy-Charts API (Fraunhofer ISE).
// Data: Bundesnetzagentur | SMARD.de, licensed CC BY 4.0.
// Destination: Warehouse wh_am_energy, new table ref.day_ahead_price, update method Replace (fixed schema).
let
    // The whole simulated history plus the published day-ahead prices for tomorrow: about 9,000 rows, so a full
    // refresh is simpler than incremental refresh and still takes seconds.
    FromDate = #date(2025, 9, 30),
    ToDate = Date.AddDays(Date.From(DateTimeZone.FixedUtcNow()), 2),
    // Static base URL with RelativePath and Query: the service can refresh it (no dynamic data source).
    Response = Json.Document(
        Web.Contents(
            "https://api.energy-charts.info",
            [
                RelativePath = "price",
                Query = [
                    bzn = "CH",
                    start = Date.ToText(FromDate, [Format = "yyyy-MM-dd"]),
                    end = Date.ToText(ToDate, [Format = "yyyy-MM-dd"])
                ]
            ]
        )
    ),
    // The API returns two parallel arrays: Unix timestamps (start of each hour, UTC) and prices.
    Prices = Table.FromColumns({Response[unix_seconds], Response[price]}, {"unix_seconds", "price_eur_mwh"}),
    WithStart = Table.AddColumn(
        Prices, "price_start_utc", each #datetime(1970, 1, 1, 0, 0, 0) + #duration(0, 0, 0, [unix_seconds]), type datetime
    ),
    WithZone = Table.AddColumn(WithStart, "bidding_zone", each "CH", type text),
    WithLoadTime = Table.AddColumn(
        WithZone, "loaded_at_utc", each DateTimeZone.RemoveZone(DateTimeZone.FixedUtcNow()), type datetime
    ),
    // The Warehouse takes no DateTimeZone type, so times stay in UTC; prices can be null for a missing hour.
    Typed = Table.TransformColumnTypes(WithLoadTime, {{"price_eur_mwh", type nullable number}}),
    Result = Table.SelectColumns(Typed, {"price_start_utc", "bidding_zone", "price_eur_mwh", "loaded_at_utc"})
in
    Result
