import polars as pl


CONFIG = {
    "futs_db": "./futs.parquet",
    "opts_db": "./opts.parquet",
    "cme_ftp":  "ftp.cmegroup.com",
    "cme_files_new": [
        "cbt.settle.s.csv",
        "cme.settle.s.csv",
        "comex.settle.s.csv",
        "nymex.settle.s.csv"
    ],
    "cme_files_old": [
        "cbt.settle.{0}.s.csv",
        "cme.settle.{0}.s.csv",
        "comex.settle.{0}.s.csv",
        "nymex.settle.{0}.s.csv"
    ],
    "cboe_settlements_url": "https://www.cboe.com/us/futures/market_statistics/settlement/csv?dt={0}",
    "vx_url":               "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{0}.csv",
    "wasde": {
        "db_path":      "./wasde.parquet",
        "base_url":     "https://www.usda.gov",
        "links":        "/oce/commodity-markets/wasde/historical-wasde-report-data",
        "new_pattern":  "/sites/default/files/documents/oce-wasde-report-data-\d{4}-\d{2}.csv",
        "arch_pattern": "/sites/default/files/documents/oce-wasde-report-data-\d{4}-\d{2}-to-\d{4}-\d{2}.zip"
    },
    "spot": {
        "VX": {
            "url":  "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
            "file": "./VX.parquet"
        }
    },
    "futs_schema": {
        "contract_id":  pl.Utf8,
        "exchange":     pl.Utf8,
        "name":         pl.Utf8,
        "month":        pl.Utf8,
        "year":         pl.Utf8,
        "date":         pl.Utf8,
        "open":         pl.Float64,
        "high":         pl.Float64,
        "low":          pl.Float64,
        "settle":       pl.Float64,
        "volume":       pl.Int64,
        "oi":           pl.Int64,
        "dte":          pl.Int64
    },
    "opts_schema": {
        "date":                 pl.Utf8,
        "name":                 pl.Utf8,
        "strike":               pl.Float64,
        "expiry":               pl.Utf8,
        "call":                 pl.Int64,
        "last_traded":          pl.Utf8,
        "settle":               pl.Float64,
        "settle_delta":         pl.Float64,
        "high_limit":           pl.Float64,
        "low_limit":            pl.Float64,
        "high_bid":             pl.Float64,
        "low_bid":              pl.Float64,
        "previous_volume":      pl.Int64,
        "previous_interest":    pl.Int64,
        "underlying_symbol":    pl.Utf8,
        "underlying_exchange":  pl.Utf8,
        "underlying_id":        pl.Utf8
    },
    "wasde_schema": {
        "WasdeNumber":              pl.Int64,
        "ReportDate":               pl.Utf8,
        "ReportTitle":              pl.Utf8,
        "Attribute":                pl.Utf8,
        "ReliabilityProjection":    pl.Utf8,
        "Commodity":                pl.Utf8,
        "Region":                   pl.Utf8,
        "MarketYear":               pl.Utf8,
        "ProjEstFlag":              pl.Utf8,
        "AnnualQuarterFlag":        pl.Utf8,
        "Value":                    pl.Float64,
        "Unit":                     pl.Utf8,
        "ReleaseDate":              pl.Utf8,
        "ReleaseTime":              pl.Utf8,
        "ForecastYear":             pl.Int64,
        "ForecastMonth":            pl.Int64
    }
}