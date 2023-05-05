from    config              import  CONFIG
from    contract_settings   import  CONTRACT_SETTINGS
from    csv                 import  reader
from    datetime            import  datetime
import  polars              as      pl
from    requests            import  get
from    time                import  time


DATE_FMT            = "%Y-%m-%d"
ENABLED_FUTS        = {
                        settings["globex"] : settings
                        for _, settings in CONTRACT_SETTINGS.items()
                        if "globex" in settings
                    }
ENABLED_OPTS        = {
                        definition["opts"]: {
                            "globex":   definition["globex"], 
                            "exchange": definition["exchange"]
                        }
                        for _, definition in CONTRACT_SETTINGS.items()
                        if "opts" in definition
                    }
MONTHS              = {
                        1: "F",
                        2: "G",
                        3: "H",
                        4: "J",
                        5: "K",
                        6: "M",
                        7: "N",
                        8: "Q",
                        9: "U",
                        10: "V",
                        11: "X",
                        12: "Z"
                    }

EXPECTED_SETTLEMENT_COLS    = [ 'Product', 'Symbol', 'Expiration Date', 'Price' ]
EXPECTED_VX_COLS            = [ 'Trade Date', 'Futures', 'Open', 'High', 'Low', 'Close', 'Settle', 'Change', 'Total Volume', 'EFP', 'Open Interest' ]


def update(date: str):

    t0 = time()

    futs_db = pl.read_parquet(CONFIG["futs_db"])
    
    id_col          = []
    exchange_col    = []
    name_col        = []
    month_col       = []
    year_col        = []
    date_col        = []
    open_col        = []
    high_col        = []
    low_col         = []
    settle_col      = []
    volume_col      = []
    oi_col          = []
    dte_col         = []

    cboe_settlements_url    = CONFIG["cboe_settlements_url"].format(date)
    vx_url_template         = CONFIG["vx_url"]
    res                     = get(cboe_settlements_url)

    if res.status_code != 200:

        print(f"error: cboe settlements download failed {res.status_code}")

        return

    txt         = res.text
    settle_rows = reader(txt.splitlines())
    headers     = next(settle_rows)         # skip header

    if headers != EXPECTED_SETTLEMENT_COLS:

        print(f"error: unexpected column format in cboe settlements file")

    for settle_row in settle_rows:

        # settlements record:

        # 0 Product         'VX'
        # 1 Symbol          'VX/G2', 'VX06/G2'
        # 2 Expiration Date '2022-06-16'
        # 3 Price           '26.8734'

        if settle_row[0] != "VX" or len(settle_row[1]) > 5:

            continue
        
        expiry  = settle_row[2]
        url     = vx_url_template.format(settle_row[2])
        res     = get(url)

        if res.status_code != 200:

            print(f"error: vx file download failed {res.status_code}: {url}")

            continue

        # vx record:

        # 0 Trade Date      2021-01-20
        # 1 Futures         F (Jan 2021)
        # 2 Open            22.9000
        # 3 High            23.0000
        # 4 Low             22.2000
        # 5 Close           22.5000
        # 6 Settle          22.59
        # 7 Change          -0.635
        # 8 Total Volume    908
        # 9 EFP             0
        # 10 Open Interest  16066

        vx_rows = list(reader(res.text.splitlines()))
        dates   = [ vx_row[0] for vx_row in vx_rows ]

        headers = vx_rows[0]

        if headers != EXPECTED_VX_COLS:

            print(f"error: unexpected column format in vx file")

        selected = vx_rows[-1] if date not in dates else vx_rows[dates.index(date)]

        month       = selected[1][0]
        year        = selected[1][-5:-1]
        id          = f"CFE_VX{month}{year}"
        date_       = selected[0]
        open        = float(selected[2])    if selected[2]  != '' else None
        high        = float(selected[3])    if selected[3]  != '' else None
        low         = float(selected[4])    if selected[4]  != '' else None
        settle      = float(selected[6])    if selected[6]  != '' else None
        volume      = int(selected[8])      if selected[8]  != '' else None
        oi          = int(selected[10])     if selected[10] != '' else None 
        dte         = (datetime.strptime(expiry, DATE_FMT) - datetime.strptime(date_, DATE_FMT)).days

        id_col.append(id)
        exchange_col.append("CFE")
        name_col.append("VX")
        month_col.append(month)
        year_col.append(year)
        date_col.append(date)
        open_col.append(open)
        high_col.append(high)
        low_col.append(low)
        settle_col.append(settle)
        volume_col.append(volume)
        oi_col.append(oi)
        dte_col.append(dte)

    cols = pl.from_dict(
        {
            "contract_id":  id_col,
            "exchange":     exchange_col,
            "name":         name_col,
            "month":        month_col,
            "year":         year_col,
            "date":         date_col,
            "open":         open_col,
            "high":         high_col,
            "low":          low_col,
            "settle":       settle_col,
            "volume":       volume_col,
            "oi":           oi_col,
            "dte":          dte_col
        },
        schema = {
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
        }
    )
    
    futs_db.extend(cols)
    futs_db = futs_db.unique(maintain_order = True).sort( [ "contract_id", "date" ] )
    futs_db.write_parquet(CONFIG["futs_db"])

    print(f"{'update_cboe.update':30}{'all':30}{time() - t0:0.1f}")