from    csv         import  reader
from    datetime    import  datetime
from    ftplib      import  FTP
from    json        import  loads
import  polars      as      pl
from    time        import  time
from    typing      import  List


CONFIG              = loads(open("./config.json", "r").read())
CONTRACT_SETTINGS   = loads(open("./contract_settings.json", "r").read())
DATE_FMT            = "%Y-%m-%d"
ENABLED_CONTRACTS   = {
                        settings["globex"] : settings
                        for _, settings in CONTRACT_SETTINGS.items()
                        if "globex" in settings
                    }
CALENDAR            = {
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


# cme record format

# 0  BizDt          current date        YYYY-MM-DD
# 1  Sym            globex symbol       ZN
# 2  ID             clearing symbol     21
# 3  StrkPx         strike price        float
# 4  SecTyp         security type       FUT, OOF, COMBO, OOC
# 5  MMY            exp y+m             YYYYMM
# 6  MatDt          ?                   YYYY-MM-DD          
# 7  PutCall        put or call         1 or 0
# 8  Exch           exchange            NYMEX
# 9  Desc           always empty        "" 
# 10 LastTrdDt      last trade date     YYYY-MM-DD
# 11 BidPrice       ?                   float
# 12 OpeningPrice   open                float
# 13 SettlePrice    settle              float
# 14 SettleDelta    delta settle?       float
# 15 HighLimit      high bid/offer?     float
# 16 LowLimit       low bid/offer?      float
# 17 DHighPrice     high                float
# 18 DLowPrice      low                 float
# 19 HighBid        ?                   float    
# 20 LowBid         ?                   float
# 21 PrevDayVol     volume              int
# 22 PrevDayOI      OI                  int
# 23 FixingPrice    ?                   float
# 24 UndlyExch      underlying exchange NYMEX
# 25 UndlyID        underlying clearing 21
# 26 UndlySecTyp    underlying sectype  FUT, OOF, COMBO, OOC
# 27 UndlyMMY       exp y+m             YYYYMM
# 28 BankBusDay     ?                   YYYY-MM-DD


EXPECTED_COLS = [
    "BizDt",
    "Sym",
    "ID",
    "StrkPx",
    "SecTyp",
    "MMY",
    "MatDt",
    "PutCall",
    "Exch",
    "Desc",
    "LastTrdDt",
    "BidPrice",
    "OpeningPrice",
    "SettlePrice",
    "SettleDelta",
    "HighLimit",
    "LowLimit",
    "DHighPrice",
    "DLowPrice",
    "HighBid",
    "LowBid",
    "PrevDayVol",
    "PrevDayOI",
    "FixingPrice",
    "UndlyExch",
    "UndlyID",
    "UndlySecTyp",
    "UndlyMMY",
    "BankBusDay"
]


def insert_futs_rows(futs_db: pl.DataFrame, rows: List):

    recs = []
    rows = reader(rows)
    
    next(rows)  # skip header

    for row in rows:

        if row[4] != "FUT" or row[1] not in ENABLED_CONTRACTS:

            continue

        symbol      = row[1]
        exchange    = ENABLED_CONTRACTS[symbol]["exchange"]
        delivery    = row[5]
        year        = delivery[0:4]
        month       = None

        # format varies: yyyymm vs yyyymmdd

        if len(delivery) == 6:

            month = CALENDAR[int(delivery[4:])]

        else:

            month = CALENDAR[int(delivery[4:6])]

        # propagate settle for contracts taht didn't trade

        scale = ENABLED_CONTRACTS[symbol]["scale"]

        id          = f"{exchange}_{symbol}{month}{year}"
        date        = row[0]
        settle      = float(row[13]) * scale if row[13] != "" else None
        open        = float(row[12]) * scale if row[12] != "" else None
        high        = float(row[17]) * scale if row[17] != "" else None
        low         = float(row[18]) * scale if row[18] != "" else None
        vol         = int(row[21])           if row[21] != "" else None
        oi          = int(row[22])           if row[22] != "" else None
        exp_date    = row[10]
        dte         = (datetime.strptime(exp_date, DATE_FMT) - datetime.strptime(date, DATE_FMT)).days

        recs.append(
            [
                id,
                exchange,
                symbol,
                month,
                year,
                date,
                open,
                high,
                low,
                settle,
                vol,
                oi,
                dte
            ]
        )

    pass


def insert_opts_rows(opts_db: pl.DataFrame, rows: List):

    pass


def update(date: str, new: bool):

    t0 = time()

    futs_db = pl.read_parquet(CONFIG["futs_db"])
    #opts_db = pl.read_parquet(CONFIG["opts_db"])
    files   = CONFIG["cme_files_new"]

    if not new:

        yyyymmdd    = date.replace("-", "")
        files       = [
                        file.format(yyyymmdd)
                        for file in CONFIG["cme_files_old"]
                    ]

    ftp = FTP(CONFIG["cme_ftp"])
    
    ftp.login()
    ftp.cwd("settle")

    for file in files:

        rows = []
        
        ftp.retrlines(f"RETR {file}", rows.append)

        if rows[0].split(",") != EXPECTED_COLS:

            print(f"error: unexpected column format in {file}")

        insert_futs_rows(futs_db, rows)
        #insert_opts_rows(opts_db, rows)

        pass

    print(f"{'update_cme.update':30}{time() - t0:0.1f}s")

    pass
        