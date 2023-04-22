import  pyarrow         as      pa
import  pyarrow.parquet as      pq
from    sqlite3         import  connect
from    time            import  time
from    typing          import  List


DB = connect("../data/futures.db")


def get_futs():

    t0  = time()
    fn  = "get_all_futs"
    cur = DB.cursor()

    recs = cur.execute(
        '''
            SELECT DISTINCT 
                contract_id,
                exchange,
                name,
                month,
                year,
                date,
                open,
                high,
                low,
                settle,
                volume,
                open_interest,
                CAST(julianday(to_date) - julianday(date) AS INT)
            FROM ohlc INNER JOIN metadata USING(contract_id)
            ORDER BY date ASC, year ASC, month ASC;
        '''
    ).fetchall()

    print(f"{fn:15}{time() - t0:0.1f}s")

    return recs


def get_opts():

    t0  = time()
    fn  = "get_all_opts"
    cur = DB.cursor()

    recs = cur.execute(
        '''
            SELECT DISTINCT 
                date,
                name,
                strike,
                expiry,
                call,
                last_traded,
                settle,
                settle_delta,
                high_limit,
                low_limit,
                high_bid,
                low_bid,
                previous_volume,
                previous_interest,
                underlying_symbol,
                underlying_exchange,
                underlying_id
            FROM cme_opts
            ORDER BY date ASC;
        '''
    ).fetchall()

    print(f"{fn:15}{time() - t0:0.1f}s")

    return recs


def futs_to_pq(file: str, recs: List):

    t0 = time()
    fn = "futs_to_pq"
    
    contract_id = pa.array([ rec[0] for rec in recs ])
    exchange    = pa.array([ rec[1] for rec in recs ])
    name        = pa.array([ rec[2] for rec in recs ])
    month       = pa.array([ rec[3] for rec in recs ])
    year        = pa.array([ rec[4] for rec in recs ])
    date        = pa.array([ rec[5] for rec in recs ])
    open        = pa.array([ rec[6] if type(rec[6]) is float else None for rec in recs ])
    high        = pa.array([ rec[7] if type(rec[7]) is float else None for rec in recs ])
    low         = pa.array([ rec[8] if type(rec[8]) is float else None for rec in recs ])
    settle      = pa.array([ rec[9] if type(rec[9]) is float else None for rec in recs ])
    volume      = pa.array([ rec[10] if type(rec[10]) is int else None for rec in recs ])
    oi          = pa.array([ rec[11] if type(rec[11]) is int else None for rec in recs ])
    dte         = pa.array([ rec[12] if type(rec[12]) is int else None for rec in recs ])

    table   = pa.table(
                        [ 
                            contract_id, exchange, name, month, year, date, 
                            open, high, low, settle, volume, oi, dte
                        ],
                        names = [ 
                            "contract_id", "exchange", "name", "month", "year", "date",
                            "open", "high", "low", "settle", "volume", "oi", "dte"
                        ]
                    )

    pq.write_table(table, file)

    print(f"{fn:15}{time() - t0:0.1f}s")


def opts_to_pq(file: str, recs: List):

    t0 = time()
    fn = "opts_to_pq"

    date                = pa.array([ rec[0] for rec in recs ])
    name                = pa.array([ rec[1] for rec in recs ])
    strike              = pa.array([ rec[2] if type(rec[2]) is float else None for rec in recs ])
    expiry              = pa.array([ rec[3] for rec in recs ])
    call                = pa.array([ rec[4] if type(rec[4]) is int else None for rec in recs ])
    last_traded         = pa.array([ rec[5] for rec in recs ])
    settle              = pa.array([ rec[6] if type(rec[6]) is float else None for rec in recs ])
    settle_delta        = pa.array([ rec[7] if type(rec[7]) is float else None for rec in recs ])
    high_limit          = pa.array([ rec[8] if type(rec[8]) is float else None for rec in recs ])
    low_limit           = pa.array([ rec[9] if type(rec[9]) is float else None for rec in recs ])
    high_bid            = pa.array([ rec[10] if type(rec[10]) is float else None for rec in recs ])
    low_bid             = pa.array([ rec[11] if type(rec[11]) is float else None for rec in recs ])
    previous_volume     = pa.array([ rec[12] if type(rec[12]) is int else None for rec in recs ])
    previous_interest   = pa.array([ rec[13] if type(rec[13]) is int else None for rec in recs ])
    underlying_symbol   = pa.array([ rec[14] for rec in recs ])
    underlying_exchange = pa.array([ rec[15] for rec in recs ])
    underlying_id       = pa.array([ rec[16] for rec in recs ])

    table   = pa.table(
                        [   date, name, strike, expiry, call, last_traded, settle, settle_delta, high_limit, 
                            low_limit, high_bid, low_bid, previous_volume, previous_interest, underlying_symbol,
                            underlying_exchange, underlying_id
                        ],
                        names = [    
                            "date", "name", "strike", "expiry", "call", "last_traded", "settle", "settle_delta",
                            "high_limit", "low_limit", "high_bid", "low_bid", "previous_volume", "previous_interest",
                            "underlying_symbol", "underlying_exchange", "underlying_id"
                        ]
                    )

    pq.write_table(table, file)

    print(f"{fn:15}{time() - t0:0.1f}s")


def from_pq(file: str):

    t0 = time()
    fn = "from_pq"

    recs = pq.read_table(file)

    print(f"{fn:15}{time() - t0:0.1f}s")

    return recs


if __name__ == "__main__":
    
    recs = get_futs()
    futs_to_pq("futs.parquet", recs)
    
    recs = get_opts()
    opts_to_pq("opts.parquet", recs)

