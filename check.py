import  polars          as      pl
from    sys             import  argv
from    time            import  time
from    ..reports.util  import  get_groups


def get_latest(symbol:str, start:str, end:str):

    term_days = get_groups(symbol, start, end)

    # check records for most recent day

    for row in term_days[-1]:

        print(row)

    print(f"num_days: {len(term_days)}")


def get_dates(symbol: str):

    df = pl.read_parquet(
            "./futs.parquet"
        ).filter(
            (pl.col("name") == symbol) 
        ).select(
            "date"
        ).unique().sort(
            "date"
        )

    for row in df.rows():

        print(row[0])

    pass


def check_chain(name: str, date: str):

    df = pl.read_parquet(
            "./opts.parquet"
        ).filter(
            (pl.col("name") == name) &
            (pl.col("date") == date)
        )
    
    rows = df.rows()

    for row in rows:

        print(row)

    print(len(rows))
    

if __name__ == "__main__":

    t0 = time()

    test = argv[1]

    if test == "get_latest":

        symbol  = argv[2]
        start   = argv[3]
        end     = argv[4]

        get_latest(argv[1], argv[2], argv[3])

    elif test == "get_dates":

        symbol = argv[2]

        get_dates(symbol)

    elif test == "check_chain":

        name = argv[2]
        date = argv[3]

        check_chain(name, date)

    print(f"{time() - t0:0.1f}s")