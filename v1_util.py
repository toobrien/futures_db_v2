from    enum    import  IntEnum
from    json    import  dumps, loads
import  polars  as      pl
from    time    import  time

CONFIG  = loads(open("./config.json", "r").read())
V1_FUTS = None


class term(IntEnum):

    date        = 0
    month       = 1
    year        = 2
    settle      = 3
    dte         = 4


# for use in spreads folder

def get_term_days(
    symbol: str, 
    start: str, 
    end: str
):

    if not V1_FUTS:

        v1_futs = pl.read_parquet(CONFIG["futs_db"])
    
    filtered = v1_futs.filter(
                                (pl.col("name") == symbol) &
                                (pl.col("date") < end)     & 
                                (pl.col("date") >= start)
                            ).sort(
                                [ "date", "month", "year" ]
                            )
    
    terms = filtered.select(
                            [
                                "date",
                                "month",
                                "year",
                                "settle",
                                "dte"
                            ]
                        ).rows()
    term_days   = []
    cur_date    = terms[0][term.date]
    cur_day     = []

    for row in terms:

        if row[term.date] != cur_date:

            term_days.append(cur_day)

            cur_date    = row[term.date]
            cur_day     = []
        
        cur_day.append(row)
    
    term_days.append(cur_day)

    return term_days


if __name__ == "__main__":

    t0 = time()

    term_days = get_term_days("VX", "2018-01-01", "2024-01-01")

    for row in term_days[-1]:

        print(row)

    print(f"num_days: {len(term_days)}")
    print(f"{time() - t0:0.1f}s")

    pass