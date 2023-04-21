import  polars  as      pl
from    sys     import  argv
from    time    import  time

if __name__ == "__main__":

    t0 = time()

    symbol  = argv[1]
    begin   = argv[2]
    end     = argv[3]

    df  = pl.scan_parquet("ohlc.parquet")
    sql = pl.SQLContext()

    sql.register("ohlc", df)

    cols = sql.query(
                f'''
                    SELECT
                        date,
                        month,
                        year,
                        settle,
                        dte
                    FROM ohlc
                    WHERE name = '{symbol}'
                    AND date between '{begin}' and '{end}'
                    ORDER BY date ASC, year ASC, month ASC;
                '''
            )

    print(cols)

    print(f"{time() - t0:0.1f}s")