from    config              import  CONFIG
from    csv                 import  reader
import  polars              as      pl
from    requests            import  get
from    time                import  time


# VIX spot https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv


def vx():

    t0 = time()

    config  = CONFIG["spot"]["VX"]
    res     = get(config["url"])

    if res.status_code != 200:

        print(f"error: VIX spot download failed {res.status_code}")

    txt     = res.text
    rows    = reader(txt.splitlines())
    headers = next(rows)                # skip
    dates   = []
    closes  = []

    for row in rows:

        date    = row[0].split("/")
        date    = f"{date[2]}-{date[0]}-{date[1]}"
        close   = float(row[4])

        dates.append(date)
        closes.append(close)
    
    df = pl.from_dict(
        {
            "date":     dates,
            "price":    closes
        }
    )

    df.write_parquet(config["file"])

    print(f"{'update_spot.vx':30}{'all':30}{time() - t0:0.1f}")


def update():

    t0 = time()

    vx()

    print(f"{'update_spot.update':30}{'all':30}{time() - t0:0.1f}")


if __name__ == "__main__":

    vx()