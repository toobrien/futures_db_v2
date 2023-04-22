from    datetime    import datetime
import  update_cboe
import  update_cme
from    sys         import argv
from    time        import time

# example usage:
#
#   - python update.py
#   - python update.py 2023-04-20
#
#   The first form updates the database with the most recent day's settlements.
#   The second form attempts to update the database with a given day's settlement values.
#   Note that the CME FTP retains about a month of old data. CBOE offers considerably more.


if __name__ == "__main__":

    t0      = time()
    date    = datetime.strftime(datetime.today(), "%Y-%m-%d")
    new     = True

    if len(argv) > 1:

        date    = argv[1]
        new     = False

    cme_date = date.replace("-", "")

    update_cboe.update(date)
    update_cme.update(cme_date, new)

    print(f"{'update':30s}{date:30s}{time() - t0:0.1f}")