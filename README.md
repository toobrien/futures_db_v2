A new version of the [`futures_db`](https://www.github.com/toobrien/futures_db) repo. The python sqlite3 module is too slow to keep up with the growing database, so a new solution was needed. To convert the old futures_db to the new format, run `from_v1.py`, substituting the path in the connection string to wherever your own copy is stored.

The only important differences between the two databases, apart from the smaller size and higher speed of access within python programs, are:

- The new database is split into two separate files; a futures file replaces the old `ohlc` table and an options file for the `cme_opts` table.
- The fundamental tables have been discarded. I made minimal progress implementing them, and what little I did add was easy to pull from the EIA API anyway.
- The `metadata` table is also gone. Rather than storing the first and last trading dates for each contract in this extra table (which was only used to calculate the days to expiration), I have added a column for "days to expiration" to directly into the futures schema.

The third point indicates the only change to the schema of either table, an addition of a new "days to expiration" column. The options table is exactly the same. The `contract_id` field has been kept to join the futures and options files. The `contract_id` in the futures file joins on `underlying_id` in the options file.

The update process has also been greatly simplified. There is no option to populate the database from initial sources, as the Stevens Reference Futures data set is no longer available (as far as I can tell). Run `update.py` once daily after `6:30 PM CST` to get the latest settlements from CME and CBOE. Optionally, the first argument can be a date in `YYYY-MM-DD` format, if you want to attempt to retrieve data you may have missed on previous days. The CME stores about a month of data on their FTP.

