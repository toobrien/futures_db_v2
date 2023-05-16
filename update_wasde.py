from    config      import  CONFIG
from    io          import  StringIO
from    os          import  remove
import  polars      as      pl
from    re          import  findall
from    requests    import  get
from    shutil      import  unpack_archive
from    time        import  time


# relatively small data set, just rebuild monthly


DB_PATH         = CONFIG["wasde"]["db_path"]
BASE_URL        = CONFIG["wasde"]["base_url"]
LINKS           = CONFIG["wasde"]["links"]
NEW_PATTERN     = CONFIG["wasde"]["new_pattern"]
ARCH_PATTERN    = CONFIG["wasde"]["arch_pattern"]
SCHEMA          = CONFIG["wasde_schema"]


def archived():

    res = get(BASE_URL + LINKS)

    paths   = sorted(findall(ARCH_PATTERN, res.text))
    dfs     = []

    for path in paths:

        res         = get(BASE_URL + path, stream = True)
        fn          = path.split('/')[-1].split('.')[0]
        archive_fn  = f"./{fn}.zip"
        csv_fn      = f"./{fn}.csv"

        with open(archive_fn, "wb") as fd:

            for chunk in res.iter_content(chunk_size = 1024):

                fd.write(chunk)

        unpack_archive(archive_fn, "./")
        remove(archive_fn)

        dfs.append(pl.read_csv(csv_fn, dtypes = SCHEMA))

        remove(csv_fn)

    df = dfs[0]

    for i in range(1, len(dfs)):

        df.extend(dfs[i])

    return df


def update():

    df = archived()

    res = get(BASE_URL + LINKS)

    paths = sorted(findall(NEW_PATTERN, res.text))

    for path in paths:

        res = get(BASE_URL + path)

        next_df = pl.read_csv(StringIO(res.text), dtypes = SCHEMA)

        df.extend(next_df)

    df.write_parquet(DB_PATH)


if __name__ == "__main__":

    t0 = time()

    update()

    print(f"{'update_wasde':30}{'':30}{time() - t0:0.1f}")