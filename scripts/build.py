import duckdb, glob, os
con = duckdb.connect("synaptic.duckdb")
for sf in sorted(glob.glob("db_structure/*.csv")):
    t = os.path.basename(sf)[:-4]
    with open(sf, encoding="utf-8-sig") as fh:
        names = fh.readline().strip().split(";")
    src = "user_connection_logs_*.csv" if t == "user_connection_logs" else f"{t}.csv"
    if not glob.glob(src):
        print("SKIP", t); continue
    if t == "user_connection_logs":
        con.execute("""create or replace table user_connection_logs as select * from read_csv(
         'user_connection_logs_*.csv', header=true, union_by_name=true,
         types={'user_id':'BIGINT','subscribe_id':'BIGINT','created_at':'TIMESTAMP'})""")
    else:
        opts = f"header=false, names={names}, sample_size=200000"
        try:
            con.execute(f"create or replace table {t} as select * from read_csv('{src}', {opts})")
        except Exception as e:
            print("WARN retry with ignore_errors:", t, str(e)[:120])
            con.execute(f"create or replace table {t} as select * from read_csv('{src}', {opts}, ignore_errors=true)")
    print(t, con.execute(f"select count(*) from {t}").fetchall()[0][0], flush=True)
