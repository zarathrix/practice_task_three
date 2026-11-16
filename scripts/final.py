import duckdb
con = duckdb.connect("synaptic.duckdb", read_only=True)
con.execute("create temp table t as select user_id, report, ts, left(ts,10) d, lim, used from read_csv('out/traffic.csv', types={'ts':'VARCHAR'})")
Q = {
"counter_reset_hour_utc": """with s as (select ts, used - lag(used) over (partition by user_id, report order by ts) du from t)
  select substr(ts,12,2) as hr, count(*) resets from s where du<0 group by 1 order by 2 desc limit 6""",
"threshold_curve_user_weighted": """with x as (select user_id, d, max(used)/1048576.0 mb from t group by 1,2),
  u as (select user_id, thr, avg((mb>=thr)::int) sh from x cross join (select unnest([100,200,300,500,750,1000,2000]) thr) group by 1,2)
  select thr, round(100*avg(sh),1) pct_user_weighted, count(*) users from u group by thr order by thr""",
"failed_payment_duration_min": """select o.provider_id, tr.status, count(*) n,
  quantile_cont(epoch(tr.updated_at-tr.created_at)/60,[0.1,0.5,0.9]) q_min
  from transactions tr join orders o on o.id=tr.order_id group by all order by n desc limit 12""",
}
for k, q in Q.items():
    print(f"\n=== {k}")
    try: print(con.sql(q).df().to_string())
    except Exception as e: print("ERROR:", str(e)[:300])
