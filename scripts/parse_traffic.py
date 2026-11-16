import zipfile, io, re, os, csv, glob, duckdb
from collections import Counter
from multiprocessing import Pool
TI = re.compile(r'Traffic info changed: (\d+) bytes limit, (\d+) bytes used')
TS = re.compile(r'(\d{4}-\d\d-\d\dT[\d:.]+)')
DR = re.compile(r'Realtime disconnect.*?reason=DisconnectReason\.(\w+)')
KW = re.compile(r'raffic|eward|AdLimit|imit|[Ee]xtend|[Qq]uota')
def norm(s):
    s = re.sub(r'Bearer [\w.-]+', 'Bearer ***', s)
    s = re.sub(r'[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}', '<uuid>', s)
    return re.sub(r'\d+', 'N', s).strip()[:170]
def work(zp):
    uid = os.path.basename(os.path.dirname(zp)); rep = os.path.basename(zp)[:-4]
    rows, dr, kw = [], Counter(), Counter()
    try:
        with zipfile.ZipFile(zp) as z:
            for n in z.namelist():
                if not n.endswith('.log') or n.endswith('/'): continue
                with z.open(n) as raw:
                    for line in io.TextIOWrapper(raw, encoding='utf-8', errors='replace'):
                        if 'Traffic info changed' in line:
                            m, t = TI.search(line), TS.search(line)
                            if m and t: rows.append((uid, rep, t.group(1), int(m.group(1)), int(m.group(2))))
                            continue
                        if not line.startswith('['): continue
                        m = DR.search(line)
                        if m: dr[m.group(1)] += 1
                        if KW.search(line): kw[norm(line.split('  ', 2)[-1])] += 1
    except Exception: pass
    return uid, rows, dr, kw
if __name__ == "__main__":
    os.makedirs("out", exist_ok=True)
    zips = sorted(glob.glob("logs/*/*.zip"))
    dr_all, kw_all, kw_users = Counter(), Counter(), Counter()
    with open("out/traffic.csv", "w", newline="") as f, Pool() as p:
        w = csv.writer(f); w.writerow(["user_id","report","ts","lim","used"])
        for uid, rows, dr, kw in p.imap_unordered(work, zips, chunksize=2):
            w.writerows(rows); dr_all.update(dr); kw_all.update(kw)
            for k in kw: kw_users[k] += 1
    print("=== disconnect reasons"); print(dr_all.most_common(15))
    print("\n=== keyword messages (по числу отчётов)")
    for k, c in kw_users.most_common(40): print(c, kw_all[k], k)
    con = duckdb.connect()
    con.execute("create table t as select user_id, report, ts, left(ts,10) d, lim, used from read_csv('out/traffic.csv', types={'ts':'VARCHAR'})")
    Q = {
    "coverage": "select count(*) rows, count(distinct user_id) users, count(distinct (user_id,d)) user_days, min(d) mn, max(d) mx from t",
    "limits": "select lim, round(lim/1048576.0) mb, count(*) rows, count(distinct user_id) users from t group by 1 order by 3 desc limit 12",
    "daily_usage_mb (дни с лимитом 500 МиБ)": """with x as (select user_id, d, max(used)/1048576.0 mb from t group by 1,2 having min(lim)=524288000)
      select count(*) user_days, quantile_cont(mb,[0.25,0.5,0.75,0.9,0.95,0.99]) q_mb, round(100*avg((mb>=500*0.95)::int),1) pct_at_limit from x""",
    "threshold_curve (какой % user-days упирается в порог)": """with x as (select user_id, d, max(used)/1048576.0 mb from t group by 1,2)
      select thr, round(100.0*avg((mb>=thr)::int),1) pct_days from x cross join (select unnest([50,100,200,300,400,500,750,1000,2000]) thr) group by thr order by thr""",
    "limit_increases (прирост лимита)": """with s as (select *, lim - lag(lim) over (partition by user_id, report order by ts) delta from t)
      select round(delta/1048576.0) mb_added, count(*) n, count(distinct user_id) users from s where delta>0 group by 1 order by 2 desc limit 10""",
    "used_resets_hour": """with s as (select *, used - lag(used) over (partition by user_id, report order by ts) du from t)
      select substr(ts,12,2) as hr, count(*) resets from s where du<0 group by 1 order by 2 desc limit 6""",
    "days_with_any_extension": """with x as (select user_id, d, max(lim)>min(lim) ext, max(used)/1048576.0 mb, max(used)>=min(lim)*0.95 hit from t group by 1,2)
      select count(*) user_days, sum(hit::int) hit_limit, sum(ext::int) extended, round(100.0*sum(ext::int)/nullif(sum(hit::int),0),1) pct_hit_then_extended from x""",
    }
    for k, q in Q.items():
        print(f"\n=== {k}")
        try: print(con.sql(q).df().to_string())
        except Exception as e: print("ERROR:", str(e)[:300])
