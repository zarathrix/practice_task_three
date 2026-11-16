import zipfile, io, re, os, csv, glob
from collections import Counter, defaultdict
from multiprocessing import Pool
RX = [(re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'),'<uuid>'),
      (re.compile(r'eyJ[\w.-]+'),'<jwt>'),
      (re.compile(r'\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?'),'<ip>'),
      (re.compile(r'\b[\w-]+(?:\.[\w-]+)+\.?'),'<host>'),
      (re.compile(r'\d+'),'N')]
def norm(s):
    for r, t in RX: s = r.sub(t, s)
    return s.strip()[:160]
CL  = re.compile(r'^\[([^\]]+)\]\s+\S+\s+(\d{4}-\d\d-\d\d)T[\d:.]+\s+\[([A-Z])\]\s+(.*)')
RUN = re.compile(r'Running on (.*?); OS version (.*?); App version (\S+)')
VE  = re.compile(r'^(\d{4}/\d\d/\d\d) \S+ \[(Error|Warning)\] (.*)')
AC  = re.compile(r'^(\d{4}/\d\d/\d\d) \S+ from \S+ accepted (tcp|udp):(.+):\d+ \[(.*)\]\s*$')

def work(zp):
    uid = os.path.basename(os.path.dirname(zp)); rep = os.path.basename(zp)[:-4]
    meta = {"user_id": uid, "report": rep, "platform": "", "os": "", "app": "", "err": ""}
    ce, ve = Counter(), Counter()
    acc = defaultdict(lambda: {"n": 0, "tcp": 0, "udp": 0, "dst": set(), "direct": 0, "proxy": 0})
    try:
        with zipfile.ZipFile(zp) as z:
            for n in z.namelist():
                b = n.lower()
                if n.endswith('/') or not b.endswith(('.log', '.txt')): continue
                with z.open(n) as raw:
                    f = io.TextIOWrapper(raw, encoding='utf-8', errors='replace')
                    if b.endswith('.log'):
                        cur = None
                        for i, line in enumerate(f):
                            if i < 3 and not meta["app"]:
                                r = RUN.search(line)
                                if r: meta["platform"], meta["os"], meta["app"] = r.group(1), r.group(2), r.group(3)
                            m = CL.match(line)
                            if m:
                                if cur: ce[tuple(cur)] += 1; cur = None
                                if m.group(3) in 'EF': cur = [m.group(2), m.group(1), m.group(3), norm(m.group(4)), '', '']
                            elif cur:
                                if line.startswith('Path: '): cur[4] = norm(line[6:].split('?')[0])
                                elif line.startswith('Status code: '): cur[5] = line[13:].strip()
                        if cur: ce[tuple(cur)] += 1
                    elif 'vpn_error_log' in b:
                        for line in f:
                            m = VE.match(line)
                            if m: ve[(m.group(1), m.group(2), norm(m.group(3)))] += 1
                    elif 'vpn_access_log' in b:
                        for line in f:
                            m = AC.match(line)
                            if not m: continue
                            a = acc[m.group(1)]; a["n"] += 1; a[m.group(2)] += 1
                            if len(a["dst"]) < 200000: a["dst"].add(m.group(3))
                            tag = m.group(4).split('>>')[-1].strip()
                            if tag == 'direct': a["direct"] += 1
                            elif tag == 'proxy': a["proxy"] += 1
    except Exception as e:
        meta["err"] = str(e)[:80]
    return meta, ce, ve, {d: (a["n"], a["tcp"], a["udp"], len(a["dst"]), a["direct"], a["proxy"]) for d, a in acc.items()}

if __name__ == "__main__":
    os.makedirs("out", exist_ok=True)
    zips = sorted(glob.glob("logs/*/*.zip"))
    fr = open("out/reports.csv", "w", newline=""); fc = open("out/client_errors.csv", "w", newline="")
    fv = open("out/vpn_errors.csv", "w", newline=""); fa = open("out/access_daily.csv", "w", newline="")
    wr, wc, wv, wa = (csv.writer(x) for x in (fr, fc, fv, fa))
    wr.writerow(["user_id","report","platform","os","app","err"])
    wc.writerow(["user_id","report","day","component","level","msg","path","status","n"])
    wv.writerow(["user_id","report","day","level","msg","n"])
    wa.writerow(["user_id","report","day","conns","tcp","udp","uniq_dst","direct","proxy"])
    with Pool() as p:
        for i, (meta, ce, ve, acc) in enumerate(p.imap_unordered(work, zips, chunksize=2), 1):
            u, r = meta["user_id"], meta["report"]
            wr.writerow([u, r, meta["platform"], meta["os"], meta["app"], meta["err"]])
            for k, n in ce.items(): wc.writerow([u, r, *k, n])
            for k, n in ve.items(): wv.writerow([u, r, *k, n])
            for d, v in acc.items(): wa.writerow([u, r, d, *v])
            if i % 50 == 0: print(i, "/", len(zips), flush=True)
    for x in (fr, fc, fv, fa): x.close()
    import duckdb
    con = duckdb.connect()
    for t in ("reports", "client_errors", "vpn_errors", "access_daily"):
        con.execute(f"create table {t} as select * from read_csv('out/{t}.csv')")
    print("\n=== reports by platform/app"); print(con.sql("select platform, app, count(*) reports from reports group by all order by 3 desc limit 15").df().to_string())
    print("\n=== client errors (по числу пользователей)"); print(con.sql("select component, level, status, path, msg, count(distinct user_id) users, sum(n) n from client_errors group by all order by users desc limit 25").df().to_string(max_colwidth=90))
    print("\n=== vpn errors (по числу пользователей)"); print(con.sql("select level, msg, count(distinct user_id) users, sum(n) n from vpn_errors group by all order by users desc limit 20").df().to_string(max_colwidth=90))
    print("\n=== access daily"); print(con.sql("select count(*) user_days, quantile_cont(conns,0.5) p50, quantile_cont(conns,0.9) p90, quantile_cont(conns,0.99) p99, quantile_cont(uniq_dst,0.5) dst50 from access_daily").df().to_string())
