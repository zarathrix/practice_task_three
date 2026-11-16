import duckdb
con = duckdb.connect("synaptic.duckdb")
con.execute("""create or replace table user_connection_logs as select * from read_csv(
 'user_connection_logs_*.csv', header=true, union_by_name=true,
 types={'user_id':'BIGINT','subscribe_id':'BIGINT','created_at':'TIMESTAMP'})""")
con.execute("create or replace temp view trial_users as select distinct user_id from orders where trial and status='paid' and amount>0")
Q = {
# ---- данные
"ucl_per_month": "select date_trunc('month',created_at) m, count(*) n, count(distinct id) uniq_id, count(distinct user_id) users, count(subscribe_id) with_sub from user_connection_logs group by 1 order by 1",
"mobile_settings": "select platform, version, json_keys(global_settings) keys, json_extract(global_settings,'$.ads') ads from mobile_version order by created_at desc limit 2",
# ---- H1: мультиаккаунты на триале
"top_device_ids": "select device_identifier, count(distinct user_id) users, any_value(client_type) ct, any_value(os) os from devices where device_identifier is not null group by 1 order by 2 desc limit 8",
"trial_by_device": """select users_per_dev, count(*) devs from (select d.device_identifier, count(distinct d.user_id) users_per_dev
  from devices d join trial_users t using(user_id) where d.device_identifier is not null
  and d.device_identifier not in (select device_identifier from devices where device_identifier is not null group by 1 having count(distinct user_id)>50)
  group by 1) group by 1 order by 1 limit 15""",
"trial_by_card": """with c as (select s.user_id, json_extract_string(s.card_info,'$.first6')||json_extract_string(s.card_info,'$.last4') k
  from saved_payment_methods s join trial_users t using(user_id) where json_extract_string(s.card_info,'$.last4') is not null)
  select users_per_card, count(*) cards from (select k, count(distinct user_id) users_per_card from c group by 1) group by 1 order by 1 limit 15""",
"trial_by_external_acc": """select users_per_acc, count(*) accs from (select e.service_name, e.account_id, count(distinct e.user_id) users_per_acc
  from external_accounts e join trial_users t using(user_id) group by 1,2) group by 1 order by 1 limit 15""",
"trial_by_ip_day": """select users_per_ip_day, count(*) groups from (select s.ip_address, s.created_at::date d, count(distinct s.user_id) users_per_ip_day
  from sessions s join trial_users t using(user_id) group by 1,2 having count(distinct s.user_id)>=2) group by 1 order by 1 limit 20""",
"trial_delay_after_signup_days": """with tr as (select user_id, min(created_at) ft from orders where status='paid' and trial and amount>0 group by 1)
  select count(*) n, quantile_cont(epoch(ft-u.created_at)/86400,0.1) p10, quantile_cont(epoch(ft-u.created_at)/86400,0.5) p50,
  quantile_cont(epoch(ft-u.created_at)/86400,0.9) p90, sum((ft<u.created_at)::int) neg from tr join users u on u.id=tr.user_id""",
# ---- H3: воронка и путь к покупке
"funnel": """select (select count(*) from users) signups, (select count(*) from users where role='client') clients,
  (select count(*) from trial_users) trial_paid,
  (select count(distinct user_id) from orders where status='paid' and not trial and amount>0) buyers""",
"trial_payment_outcome": "select status, count(*) orders, count(distinct user_id) users from orders where trial group by 1",
"trial_to_paid": """with tr as (select user_id, min(created_at) ft from orders where status='paid' and trial and amount>0 group by 1),
  pd as (select user_id, min(created_at) fp from orders where status='paid' and not trial and amount>0 group by 1)
  select count(*) trial_users, count(pd.user_id) bought, round(100.0*count(pd.user_id)/count(*),1) pct,
  sum((pd.fp<tr.ft)::int) bought_before_trial,
  quantile_cont(case when pd.fp>=tr.ft then epoch(pd.fp-tr.ft)/86400 end,0.25) q25,
  quantile_cont(case when pd.fp>=tr.ft then epoch(pd.fp-tr.ft)/86400 end,0.5) q50,
  quantile_cont(case when pd.fp>=tr.ft then epoch(pd.fp-tr.ft)/86400 end,0.75) q75 from tr left join pd using(user_id)""",
"buy_no_trial": """with pd as (select user_id, min(created_at) fp from orders where status='paid' and not trial and amount>0 group by 1)
  select count(*) buyers_no_trial, quantile_cont(epoch(fp-u.created_at)/86400,0.1) p10, quantile_cont(epoch(fp-u.created_at)/86400,0.5) p50,
  quantile_cont(epoch(fp-u.created_at)/86400,0.9) p90, sum((fp<u.created_at)::int) neg
  from pd join users u on u.id=pd.user_id where pd.user_id not in (select user_id from trial_users)""",
"first_purchase_mix": """select product_id, period_id, count(*) n from (select distinct on (user_id) user_id, product_id, period_id from orders
  where status='paid' and not trial and amount>0 order by user_id, created_at) group by all order by n desc""",
# ---- H2: лимиты трафика через рекламу (объёма трафика в данных нет)
"ads_free_user_days": """with f as (select user_id, created_at::date d from user_connection_logs where subscribe_id is null and created_at>='2026-02-01' group by 1,2),
  a as (select user_id, created_at::date d, count(*) c from adv_view_history group by 1,2)
  select count(*) free_user_days, count(a.c) days_with_ads, round(100.0*count(a.c)/count(*),1) pct_with_ads,
  quantile_cont(a.c,0.5) p50, quantile_cont(a.c,0.9) p90, quantile_cont(a.c,0.99) p99 from f left join a using(user_id,d)""",
"ads_buckets": """select case when c<=3 then '1-3' when c<=10 then '4-10' when c<=25 then '11-25' when c<=100 then '26-100' else '>100' end bucket,
  count(*) user_days, count(distinct user_id) users, sum(c) views from (select user_id, created_at::date d, count(*) c from adv_view_history group by 1,2) group by 1 order by min(c)""",
"ads_per_user_month": """select quantile_cont(c,0.5) p50, quantile_cont(c,0.9) p90, quantile_cont(c,0.99) p99 from
  (select user_id, date_trunc('month',created_at) m, count(*) c from adv_view_history group by 1,2)""",
}
for k, q in Q.items():
    print(f"\n=== {k}")
    try: print(con.sql(q).df().to_string(max_colwidth=400))
    except Exception as e: print("ERROR:", str(e)[:300])
