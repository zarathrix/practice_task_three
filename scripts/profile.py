import duckdb
con = duckdb.connect("synaptic.duckdb", read_only=True)
Q = {
"users": """select count(*) n, count(trial_used_at) with_trial, count(deleted_at) deleted,
  min(created_at) mn, max(created_at) mx, count(distinct appmetrica_device_id) uniq_appm,
  count(appmetrica_device_id) has_appm, sum(is_was_guest::int) guests from users""",
"users_role": "select role, count(*) from users group by 1",
"products": "select * exclude(image_url, tap_url, gradient_color) from product order by id",
"trial_cfg": "select * from product_trial",
"orders_split": """select product_id, trial, status, count(*) n, count(distinct user_id) u,
  min(created_at) mn, max(created_at) mx, min(amount) amin, max(amount) amax from orders group by all order by 1,2,3""",
"devices_nulls": """select count(*) n, count(device_identifier) has_ident, count(distinct device_identifier) uniq_ident,
  count(client_type) has_type, count(distinct user_id) users from devices""",
"multi_by_device": """select count(*) idents_multi, sum(c) users_in_multi, max(c) maxc from
  (select device_identifier, count(distinct user_id) c from devices where device_identifier is not null group by 1 having c>1)""",
"multi_by_appmetrica": """select count(*) ids_multi, sum(c) users_in_multi, max(c) maxc from
  (select appmetrica_device_id, count(*) c from users where appmetrica_device_id is not null group by 1 having c>1)""",
"multi_by_ip_trial": """select count(*) ips_multi, sum(c) trial_users, max(c) maxc from
  (select s.ip_address, count(distinct s.user_id) c from sessions s join users u on u.id=s.user_id
   where u.trial_used_at is not null group by 1 having c>2)""",
"trial_orders_per_user": """select n, count(*) users from (select user_id, count(*) n from orders
  where trial and status='paid' group by 1) group by 1 order by 1""",
"time_to_purchase": """with f as (select user_id, min(created_at) fp from orders where status='paid' and not trial group by 1),
  t as (select user_id, min(created_at) ft from orders where status='paid' and trial group by 1)
  select count(*) buyers, count(t.user_id) had_trial,
  quantile_cont(epoch(f.fp-u.created_at)/86400,[0.1,0.25,0.5,0.75,0.9]) q_days_from_signup
  from f join users u on u.id=f.user_id left join t using(user_id)""",
"adv_overview": """select count(*) views, count(distinct user_id) users, min(created_at) mn, max(created_at) mx from adv_view_history""",
"adv_per_user_day": """select count(*) user_days, quantile_cont(c,[0.5,0.75,0.9,0.99]) q, max(c) mx from
  (select user_id, created_at::date d, count(*) c from adv_view_history group by 1,2)""",
"conn_per_month": """select date_trunc('month', created_at) m, count(*) rows, count(distinct user_id) users,
  count(subscribe_id) with_sub from user_connection_logs group by 1 order by 1""",
"conn_per_user_day": """select quantile_cont(c,[0.5,0.9,0.99]) q, max(c) mx from
  (select user_id, created_at::date d, count(*) c from user_connection_logs group by 1,2)""",
"blocks": "select type, block_reason, count(*) from user_blocks group by 1,2 order by 3 desc",
"metrics": "select metric_name, count(*) n, min(value) mn, max(value) mx from metric_values group by 1 order by 1",
}
for k, q in Q.items():
    print(f"\n=== {k}")
    try: print(con.sql(q).df().to_string(max_colwidth=60))
    except Exception as e: print("ERROR:", str(e)[:200])
