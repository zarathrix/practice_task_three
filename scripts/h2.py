import duckdb
con = duckdb.connect("synaptic.duckdb")
for t in ("client_errors", "vpn_errors", "reports", "access_daily"):
    ty = ", types={'day':'VARCHAR'}" if t in ("client_errors", "vpn_errors") else ""
    con.execute(f"create or replace table {t} as select * from read_csv('out/{t}.csv'{ty})")
con.execute("create or replace temp view tr as select user_id, min(created_at) ft from orders where status='paid' and trial and amount>0 group by 1")
con.execute("create or replace temp view pd as select user_id, min(created_at) fp from orders where status='paid' and not trial and amount>0 group by 1")
con.execute("create or replace temp view trial_users as select user_id from tr")
con.execute("""create or replace temp view card_map as select user_id, json_extract_string(card_info,'$.first6')||json_extract_string(card_info,'$.last4') k
  from saved_payment_methods where json_extract_string(card_info,'$.last4') is not null group by 1,2""")
Q = {
"free_time_before_first_payment (поправка +3ч)": """with x as (select u.created_at uc, tr.ft, pd.fp from users u left join tr on tr.user_id=u.id
   left join pd on pd.user_id=u.id where tr.user_id is not null or pd.user_id is not null),
  y as (select case when ft is not null and (fp is null or ft<=fp) then 'trial_first' else 'paid_no_trial' end kind,
   epoch(least(coalesce(ft,fp),coalesce(fp,ft)) + interval 3 hour - uc)/86400 d from x)
  select kind, case when d<1.0/24 then '1:<1h' when d<1 then '2:1-24h' when d<7 then '3:1-7d' when d<30 then '4:7-30d'
   when d<90 then '5:30-90d' else '6:>90d' end bucket, count(*) n from y group by all order by 1,2""",
"trial_outcome": """select outcome, count(*) users from (select case
  when exists(select 1 from orders o where o.user_id=tr.user_id and not o.trial and o.status='paid' and o.amount>0 and o.created_at>=tr.ft) then '1_paid_after_trial'
  when exists(select 1 from orders o where o.user_id=tr.user_id and not o.trial and o.status in ('failed','pending') and o.created_at>=tr.ft) then '2_charge_failed_or_pending'
  else '3_no_charge_attempt' end outcome from tr) group by 1 order by 1""",
"payment_fail_by_provider": """select p.name, o.trial, count(*) n, round(100.0*avg((o.status='failed')::int),1) failed_pct,
  round(100.0*avg((o.status='pending')::int),1) pending_pct from orders o left join payment_providers p on p.id=o.provider_id group by all order by n desc""",
"trial_failed_never_paid": "select count(*) users from (select user_id from orders where trial group by 1 having sum((status='paid')::int)=0 and sum((status='failed')::int)>0)",
"trial_roles": "select u.role, u.is_was_guest, count(*) n from users u join trial_users t on t.user_id=u.id group by all",
"device_ident_kinds": """select os, client_type, count(*) n, count(distinct device_identifier) uniq,
  count(*) filter (where regexp_matches(device_identifier,'^[A-Z0-9]{3,5}[.][0-9]{6}[.][0-9]{3}')) build_like
  from devices where device_identifier is not null group by all order by n desc limit 10""",
"cards_per_trial_cluster": """select least(accs,5) accs, count(*) cards, sum(accs) accounts from (select cm.k, count(distinct cm.user_id) accs
  from card_map cm join trial_users t on t.user_id=cm.user_id group by 1) group by 1 order by 1""",
"conv_by_card_cluster": """with cl as (select cm.user_id, max(c.accs) cluster from card_map cm join (select cm2.k, count(distinct cm2.user_id) accs
   from card_map cm2 join trial_users t2 on t2.user_id=cm2.user_id group by 1) c on c.k=cm.k group by 1)
  select least(cl.cluster,4) cluster, count(*) trial_users, round(100.0*avg((pd.user_id is not null)::int),1) conv_pct
  from tr join cl on cl.user_id=tr.user_id left join pd on pd.user_id=tr.user_id group by 1 order by 1""",
"gap_between_trials_same_card_days": """with t as (select cm.k, tr.ft, lag(tr.ft) over (partition by cm.k order by tr.ft) prev
  from tr join card_map cm on cm.user_id=tr.user_id)
  select count(*) pairs, quantile_cont(epoch(ft-prev)/86400,[0.1,0.25,0.5,0.75,0.9]) q_days from t where prev is not null""",
"ads_cfg_history": """select platform, version, created_at::date d, json_extract(global_settings,'$.ads.showRewardedAdLimit') g_lim,
  json_extract(ru_settings,'$.ads.showRewardedAdLimit') ru_lim, json_extract(global_settings,'$.ads.showRewardedAdConnect') g_conn,
  json_extract(ru_settings,'$.ads.showRewardedAdConnect') ru_conn from mobile_version order by created_at desc limit 12""",
"feat_sub_latest": "select platform, version, json_extract(global_settings,'$.featSub') featSub, json_extract(global_settings,'$.link') link from mobile_version order by created_at desc limit 1",
"ads_by_month": "select date_trunc('month', created_at) m, count(*) views, count(distinct user_id) users from adv_view_history group by 1 order by 1",
"ads_heavy_vs_paid": """with a as (select user_id, max(c) mx from (select user_id, created_at::date d, count(*) c from adv_view_history group by 1,2) group by 1),
  b as (select distinct user_id from orders where status='paid' and amount>0)
  select case when mx<=3 then '1-3' when mx<=10 then '4-10' when mx<=25 then '11-25' else '26+' end bucket, count(*) users,
  round(100.0*avg((b.user_id is not null)::int),2) paid_pct from a left join b using(user_id) group by 1 order by min(mx)""",
"dns_concentration": """with u as (select user_id, sum(n) n from vpn_errors where level='Error' group by 1)
  select count(*) users, sum(n) total, max(n) max_user, quantile_cont(n,[0.5,0.9,0.99]) q,
  (select sum(n) from (select n from u order by n desc limit 10)) top10 from u""",
"refused_by_node": """with e as (select user_id, try_cast(replace(day,'/','-') as date) d,
   sum(n) filter (where msg like 'proxy/tun: connection was refused%') refused, sum(n) filter (where msg like 'app/dns%') dns from vpn_errors group by 1,2),
  c as (select user_id, created_at::date d, min(node_id) node from user_connection_logs group by 1,2 having count(distinct node_id)=1)
  select c.node, l.name location, count(*) user_days, sum(e.refused) refused, sum(e.dns) dns, round(sum(e.refused)::double/count(*),1) refused_per_userday
  from e join c on c.user_id=e.user_id and c.d=e.d join nodes nd on nd.id=c.node left join locations l on l.id=nd.location_id
  group by all having count(*)>=5 order by refused desc limit 15""",
"err400_vs_subscription": """with e as (select user_id, try_cast(day as date) d, sum(n) n400 from client_errors where status='400' and path like '%location/link' group by 1,2),
  c as (select user_id, created_at::date d, bool_or(subscribe_id is not null) has_sub from user_connection_logs group by 1,2)
  select coalesce(c.has_sub::varchar,'no_connection_that_day') has_sub, count(*) user_days, sum(e.n400) n400
  from e left join c on c.user_id=e.user_id and c.d=e.d group by 1""",
"verify_err_by_platform": """select r.platform, r.app, count(*) reports, count(*) filter (where v.user_id is not null) with_verify_err
  from reports r left join (select distinct user_id, report from client_errors where path like '%auth/verify') v
  on v.user_id=r.user_id and v.report=r.report group by all order by reports desc limit 12""",
}
for k, q in Q.items():
    print(f"\n=== {k}")
    try: print(con.sql(q).df().to_string(max_colwidth=120))
    except Exception as e: print("ERROR:", str(e)[:300])
