
select  
   substr(w_warehouse_name,1,20) as w_warehouse_name_substr
  ,sm_type
  ,cc_name
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as {{ ident("30 days") }} 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as {{ ident("31-60 days") }} 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as {{ ident("61-90 days") }} 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as {{ ident("91-120 days") }} 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as {{ ident(">120 days") }} 
from
   {{ ref('catalog_sales') }}
  ,{{ ref('warehouse') }}
  ,{{ ref('ship_mode') }}
  ,{{ ref('call_center') }}
  ,{{ ref('date_dim') }}
where
    d_month_seq between 1212 and 1212 + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
 {{ add_limit() }}
