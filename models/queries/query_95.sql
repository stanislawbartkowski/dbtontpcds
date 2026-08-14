
with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from {{ ref('web_sales') }} ws1,{{ ref('web_sales') }} ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 select  
   count(distinct ws_order_number) as {{ ident("order count") }}
  ,sum(ws_ext_ship_cost) as {{ ident("total shipping cost") }}
  ,sum(ws_net_profit) as {{ ident("total net profit") }}
from
   {{ ref('web_sales') }} ws1
  ,{{ ref('date_dim') }}
  ,{{ ref('customer_address') }}
  ,{{ ref('web_site') }}
where
    d_date between '1999-5-01' and 
           (cast('1999-5-01' as date) + {{ interval_days(60) }})
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'TX'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from {{ ref('web_returns') }},ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
 {{ add_limit() }}
