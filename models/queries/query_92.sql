
select  
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    {{ ref('web_sales') }} 
   ,{{ ref('item') }} 
   ,{{ ref('date_dim') }}
where
i_manufact_id = 269
and i_item_sk = ws_item_sk 
and d_date between '1998-03-18' and 
        (cast('1998-03-18' as date) + {{ interval_days(90) }})
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            {{ ref('web_sales') }} 
           ,{{ ref('date_dim') }}
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '1998-03-18' and
                             (cast('1998-03-18' as date) + {{ interval_days(90) }})
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
 {{ add_limit() }}
