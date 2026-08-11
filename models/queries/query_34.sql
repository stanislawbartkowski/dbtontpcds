
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from {{ ref('store_sales') }},{{ ref('date_dim') }},{{ ref('store') }},{{ ref('household_demographics') }}
    where {{ ref('store_sales') }}.ss_sold_date_sk = {{ ref('date_dim') }}.d_date_sk
    and {{ ref('store_sales') }}.ss_store_sk = {{ ref('store') }}.s_store_sk  
    and {{ ref('store_sales') }}.ss_hdemo_sk = {{ ref('household_demographics') }}.hd_demo_sk
    and ({{ ref('date_dim') }}.d_dom between 1 and 3 or {{ ref('date_dim') }}.d_dom between 25 and 28)
    and ({{ ref('household_demographics') }}.hd_buy_potential = '>10000' or
         {{ ref('household_demographics') }}.hd_buy_potential = 'Unknown')
    and {{ ref('household_demographics') }}.hd_vehicle_count > 0
    and (case when {{ ref('household_demographics') }}.hd_vehicle_count > 0 
	then {{ ref('household_demographics') }}.hd_dep_count/ {{ ref('household_demographics') }}.hd_vehicle_count 
	else null 
	end)  > 1.2
    and {{ ref('date_dim') }}.d_year in (1998,1998+1,1998+2)
    and {{ ref('store') }}.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
                           'Williamson County','Williamson County','Williamson County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dn,{{ ref('customer') }}
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number
