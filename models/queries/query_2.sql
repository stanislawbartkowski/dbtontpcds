
with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from {{ ref('web_sales') }} 
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from {{ ref('catalog_sales') }}) wscs_src),
 wswscs as 
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,{{ ref('date_dim') }}
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2) as sun_sales_ratio
       ,round(mon_sales1/mon_sales2,2) as mon_sales_ratio
       ,round(tue_sales1/tue_sales2,2) as tue_sales_ratio
       ,round(wed_sales1/wed_sales2,2) as wed_sales_ratio
       ,round(thu_sales1/thu_sales2,2) as thu_sales_ratio
       ,round(fri_sales1/fri_sales2,2) as fri_sales_ratio
       ,round(sat_sales1/sat_sales2,2) as sat_sales_ratio
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,{{ ref('date_dim') }} 
  where {{ ref('date_dim') }}.d_week_seq = wswscs.d_week_seq and
        d_year = 2001) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs
      ,{{ ref('date_dim') }} 
  where {{ ref('date_dim') }}.d_week_seq = wswscs.d_week_seq and
        d_year = 2001+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1
