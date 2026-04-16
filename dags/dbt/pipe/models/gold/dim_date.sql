{{ config(
    materialized = 'table'
) }}


with params as (


        select
            to_date('1975-01-01') as start_date,
            to_date('2050-12-31') as end_date


),

spine as (

    select
        dateadd(day, seq4(), start_date) as date_day
    from params,
         table(generator(rowcount => 200000))
    where date_day <= end_date
      and start_date <= end_date   

),

final as (

    select
        to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
        date_day,

        year(date_day)              as year,
        quarter(date_day)           as quarter_of_year,
        'Q' || quarter(date_day)    as quarter_label,
        month(date_day)             as month_of_year,
        monthname(date_day)         as month_name,
        left(monthname(date_day),3) as month_short,
        to_char(date_day, 'YYYY-MM')as year_month,
        weekofyear(date_day)        as week_of_year,
        dayofmonth(date_day)        as day_of_month,
        dayofweek(date_day)         as day_of_week,
        dayname(date_day)           as day_name,

        case when dayofweek(date_day) in (0,6) then true else false end as is_weekend

    from spine
)

select * from final