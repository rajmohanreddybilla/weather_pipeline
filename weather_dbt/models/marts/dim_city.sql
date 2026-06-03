-- models/marts/dim_city.sql
-- One row per city with static metadata and region info

with cities as (

    select distinct
        city_name,
        latitude,
        longitude
    from {{ ref('stg_weather') }}

),

enriched as (

    select
        city_name,
        latitude,
        longitude,

        -- UK region classification
        case city_name
            when 'London'     then 'South East England'
            when 'Birmingham' then 'West Midlands'
            when 'Manchester' then 'North West England'
            when 'Leeds'      then 'Yorkshire'
            when 'Sheffield'  then 'Yorkshire'
            when 'Liverpool'  then 'North West England'
            when 'Bristol'    then 'South West England'
            when 'Cardiff'    then 'Wales'
            when 'Edinburgh'  then 'Scotland'
            when 'Glasgow'    then 'Scotland'
            else 'Unknown'
        end as region,

        -- country classification
        case city_name
            when 'Cardiff'    then 'Wales'
            when 'Edinburgh'  then 'Scotland'
            when 'Glasgow'    then 'Scotland'
            else 'England'
        end as country,

        -- population tier (for Power BI sizing/filtering)
        case city_name
            when 'London'     then 'Large'
            when 'Birmingham' then 'Large'
            when 'Manchester' then 'Large'
            when 'Leeds'      then 'Medium'
            when 'Glasgow'    then 'Medium'
            when 'Sheffield'  then 'Medium'
            when 'Liverpool'  then 'Medium'
            when 'Edinburgh'  then 'Medium'
            when 'Bristol'    then 'Medium'
            when 'Cardiff'    then 'Medium'
            else 'Unknown'
        end as city_size,

        -- surrogate key for joining
        {{ dbt_utils.generate_surrogate_key(['city_name']) }} as city_key

    from cities

)

select * from enriched