-- models/marts/fct_weather.sql
-- Main fact table joining weather data with city dimension
-- This is the primary table for Power BI

with weather as (

    select * from {{ ref('stg_weather') }}

),

cities as (

    select * from {{ ref('dim_city') }}

),

final as (

    select
        -- surrogate key for this fact row
        {{ dbt_utils.generate_surrogate_key(['w.city_name', 'w.weather_date']) }} as weather_key,

        -- foreign key to dim_city
        c.city_key,

        -- date (Power BI can build its own date table from this)
        w.weather_date,
        year(w.weather_date)                         as year,
        month(w.weather_date)                        as month_number,
        monthname(w.weather_date)                    as month_name,
        dayofweek(w.weather_date)                    as day_of_week_number,
        dayname(w.weather_date)                      as day_of_week_name,
        quarter(w.weather_date)                      as quarter,
        case
            when month(w.weather_date) in (12,1,2) then 'Winter'
            when month(w.weather_date) in (3,4,5)  then 'Spring'
            when month(w.weather_date) in (6,7,8)  then 'Summer'
            when month(w.weather_date) in (9,10,11) then 'Autumn'
        end                                          as season,

        -- city info (denormalised for easier Power BI use)
        w.city_name,
        c.region,
        c.country,
        c.city_size,
        c.latitude,
        c.longitude,

        -- temperature
        w.temperature_max_c,
        w.temperature_min_c,
        w.temperature_mean_c,
        w.temperature_range_c,

        -- precipitation
        w.precipitation_mm,
        w.rain_mm,
        w.snowfall_cm,

        -- wind
        w.windspeed_max_kmh,
        w.windgusts_max_kmh,
        w.wind_direction_dominant_deg,

        -- weather description
        w.weather_code,
        w.weather_description,

        -- boolean flags
        w.is_rainy_day,
        w.is_snowy_day,
        w.is_windy_day,
        w.is_hot_day,
        w.is_freezing_day,

        -- sun times
        w.sunrise,
        w.sunset,

        -- metadata
        w._loaded_at

    from weather w
    left join cities c
        on w.city_name = c.city_name

)

select * from final