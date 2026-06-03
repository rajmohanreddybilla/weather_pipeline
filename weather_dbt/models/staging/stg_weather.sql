-- models/staging/stg_weather.sql
-- Cleans and renames columns from the raw weather table

with source as (

    select * from {{ source('raw', 'RAW_WEATHER') }}

),

cleaned as (

    select
        -- identifiers
        city_name,
        weather_date,

        -- location
        latitude,
        longitude,

        -- temperature (already in Celsius)
        temperature_max_c,
        temperature_min_c,
        temperature_mean_c,

        -- derived: temperature range for the day
        round(temperature_max_c - temperature_min_c, 2) as temperature_range_c,

        -- precipitation
        precipitation_mm,
        rain_mm,
        snowfall_cm,

        -- wind
        windspeed_max_kmh,
        windgusts_max_kmh,
        wind_direction_dominant_deg,

        -- weather classification
        weather_code,

        -- map WMO weather codes to human-readable descriptions
        case
            when weather_code = 0  then 'Clear sky'
            when weather_code = 1  then 'Mainly clear'
            when weather_code = 2  then 'Partly cloudy'
            when weather_code = 3  then 'Overcast'
            when weather_code in (45, 48) then 'Foggy'
            when weather_code in (51, 53, 55) then 'Drizzle'
            when weather_code in (61, 63, 65) then 'Rain'
            when weather_code in (71, 73, 75) then 'Snow'
            when weather_code in (80, 81, 82) then 'Rain showers'
            when weather_code in (85, 86) then 'Snow showers'
            when weather_code in (95, 96, 99) then 'Thunderstorm'
            else 'Unknown'
        end as weather_description,

        -- boolean flags for easy filtering in Power BI
        case when precipitation_mm > 0 then true else false end as is_rainy_day,
        case when snowfall_cm > 0 then true else false end as is_snowy_day,
        case when windspeed_max_kmh > 50 then true else false end as is_windy_day,
        case when temperature_max_c >= 25 then true else false end as is_hot_day,
        case when temperature_max_c <= 0 then true else false end as is_freezing_day,

        -- sun hours (derived from sunrise/sunset strings)
        sunrise,
        sunset,

        -- metadata
        _loaded_at

    from source
    where city_name is not null
      and weather_date is not null

)

select * from cleaned