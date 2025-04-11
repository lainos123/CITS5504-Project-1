-- 1. Which local government areas had the most road fatalities each year
WITH RankedFatalities AS (
    SELECT 
        dim_date.year,
        dim_lga.lga_name,
        COUNT(fact_fatalities.fatality_id) AS total_fatalities,
        RANK() OVER (PARTITION BY dim_date.year ORDER BY COUNT(fact_fatalities.fatality_id) DESC) AS rank_num
    FROM fact_fatalities
    JOIN fact_crashes ON fact_fatalities.crash_id = fact_crashes.crash_id
    JOIN dim_date ON fact_crashes.date_id = dim_date.date_id
    JOIN dim_lga ON fact_crashes.lga_id = dim_lga.lga_id
    WHERE dim_lga.lga_name <> 'Unknown'
    GROUP BY dim_date.year, dim_lga.lga_name
)
SELECT 
    year,
    lga_name,
    total_fatalities
FROM RankedFatalities
WHERE rank_num = 1
ORDER BY year;

-- 2. Which state had the most crashes in 2023?
SELECT 
    dim_state.state_name,
    COUNT(fact_crashes.crash_id) AS total_crashes
FROM fact_crashes
JOIN dim_date ON fact_crashes.date_id = dim_date.date_id
JOIN dim_state ON fact_crashes.state_id = dim_state.state_id
WHERE dim_date.year = 2023
GROUP BY dim_state.state_name
ORDER BY total_crashes DESC

-- 3. What time of day and days of the week are associated with the highest number of fatalities
-- Uses CUBE
SELECT 
    dim_date.day_of_week,
    dim_time.time_of_day,
    COUNT(fact_fatalities.fatality_id) AS total_fatalities
FROM fact_fatalities
JOIN fact_crashes ON fact_fatalities.crash_id = fact_crashes.crash_id
JOIN dim_date ON fact_crashes.date_id = dim_date.date_id
JOIN dim_time ON fact_fatalities.crash_id = dim_time.crash_id
GROUP BY CUBE (dim_date.day_of_week, dim_time.time_of_day)
HAVING dim_date.day_of_week IS NOT NULL 
   AND dim_time.time_of_day IS NOT NULL 
   AND dim_time.time_of_day != 'Unknown'
ORDER BY total_fatalities DESC;

-- 4. How many fatalities occurred during the Christmas and Easter holiday periods
SELECT 
    SUM(CASE WHEN dim_event."Christmas Period" = 'Yes' THEN 1 ELSE 0 END) AS christmas_fatalities,
    SUM(CASE WHEN dim_event."Easter Period" = 'Yes' THEN 1 ELSE 0 END) AS easter_fatalities
FROM fact_fatalities
JOIN dim_event ON fact_fatalities.crash_id = dim_event.crash_id;

-- 5. Which types of vehicles are most commonly involved in fatal crashes by year in years after 2010
-- Uses CUBE
SELECT 
    year,
    vehicle_type,
    COUNT(*) AS total_fatal_crashes
FROM (
    SELECT dim_date.year, 'Bus' AS vehicle_type, dim_vehicle.crash_id
    FROM dim_vehicle
    JOIN fact_fatalities ON dim_vehicle.crash_id = fact_fatalities.crash_id
    JOIN fact_crashes ON dim_vehicle.crash_id = fact_crashes.crash_id
    JOIN dim_date ON fact_crashes.date_id = dim_date.date_id
    WHERE dim_vehicle."Bus Involvement" = 'Yes' AND dim_date.year > 2010

    UNION ALL

    SELECT dim_date.year, 'Heavy Rigid Truck' AS vehicle_type, dim_vehicle.crash_id
    FROM dim_vehicle
    JOIN fact_fatalities ON dim_vehicle.crash_id = fact_fatalities.crash_id
    JOIN fact_crashes ON dim_vehicle.crash_id = fact_crashes.crash_id
    JOIN dim_date ON fact_crashes.date_id = dim_date.date_id
    WHERE dim_vehicle."Heavy Rigid Truck Involvement" = 'Yes' AND dim_date.year > 2010

    UNION ALL

    SELECT dim_date.year, 'Articulated Truck' AS vehicle_type, dim_vehicle.crash_id
    FROM dim_vehicle
    JOIN fact_fatalities ON dim_vehicle.crash_id = fact_fatalities.crash_id
    JOIN fact_crashes ON dim_vehicle.crash_id = fact_crashes.crash_id
    JOIN dim_date ON fact_crashes.date_id = dim_date.date_id
    WHERE dim_vehicle."Articulated Truck Involvement" = 'Yes' AND dim_date.year > 2010
) AS combined
GROUP BY CUBE (year, vehicle_type)
HAVING year IS NOT NULL AND vehicle_type IS NOT NULL
ORDER BY year, total_fatal_crashes DESC;

-- 6. What is the average road fatality rate per 1000 dwellings in each local government area?
SELECT
    dim_lga.lga_name,
    dim_lga.dwelling_count,
    COUNT(fact_fatalities.fatality_id) AS total_fatalities,
    ROUND((COUNT(fact_fatalities.fatality_id) * 1000.0 / CAST(dim_lga.dwelling_count AS NUMERIC)), 2) AS avg_fatalities_per_1000_dwellings
FROM
    fact_fatalities
JOIN
    fact_crashes ON fact_fatalities.crash_id = fact_crashes.crash_id
JOIN
    dim_lga ON fact_crashes.lga_id = dim_lga.lga_id
WHERE dim_lga.dwelling_count IS NOT NULL
GROUP BY
    dim_lga.lga_name, dim_lga.dwelling_count
ORDER BY
    avg_fatalities_per_1000_dwellings DESC;