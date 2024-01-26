-- Count records
SELECT COUNT(1)
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = MAKE_DATE(2019, 9, 18)
  AND CAST(lpep_dropoff_datetime AS DATE) = MAKE_DATE(2019, 9, 18);

-- Largest trip for each day 1
SELECT
    lpep_pickup_datetime::DATE,
    trip_distance
FROM green_taxi_trips
ORDER BY trip_distance DESC;


-- Largest trip for each day 2
SELECT
    lpep_pickup_datetime::DATE,
    MAX(trip_distance) largest_trip
FROM green_taxi_trips
GROUP BY lpep_pickup_datetime::DATE
ORDER BY largest_trip DESC;

-- Three biggest pick up Boroughs
SELECT
    zpu."Borough" pick_up_loc,
    SUM(total_amount) total
FROM green_taxi_trips gtt
JOIN taxi_zones zpu on gtt."PULocationID" = zpu."LocationID"
JOIN taxi_zones zdo on gtt."DOLocationID" = zdo."LocationID"
WHERE lpep_pickup_datetime::DATE = MAKE_DATE(2019, 9, 18)
GROUP BY pick_up_loc
ORDER BY total DESC;

-- Largest tip
-- picked up in September 2019 in the zone name Astoria
SELECT
    zdo."Zone" dropoff_loc,
    MAX(tip_amount) max_tip
FROM green_taxi_trips gtt
    INNER JOIN taxi_zones zpu on gtt."PULocationID" = zpu."LocationID"
    INNER JOIN taxi_zones zdo on gtt."DOLocationID" = zdo."LocationID"
WHERE lpep_pickup_datetime::DATE BETWEEN MAKE_DATE(2019, 9, 1) AND MAKE_DATE(2019, 9, 30)
AND zpu."Zone" = 'Astoria'
GROUP BY dropoff_loc
ORDER BY max_tip DESC;