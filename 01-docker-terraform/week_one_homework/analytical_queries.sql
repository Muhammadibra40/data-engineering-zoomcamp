-----------------------------------------------------------------------
-- Question 3: 8007
-- For the trips in November 2025, 
-- how many trips had a trip_distance of less than or equal to 1 mile?
-----------------------------------------------------------------------
SELECT count(*)
FROM public.green_taxi_data
WHERE lpep_pickup_datetime >= DATE '2025-11-01'
  AND lpep_pickup_datetime <  DATE '2025-12-01'
  AND trip_distance <= 1;

-----------------------------------------------------------------------
-- Question 4: 2025-11-20
-- Which was the pick up day with the longest trip distance? 
-- Only consider trips with trip_distance less than 100 miles. 
-----------------------------------------------------------------------
SELECT date(lpep_pickup_datetime) as date_trip,
		sum(trip_distance) as total_trip_distance
FROM public.green_taxi_data
where trip_distance  < 100
group by date(lpep_pickup_datetime)
order by total_trip_distance  desc
limit 1

-----------------------------------------------------------------------
-- Question 5: East Harlem North
-- Which was the pickup zone with the largest total_amount 
-- (sum of all trips) on November 18th, 2025?
-----------------------------------------------------------------------
SELECT  z."Zone",
		sum(total_amount) as zone_amount
FROM public.green_taxi_data gt
JOIN public.taxi_zone_lookup z
ON gt."PULocationID"  = z."LocationID"
where date(lpep_pickup_datetime) = '2025-11-18'
group by z."Zone"
order by zone_amount desc
LIMIT 1

-----------------------------------------------------------------------
-- Question 6: LaGuardia Airport
-- For the passengers picked up in the zone named "East Harlem North" in November 2025, 
-- which was the drop off zone that had the largest tip?
-----------------------------------------------------------------------
SELECT  z."Zone",
		sum("extra") as total_zone_tip
FROM public.green_taxi_data gt
JOIN public.taxi_zone_lookup z
ON gt."DOLocationID"  = z."LocationID"
WHERE gt."PULocationID" = 74
  AND extract (month from lpep_pickup_datetime) = 11
group by z."Zone"
order by total_zone_tip desc
limit 1

-- -- EDA
-- select distinct extract(year from lpep_pickup_datetime)
-- FROM public.green_taxi_data

-- select min(lpep_pickup_datetime),
-- max(lpep_pickup_datetime)
-- from public.green_taxi_data

-- SELECT  distinct z."Zone"
-- FROM public.green_taxi_data gt
-- JOIN public.taxi_zone_lookup z
-- ON gt."DOLocationID"  = z."LocationID"
-- WHERE gt."PULocationID" = 74
--   AND gt."lpep_pickup_datetime" >= DATE '2025-11-01'
--   AND gt."lpep_pickup_datetime" <  DATE '2025-12-01'