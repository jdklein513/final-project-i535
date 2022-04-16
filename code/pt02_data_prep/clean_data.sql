-- age, sex demographic clean table
WITH census_demographics_est_2020_clean AS
(SELECT
    CONCAT(format("%02d", STATE), format("%03d", COUNTY)) AS fips, -- need to combine the state and county code after padding those with zeros
    STNAME,
    CAST(AGE18PLUS_TOT AS BIGNUMERIC) AS AGE18PLUS_TOT,
    CAST(AGE18PLUS_FEM AS BIGNUMERIC) AS AGE18PLUS_FEM,
    CAST(AGE18PLUS_MALE AS BIGNUMERIC) AS AGE18PLUS_MALE,
    ROUND(CAST(AGE18PLUS_FEM AS BIGNUMERIC)/CAST(AGE18PLUS_TOT AS BIGNUMERIC),4) AS AGE18PLUS_PCT_FEM
FROM
    `joeklein-i535-trips-pipeline.census_demographics_est_2020.census_demographics_est_2020`
WHERE
    YEAR = 14), -- want the latest year available for the age comparison

-- 2020 elections results grouped to fips and party level
-- raw data is at fips, party, submission type level
election_results_grouped AS 
(SELECT
    county_fips,
    state,
    party,
    totalvotes,
    sum(candidatevotes) AS candidatevotes
FROM
    `joeklein-i535-trips-pipeline.county_pres_election_results_2000_2020.county_pres_election_results_2000_2020`
WHERE
    year = 2020 -- filtering to most recent election
GROUP BY
    county_fips,
    state,
    party,
    totalvotes),

-- max votes for candidate by fips
-- creating this temp table to be able to determine which party received the most votes
election_results_grouped_top_votes AS
(SELECT
    county_fips,
    state,
    max(candidatevotes) as max_candidatevotes
FROM
    election_results_grouped
GROUP BY
    county_fips,
    state),

-- creating the temp table to get the party which received the most votes in 2020 election
election_results_grouped_top_party AS
(SELECT
    A.county_fips,
    A.state,
    A.party
FROM
    election_results_grouped A
INNER JOIN 
    election_results_grouped_top_votes B ON A.county_fips = B.county_fips AND A.state = B.state
WHERE
    A.candidatevotes = B.max_candidatevotes),

-- 2020 presidential election results clean table
-- has the percentage of people voting for democratic party and the top candidate indicator
election_results_clean AS
(SELECT
    format("%05d", A.county_fips) AS county_fips,
    A.state,
    ROUND(sum(A.candidatevotes)/sum(A.totalvotes),4) AS percent_democrat,
    B.party AS top_party
FROM
    election_results_grouped A
LEFT JOIN 
    election_results_grouped_top_party B ON A.county_fips = B.county_fips AND A.state = B.state
WHERE 
    A.party = 'DEMOCRAT' -- filtering to one party
GROUP BY 
    A.county_fips,
    A.state,
    B.party),

-- temp table which changes the types for the trips data
trips_clean AS (
    SELECT 
        `date`,
        state_code,
        county_fips,
        county,
        CAST(LEFT(`date`, 4) AS FLOAT64) AS year,
        SUBSTR(`date`, 6, 2) AS month,
        CAST(pop_stay_at_home AS FLOAT64) AS pop_stay_at_home,
        CAST(pop_not_stay_at_home AS FLOAT64) AS pop_not_stay_at_home,
        CAST(trips AS FLOAT64) AS trips,
        CAST(trips_1 AS FLOAT64) AS trips_1,
        CAST(trips_1_3 AS FLOAT64) AS trips_1_3,
        CAST(trips_3_5 AS FLOAT64) AS trips_3_5,
        CAST(trips_5_10 AS FLOAT64) AS trips_5_10,
        CAST(trips_10_25 AS FLOAT64) AS trips_10_25,
        CAST(trips_25_50 AS FLOAT64) AS trips_25_50,
        CAST(trips_50_100 AS FLOAT64) AS trips_50_100,
        CAST(trips_100_250 AS FLOAT64) AS trips_100_250,
        CAST(trips_250_500 AS FLOAT64) AS trips_250_500,
        CAST(trips_500 AS FLOAT64) AS trips_500
    FROM
        `joeklein-i535-trips-pipeline.bts_trips.bts_trips`
)

-- trips data by fips clean with trip, vax rate, demographic, political, and weather data
SELECT 
    LEFT(CAST(A.`date` AS STRING), 10) AS `date`, -- convert the month start timestamp to a string
    A.state_code,
    A.county_fips,
    A.county,
    A.pop_stay_at_home,
    A.pop_not_stay_at_home,
    ROUND(A.pop_stay_at_home/(A.pop_stay_at_home + A.pop_not_stay_at_home),4) AS pct_pop_stay_at_home,
    A.trips,
    A.trips_1,
    A.trips_1_3,
    A.trips_3_5,
    A.trips_5_10,
    A.trips_10_25,
    A.trips_25_50,
    A.trips_50_100,
    A.pop_not_stay_at_home - LAG(A.pop_not_stay_at_home, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS pop_not_stay_at_home_2yr_diff,
    A.trips - LAG(A.trips, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_2yr_diff,
    A.trips_1 - LAG(A.trips_1, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_1_2yr_diff,
    A.trips_1_3 - LAG(A.trips_1_3, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_1_3_2yr_diff,
    A.trips_3_5 - LAG(A.trips_3_5, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_3_5_2yr_diff,
    A.trips_5_10 - LAG(A.trips_5_10, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_5_10_2yr_diff,
    A.trips_10_25 - LAG(A.trips_10_25, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_10_25_2yr_diff,
    A.trips_25_50 - LAG(A.trips_25_50, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_25_50_2yr_diff,
    A.trips_50_100 - LAG(A.trips_50_100, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) AS trips_50_100_2yr_diff,
    CASE WHEN LAG(A.pop_not_stay_at_home, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.pop_not_stay_at_home/LAG(A.pop_not_stay_at_home, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS pop_not_stay_at_home_2yr_diff_pct,
    CASE WHEN LAG(A.trips, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN 
        A.trips - LAG(A.trips, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year)/LAG(A.trips, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_2yr_diff_pct,
    CASE WHEN LAG(A.trips_1, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.trips_1/LAG(A.trips_1, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_1_2yr_diff_pct,
    CASE WHEN LAG(A.trips_1_3, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.trips_1_3/LAG(A.trips_1_3, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_1_3_2yr_diff_pct,
    CASE WHEN LAG(A.trips_3_5, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.trips_3_5/LAG(A.trips_3_5, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_3_5_2yr_diff_pct,
    CASE WHEN LAG(A.trips_5_10, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.trips_5_10/LAG(A.trips_5_10, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_5_10_2yr_diff_pct,
    CASE WHEN LAG(A.trips_10_25, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.trips_10_25/LAG(A.trips_10_25, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_10_25_2yr_diff_pct,
    CASE WHEN LAG(A.trips_25_50, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.trips_25_50/LAG(A.trips_25_50, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_25_50_2yr_diff_pct,
    CASE WHEN LAG(A.trips_50_100, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) <> 0 THEN
        A.trips_50_100/LAG(A.trips_50_100, 2) OVER (PARTITION BY A.county_fips, A.month ORDER BY A.year) 
    END AS trips_50_100_2yr_diff_pct,
    B.series_complete_18plus, 
    B.series_complete_18pluspop_pct, 
    B.series_complete_65plus, 
    B.series_complete_65pluspop_pct, 
    B.series_complete_yes, 
    B.series_complete_pop_pct,
    C.AGE18PLUS_TOT,
    C.AGE18PLUS_FEM,
    C.AGE18PLUS_MALE,
    C.AGE18PLUS_PCT_FEM,
    D.percent_democrat,
    D.top_party,
    CASE WHEN E._2013_code <= 4 THEN 'Metro'
         WHEN E._2013_code > 4 THEN 'Not Metro'
         ELSE NULL
    END AS ruca, -- decoding the ruca codes to name
    CASE WHEN E._2013_code = 1 THEN 'Large central metro'
         WHEN E._2013_code = 2 THEN 'Large fringe metro'
         WHEN E._2013_code = 3 THEN 'Medium metro'
         WHEN E._2013_code = 4 THEN 'Small metro'
         WHEN E._2013_code = 5 THEN 'Micropolitan'
         WHEN E._2013_code = 6 THEN 'Noncore'
         ELSE NULL
    END AS ruca_det, -- decoding the ruca codes to name
    F.cddc,
    F.hddc,
    F.pcpn,
    F.tmax,
    F.tmin,
    F.tmpc
FROM 
    trips_clean A
LEFT JOIN
    `joeklein-i535-trips-pipeline.cdc_vax_rates.cdc_vax_rates` B ON LEFT(CAST(A.`date` AS STRING), 10) = LEFT(CAST(B.`date` AS STRING), 10) AND 
                                                                    A.county_fips = B.fips
LEFT JOIN
    census_demographics_est_2020_clean C ON A.county_fips = C.fips
LEFT JOIN
    election_results_clean D ON A.county_fips = D.county_fips
LEFT JOIN
    `joeklein-i535-trips-pipeline.county_urban_rural_codes_2013.county_urban_rural_codes_2013` E ON A.county_fips = format("%05d", E.FIPS_code)
LEFT JOIN
    `joeklein-i535-trips-pipeline.ncei_weather.ncei_weather` F ON LEFT(CAST(A.`date` AS STRING), 10) = F.date AND
                                                                  A.county_fips = F.fips;
