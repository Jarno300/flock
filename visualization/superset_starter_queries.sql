-- Starter query 1: daily observation volume trend
select
  observation_date,
  sum(observation_records) as observation_records,
  sum(individuals_observed) as individuals_observed
from mart_bird_data.fct_daily_species_observations
group by 1
order by 1;

-- Starter query 2: top species by individuals observed
select
  species_id,
  sum(individuals_observed) as total_individuals
from mart_bird_data.fct_daily_species_observations
group by 1
order by total_individuals desc
limit 20;

-- Starter query 3: species activity window
select
  species_id,
  first_observed_date,
  last_observed_date,
  total_observation_records
from mart_bird_data.dim_species
order by total_observation_records desc
limit 50;

-- Starter query 4: geo-density points (aggregated)
select
  round(avg_latitude, 2) as lat_bucket,
  round(avg_longitude, 2) as lon_bucket,
  sum(individuals_observed) as individuals
from mart_bird_data.fct_daily_species_observations
group by 1, 2
order by individuals desc
limit 1000;
