select
    species_name as species_id,
    species_name,
    min(observation_date) as first_observed_date,
    max(observation_date) as last_observed_date,
    count(*) as total_observation_records
from {{ ref('stg_belgian_birds') }}
where is_valid_observation
group by 1, 2
