select
    observation_date,
    species_name as species_id,
    count(*) as observation_records,
    sum(individual_count) as individuals_observed,
    avg(latitude) as avg_latitude,
    avg(longitude) as avg_longitude
from {{ ref('stg_belgian_birds') }}
where is_valid_observation
group by 1, 2
