select
    coalesce(cast(taxon_key as varchar), species_name) as species_id,
    taxon_key,
    species_name,
    max(scientific_name) as scientific_name,
    min(observation_date) as first_observed_date,
    max(observation_date) as last_observed_date,
    count(*) as total_observation_records
from {{ ref('stg_belgian_birds') }}
where is_valid_observation
  and observation_date is not null
group by 1, 2, 3
