select
    observation_date,
    coalesce(cast(taxon_key as varchar), species_name) as species_id,
    count(*) as observation_records,
    sum(individual_count) as individuals_observed,
    avg(latitude) as centroid_latitude,
    avg(longitude) as centroid_longitude
from {{ ref('stg_belgian_birds') }}
where is_valid_observation
  and observation_date is not null
group by 1, 2
