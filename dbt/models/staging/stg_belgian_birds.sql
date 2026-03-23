with source_data as (
    select
        cast(gbif_id as varchar) as gbif_id,
        cast(species as varchar) as species_name,
        cast(event_date as date) as observation_date,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(individual_count as integer) as individual_count,
        cast(scientific_name as varchar) as scientific_name,
        cast(basis_of_record as varchar) as basis_of_record,
        cast(occurrence_status as varchar) as occurrence_status,
        cast(country_code as varchar) as country_code,
        cast(state_province as varchar) as state_province,
        cast(dataset_key as varchar) as dataset_key,
        cast(ingestion_ts as timestamp) as ingestion_ts,
        cast(raw_payload as varchar) as raw_payload
    from {{ source('raw', 'gbif_observations_raw') }}
),
standardized as (
    select
        gbif_id,
        coalesce(nullif(trim(species_name), ''), 'Unknown') as species_name,
        observation_date,
        latitude,
        longitude,
        coalesce(individual_count, 1) as individual_count,
        scientific_name,
        basis_of_record,
        occurrence_status,
        country_code,
        state_province,
        dataset_key,
        ingestion_ts,
        raw_payload
    from source_data
),
quality_tagged as (
    select
        *,
        case
            when latitude between -90 and 90
              and longitude between -180 and 180
              and individual_count >= 0
            then true
            else false
        end as is_valid_observation
    from standardized
)
select *
from quality_tagged
