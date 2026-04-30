-- Canonical DDL for raw.gbif_observations_raw (reference for migrations / manual alignment).
-- The loader uses the equivalent CREATE TABLE IF NOT EXISTS in code; keep this file in sync when columns change.

create schema if not exists raw;

create table if not exists raw.gbif_observations_raw (
    gbif_id varchar primary key,
    event_date date,
    event_date_raw varchar,
    species varchar,
    scientific_name varchar,
    taxon_rank varchar,
    basis_of_record varchar,
    occurrence_status varchar,
    individual_count integer,
    latitude double,
    longitude double,
    coordinate_uncertainty_m double,
    country_code varchar,
    state_province varchar,
    dataset_key varchar,
    publishing_org_key varchar,
    key bigint,
    taxon_key bigint,
    genus varchar,
    family varchar,
    "order" varchar,
    class_name varchar,
    phylum varchar,
    kingdom varchar,
    issues_json varchar,
    media_count integer,
    ingestion_ts timestamp,
    raw_payload varchar
);
