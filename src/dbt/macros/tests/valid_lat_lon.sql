{% test valid_lat_lon(model) %}

-- Rows flagged valid must have coordinates inside WGS84 bounds (flags logic bugs, not messy GBIF rows).
select *
from {{ model }}
where is_valid_observation
  and (
    latitude is null
    or longitude is null
    or latitude not between -90 and 90
    or longitude not between -180 and 180
  )

{% endtest %}
