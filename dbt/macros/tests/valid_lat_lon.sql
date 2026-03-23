{% test valid_lat_lon(model) %}

select *
from {{ model }}
where not (
    latitude between -90 and 90
    and longitude between -180 and 180
)

{% endtest %}
