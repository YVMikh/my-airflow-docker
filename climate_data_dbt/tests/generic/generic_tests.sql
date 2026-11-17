{# Uniqueness of a combination of columns (the grain of an aggregated model) #}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n_records
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}


{# The column must not be below min_value #}
{% test value_not_below(model, column_name, min_value) %}
select *
from {{ model }}
where {{ column_name }} < {{ min_value }}
{% endtest %}


{# Timestamps (UTC, without time zone) must not lie in the future; tolerance in hours #}
{% test not_in_future(model, column_name, tolerance_hours=1) %}
select *
from {{ model }}
where {{ column_name }} > (now() at time zone 'utc') + interval '{{ tolerance_hours }} hour'
{% endtest %}


{# min <= median <= max and min <= avg <= max for the statistics of a pollutant (tolerance for rounding) #}
{% test statistics_consistent(model, prefix) %}
select *
from {{ model }}
where {{ prefix }}_min    > {{ prefix }}_median + 0.0001
   or {{ prefix }}_median > {{ prefix }}_max    + 0.0001
   or {{ prefix }}_avg    < {{ prefix }}_min    - 0.0001
   or {{ prefix }}_avg    > {{ prefix }}_max    + 0.0001
{% endtest %}


{# The column must not be above max_value #}
{% test value_not_above(model, column_name, max_value) %}
select *
from {{ model }}
where {{ column_name }} > {{ max_value }}
{% endtest %}
