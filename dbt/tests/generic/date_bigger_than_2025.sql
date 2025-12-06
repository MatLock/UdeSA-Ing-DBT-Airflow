{% test date_bigger_than_2025(model, column_name) %}

-- CHECKS THAT ALL TRANSACTION ARE FROM 2025
select
    {{ column_name }}
from {{ model }}
where extract(year from {{ column_name }}) < 2025

{% endtest %}
