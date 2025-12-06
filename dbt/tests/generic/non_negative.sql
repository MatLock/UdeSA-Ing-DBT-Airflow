{% test non_negative(model, column_name) %}


select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0 or {{ column_name }} is null

{% endtest %}
