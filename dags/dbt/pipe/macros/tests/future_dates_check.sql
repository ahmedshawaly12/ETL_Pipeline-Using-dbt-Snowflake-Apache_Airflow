{% test future_dates_check(model, column_name) %}

SELECT * 
FROM {{ model }}
WHERE {{ column_name }} > CURRENT_DATE

{% endtest %}
