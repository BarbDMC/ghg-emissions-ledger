{#- Generic test: the combination of `columns` is unique. -#}
{% test unique_combination_of_columns(model, columns) %}
    select {{ columns | join(', ') }}, count(*) as n
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}
