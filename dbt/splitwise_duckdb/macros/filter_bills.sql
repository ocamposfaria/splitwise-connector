{% macro filter_bills(column_name) %}
    (
        trim({{ column_name }}) LIKE '%aluguel%' 
        or trim({{ column_name }}) LIKE '%condomínio%' 
        or trim({{ column_name }}) LIKE '%seguro do carro%' 
        or trim({{ column_name }}) LIKE '%energia%' 
        or trim({{ column_name }}) LIKE '%internet%' 
        or trim({{ column_name }}) LIKE '%IPTU%' 
        or trim({{ column_name }}) LIKE '%água%' 
        or trim({{ column_name }}) LIKE '%celular lana%' 
        or trim({{ column_name }}) LIKE '%gás%' 
        or trim({{ column_name }}) LIKE '%seguro incêndio%'
    )
{% endmacro %}