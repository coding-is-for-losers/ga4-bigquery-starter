{% macro get_column_values(table, column, max_records=none, filter_column=none, filter_value=none, filter_column_2=none, filter_value_2=none) -%}

    {%- call statement('get_column_values', fetch_result=True) %}

        select
            {{ column }} as value

        from {{ table }}

        {% if filter_column is not none %}
        ##where 1 = 1
        where {{ filter_column }} = '{{ filter_value }}'
        {% endif %}
        
        {% if filter_column_2 is not none %}
        ##and 2 = 2
        and {{ filter_column_2 }} = '{{ filter_value_2 }}'
        {% endif %}

        group by 1
        order by count(*) desc

        {% if max_records is not none %}
        limit {{ max_records }}
        {% endif %}

    {%- endcall -%}

    {%- set value_list = load_result('get_column_values') -%}

    {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | map(attribute=0) | list %}
        {{ return(values) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{%- endmacro %}