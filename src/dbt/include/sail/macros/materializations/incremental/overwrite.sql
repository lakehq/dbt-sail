{% macro get_insert_overwrite_sql(source_relation, target_relation, existing_relation) %}
    {%- set names = adapter.get_columns_in_relation(target_relation) | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {% if not existing_relation.is_iceberg %}{{ partition_cols(label='partition') }}{% endif %}
    select {{ names }} from {{ source_relation }}
{% endmacro %}
