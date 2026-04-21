{% macro sail__list_relations_without_caching(relation) %}
  {{ return(spark__list_relations_without_caching(relation)) }}
{% endmacro %}

{% macro sail__get_columns_in_relation_raw(relation) -%}
  {{ return(spark__get_columns_in_relation_raw(relation)) }}
{% endmacro %}

{% macro sail__get_columns_in_relation(relation) -%}
  {{ return(spark__get_columns_in_relation(relation)) }}
{% endmacro %}
