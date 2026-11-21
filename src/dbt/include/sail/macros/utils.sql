{% macro sail__dateadd(datepart, interval, from_date_or_timestamp) -%}
    timestampadd({{ datepart }}, {{ interval }}, cast({{ from_date_or_timestamp }} as timestamp))
{%- endmacro %}

{% macro sail__split_part(string_text, delimiter_text, part_number) -%}
    split_part({{ string_text }}, {{ delimiter_text }}, {{ 1 if part_number == 0 else part_number }})
{%- endmacro %}
