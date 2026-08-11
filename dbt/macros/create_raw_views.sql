{% macro create_raw_views() %}
  {% if execute %}
    {% set datasets = ['prices', 'fundamentals', 'earnings', 'macro', 'news'] %}
    {% set raw_root = var('raw_root', 'data/raw') %}
    {% for dataset in datasets %}
      {% set pattern = raw_root ~ '/' ~ dataset ~ '/year=*/month=*/*.parquet' %}
      {% set count_result = run_query("select count(*) from glob('" ~ pattern ~ "')") %}
      {% if count_result.columns[0].values()[0] | int > 0 %}
        {% do run_query(
          "create or replace view raw_" ~ dataset ~ " as select * from read_parquet('" ~
          pattern ~ "', hive_partitioning=true, union_by_name=false)"
        ) %}
      {% else %}
        {{ exceptions.raise_compiler_error("No raw Parquet files found for " ~ dataset ~ " at " ~ pattern) }}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
