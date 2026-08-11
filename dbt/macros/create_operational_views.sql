{% macro create_operational_views() %}
  {% if execute %}
    {% set metadata_root = var('metadata_root', 'warehouse/metadata/ingestion_runs') %}
    {% set pattern = metadata_root ~ '/*.json' %}
    {% set count_result = run_query("select count(*) from glob('" ~ pattern ~ "')") %}
    {% if count_result.columns[0].values()[0] | int > 0 %}
      {% do run_query(
        "create or replace view ingestion_runs as select * from read_json_auto('" ~ pattern ~
        "', union_by_name=true)"
      ) %}
    {% else %}
      {% do run_query(
        "create or replace view ingestion_runs as select " ~
        "cast(null as varchar) run_id, cast(null as varchar) dataset, cast(null as varchar) status, " ~
        "cast(null as bigint) input_rows, cast(null as bigint) accepted_rows, " ~
        "cast(null as bigint) quarantined_rows, cast(null as bigint) duplicate_rows, " ~
        "cast(null as bigint) files_written, cast(null as bigint) input_bytes, " ~
        "cast(null as bigint) output_bytes, cast(null as double) wall_clock_seconds, " ~
        "cast(null as timestamptz) started_at, cast(null as timestamptz) completed_at, " ~
        "cast(null as date) max_event_date where false"
      ) %}
    {% endif %}
  {% endif %}
{% endmacro %}
