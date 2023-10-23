{%- macro redshift__sat(src_pk, src_hashdiff, src_payload, src_extra_columns, src_eff, src_ldts, src_source, source_model) -%}

{%- set source_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_payload, src_extra_columns, src_eff, src_ldts, src_source]) -%}
{%- set rank_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_ldts]) -%}
{%- set pk_cols = dbtvault.expand_column_list(columns=[src_pk]) -%}

{%- if model.config.materialized == 'vault_insert_by_rank' %}
    {%- set source_cols_with_rank = source_cols + dbtvault.escape_column_names([config.get('rank_column')]) -%}
{%- endif -%}

{{ dbtvault.prepend_generated_by() }}

WITH source_data AS (
    {%- if model.config.materialized == 'vault_insert_by_rank' %}
    SELECT {{ dbtvault.prefix(source_cols_with_rank, 'a', alias_target='source') }}
    {%- else %}
    SELECT {{ dbtvault.prefix(source_cols, 'a', alias_target='source') }}
    {%- endif %}
    FROM {{ ref(source_model) }} AS a
    WHERE {{ dbtvault.multikey(src_pk, prefix='a', condition='IS NOT NULL') }}
    {%- if model.config.materialized == 'vault_insert_by_period' %}
    AND __PERIOD_FILTER__
    {% elif model.config.materialized == 'vault_insert_by_rank' %}
    AND __RANK_FILTER__
    {% endif %}
),

{%- if dbtvault.is_any_incremental() %}
existing_records as (
    SELECT {{ dbtvault.prefix(rank_cols, 'current_records', alias_target='target') }},
            ROW_NUMBER() OVER (
                PARTITION BY 
                {{ dbtvault.prefix([src_pk], 'current_records') }},
                {{ dbtvault.prefix([src_hashdiff], 'current_records') }}
                ORDER BY {{ dbtvault.prefix([src_ldts], 'current_records') }} DESC
            ) AS row_num
        FROM {{ this }} AS current_records
            JOIN (
                SELECT DISTINCT {{ dbtvault.prefix([src_pk], 'source_data') }}
                FROM source_data
            ) AS source_records
                ON {{ dbtvault.multikey(src_pk, prefix=['current_records','source_records'], condition='=') }}
),
latest_records AS (
    SELECT {{ dbtvault.prefix(rank_cols, 'a', alias_target='target') }},
    1 as src_check
    FROM existing_records AS a
    WHERE a.row_num = 1
),

{%- endif %}

records_to_insert AS (
    SELECT DISTINCT {{ dbtvault.alias_all(source_cols, 'stage') }}
    FROM source_data AS stage
    {%- if dbtvault.is_any_incremental() %}
        LEFT JOIN latest_records
        ON 
        {{ dbtvault.multikey(src_pk, prefix=['latest_records','stage'], condition='=') }}
        and 
        {{ dbtvault.multikey(src_hashdiff, prefix=['latest_records','stage'], condition='=') }}
        WHERE latest_records.src_check IS NULL
            
    {%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}
