{%- macro redshift__ma_sat(
        src_pk,
        src_cdk,
        src_hashdiff,
        src_payload,
        src_extra_columns,
        src_eff,
        src_ldts,
        src_source,
        source_model
    ) -%}
    {%- set source_cols = dbtvault.expand_column_list(
        columns = [src_pk, src_cdk, src_payload, src_extra_columns, src_hashdiff, src_eff, src_ldts, src_source]
    ) -%}
    {%- set rank_cols = dbtvault.expand_column_list(
        columns = [src_pk, src_hashdiff, src_ldts]
    ) -%}
    {%- set cdk_cols = dbtvault.expand_column_list(
        columns = [src_cdk]
    ) -%}
    {%- set cols_for_latest = dbtvault.expand_column_list(
        columns = [src_pk, src_hashdiff, src_cdk, src_ldts]
    ) %}
    {%- if model.config.materialized == 'vault_insert_by_rank' -%}
        {%- set source_cols_with_rank = source_cols + [config.get('rank_column')] -%}
    {%- endif %}

    {# Select unique source records #}
    {{dbt_utils.log_info(dbtvault.is_any_incremental())}}
    -- NEW MAS
    WITH source_data AS (
        WITH internal_source_data AS (
            {%- if model.config.materialized == 'vault_insert_by_rank' %}
            SELECT
                {{ dbtvault.prefix(
                    source_cols_with_rank,
                    's',
                    alias_target = 'source'
                ) }}
            {%- else %}
            SELECT
                {{ dbtvault.prefix(
                    source_cols,
                    's',
                    alias_target = 'source'
                ) }}
            {%- endif %},
            ROW_NUMBER() OVER(
                PARTITION BY
                    {{ dbtvault.prefix([src_pk], 's') }},
                    {{ dbtvault.prefix(cdk_cols, 's', alias_target = 'source') }},
                    {{ dbtvault.prefix([src_hashdiff], 's', alias_target = 'source') }}
                    ORDER BY 
                    {{ dbtvault.prefix([src_ldts],'s') }}
            ) as row_num
               
            FROM
                {{ ref(source_model) }} AS s
            WHERE
                {{ dbtvault.multikey(
                    [src_pk],
                    prefix = 's',
                    condition = 'IS NOT NULL'
                ) }}

                {%- for child_key in cdk_cols %}
                    AND {{ dbtvault.multikey(
                        child_key,
                        prefix = 's',
                        condition = 'IS NOT NULL'
                    ) }}
                {%- endfor %}

                {%- if model.config.materialized == 'vault_insert_by_period' %}
                    AND __PERIOD_FILTER__ {%- elif model.config.materialized == 'vault_insert_by_rank' %}
                    AND __RANK_FILTER__
                {%- endif %}
        )
        {%- if model.config.materialized == 'vault_insert_by_rank' %}
        SELECT
            DISTINCT {{ dbtvault.prefix(
                source_cols_with_rank,
                'isd',
                alias_target = 'source'
            ) }}
        {%- else %}
        SELECT
            DISTINCT {{ dbtvault.prefix(
                source_cols,
                'isd',
                alias_target = 'source'
            ) }}
        {%- endif %}
        
        FROM
            internal_source_data AS isd
            WHERE
            isd.row_num = 1
    ),
    -- Now get existing satellite records.
{% if dbtvault.is_any_incremental() %}
    sat_data AS (
        WITH internal_sat_data AS (
            {%- if model.config.materialized == 'vault_insert_by_rank' %}
            SELECT
                {{ dbtvault.prefix(
                    source_cols_with_rank,
                    'i_mas',
                    alias_target = 'target'
                ) }}
            {%- else %}
            SELECT
                {{ dbtvault.prefix(
                    source_cols,
                    'i_mas',
                    alias_target = 'target'
                ) }}
            {%- endif %},
            ROW_NUMBER() OVER(
                PARTITION BY
                    {{ dbtvault.prefix([src_pk], 'i_mas') }},
                    {{ dbtvault.prefix(cdk_cols, 'i_mas', alias_target = 'target') }},
                    i_mas.hashdiff
                    ORDER BY 
                    {{ dbtvault.prefix([src_ldts],'i_mas') }} desc
            ) as row_num
               

            FROM
                {{ this }} AS i_mas
            WHERE
                {{ dbtvault.multikey(
                    [src_pk],
                    prefix = 'i_mas',
                    condition = 'IS NOT NULL'
                ) }}

                {%- for child_key in cdk_cols %}
                    AND {{ dbtvault.multikey(
                        child_key,
                        prefix = 'i_mas',
                        condition = 'IS NOT NULL'
                    ) }}
                {%- endfor %}

                {%- if model.config.materialized == 'vault_insert_by_period' %}
                    AND __PERIOD_FILTER__
                {%- elif model.config.materialized == 'vault_insert_by_rank' %}
                    AND __RANK_FILTER__
                {%- endif %}
        )
        {%- if model.config.materialized == 'vault_insert_by_rank' %}
        SELECT
            DISTINCT {{ dbtvault.prefix(
                source_cols_with_rank,
                'mas',
                alias_target = 'target'
            ) }}
        {%- else %}
        SELECT
            DISTINCT {{ dbtvault.prefix(
                source_cols,
                'mas',
                alias_target = 'target'
            ) }}
        {%- endif %}
            , 1 AS src_check
        FROM
            internal_sat_data AS mas

            WHERE
            mas.row_num = 1

    ),
    {# Join records based on hashdiff, pk, cdk -#}
    joined_records AS (
        select
        {%- if model.config.materialized == 'vault_insert_by_rank' %}
             {{ dbtvault.prefix(
                source_cols_with_rank,
                's',
                alias_target = 'source'
            ) }}
        {%- else %}
         {{ dbtvault.prefix(
                source_cols,
                's',
                alias_target = 'source'
            ) }}
        {%- endif %}
            ,n.src_check
            FROM
            source_data s
            left join sat_data n
            ON {{ dbtvault.multikey(
                    src_pk,
                    prefix = ['s', 'n'],
                    condition = '='
                ) }}
                AND  {{dbtvault.prefix([src_hashdiff], 's')}} = n.hashdiff
                AND {{ dbtvault.multikey(
                    cdk_cols,
                    prefix = ['s', 'n'],
                    condition = '='
                ) }}
            
    ),
    {% endif %}
    records_to_insert AS (
        SELECT DISTINCT
        {%- if model.config.materialized == 'vault_insert_by_rank' %}
             {{ dbtvault.alias_all(
                source_cols_with_rank,
                'j'
            ) }}
        {%- else %}
         {{ dbtvault.alias_all(
                source_cols,
                'j'
            ) }}
        {%- endif %}
        {% if dbtvault.is_any_incremental() %}
        FROM joined_records j
        WHERE j.src_check Is NULL
        {% else %}
        FROM source_data j
        {% endif %}
    )
SELECT
    *
FROM
    records_to_insert
{%- endmacro -%}
{# 
    {% if dbtvault.is_any_incremental() %}
    active_records AS (
        SELECT
            {{ dbtvault.prefix(
                cols_for_latest,
                'lr',
                alias_target = 'target'
            ) }},
            lg.latest_count
        FROM
            latest_records AS lr
            INNER JOIN latest_group_details AS lg
            ON {{ dbtvault.multikey(
                [src_pk],
                prefix = ['lr', 'lg'],
                condition = '='
            ) }}
            AND {{ dbtvault.prefix(
                [src_ldts],
                'lr'
            ) }} = {{ dbtvault.prefix(
                [src_ldts],
                'lg'
            ) }}
    ),
    active_records_staging AS (
        SELECT
            {{ dbtvault.alias_all(
                source_cols,
                'stage'
            ) }}
        FROM
            source_data stage
            INNER JOIN active_records
            ON {{ dbtvault.multikey(
                [src_pk],
                prefix = ['stage', 'active_records'],
                condition = '='
            ) }}
            AND {{ dbtvault.prefix(
                [src_hashdiff],
                'stage'
            ) }} = active_records.hashdiff
            AND {{ dbtvault.multikey(
                cdk_cols,
                prefix = ['stage', 'active_records'],
                condition = '='
            ) }}
            AND stage.source_count = active_records.latest_count
    ),
    {% endif %}
    records_to_insert AS (
        SELECT
            {{ dbtvault.alias_all(
                source_cols,
                'source_data'
            ) }}
        FROM
            source_data {# if any_incremental -#}
            {% if dbtvault.is_any_incremental() %}
                LEFT JOIN active_records_staging
                ON {{ dbtvault.multikey(
                    src_pk,
                    prefix = ['source_data', 'active_records_staging'],
                    condition = '='
                ) }}
                AND  {{dbtvault.prefix([src_hashdiff], 'source_data')}} = active_records_staging.hashdiff
                AND {{ dbtvault.multikey(
                    cdk_cols,
                    prefix = ['source_data', 'active_records_staging'],
                    condition = '='
                ) }}
            WHERE
                {{ dbtvault.multikey(
                    src_pk,
                    prefix = 'active_records_staging',
                    condition = 'IS NULL'
                ) }}
                AND active_records_staging.hashdiff IS NULL
                AND {{ dbtvault.multikey(
                    cdk_cols,
                    prefix = 'active_records_staging',
                    condition = 'IS NULL'
                ) }}
                {# endif any_incremental -#}
            {%- endif %}
    ) #}