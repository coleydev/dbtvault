{%- macro stage(
        include_source_columns = none,
        source_model = none,
        hashed_columns = none,
        derived_columns = none,
        null_columns = none,
        ranked_columns = none
    ) -%}
    {%- if include_source_columns is none -%}
        {%- set include_source_columns = true -%}
    {%- endif -%}

    {{ print(source_model) }}

    {{- dbtvault.prepend_generated_by() }}
    {{ adapter.dispatch('stage','dbtvault')(include_source_columns = include_source_columns, source_model = source_model, hashed_columns = hashed_columns, derived_columns = derived_columns, null_columns = null_columns, ranked_columns = ranked_columns) -}}
{%- endmacro -%}

{%- macro default__stage(include_source_columns, source_model, hashed_columns, derived_columns, null_columns, ranked_columns) -%}
    {% if (source_model is none) and execute %}
        {%- set error_message -%}
        staging error: missing source_model configuration. A source model name must be provided.e.g.[REF STYLE] source_model: model_name or [SOURCES STYLE] source_model: source_name: source_table_name
        {%- endset -%}
        {{- exceptions.raise_compiler_error(error_message) -}}
    {%- endif -%}

    {#- Check for source format or ref format and create
    relation OBJECT FROM source_model - #}
    {{print('!!!!!')}}
    {{print(source_model)}}
    {% if source_model is mapping and source_model is not none -%}
        {%- set source_name = source_model | first -%}
        {%- set source_table_name = source_model[source_name] -%}
        {%- set source_relation = source(source_name, source_table_name) -%}
        {%- set all_source_columns = dbtvault.source_columns(source_relation = source_relation) -%}
        {%- elif source_model is not mapping and source_model is not none -%}
        {%- set source_relation = ref(source_model) -%}
        {%- set all_source_columns = dbtvault.source_columns(source_relation = source_relation) -%}
    {%- else -%}
        {%- set all_source_columns = [] -%}
    {%- endif -%}

    {%- set derived_column_names = dbtvault.extract_column_names(derived_columns) | map('upper') | list -%}
    {%- set null_column_names = dbtvault.extract_null_column_names(null_columns) | map('upper') | list -%}
    {%- set hashed_column_names = dbtvault.extract_column_names(hashed_columns) | map('upper') | list -%}
    {%- set ranked_column_names = dbtvault.extract_column_names(ranked_columns) | map('upper') | list -%}
    {%- set exclude_column_names = derived_column_names + null_column_names + hashed_column_names | map('upper') | list -%}
    {%- set source_and_derived_column_names = (
        all_source_columns + derived_column_names
    ) | map('upper') | unique | list -%}
    {%- set source_columns_to_select = dbtvault.process_columns_to_select(
        all_source_columns,
        exclude_column_names
    ) -%}
    {%- set derived_columns_to_select = dbtvault.process_columns_to_select(
        source_and_derived_column_names,
        null_column_names + hashed_column_names
    ) | unique | list -%}
    {%- set derived_and_null_columns_to_select = dbtvault.process_columns_to_select(
        source_and_derived_column_names + null_column_names,
        hashed_column_names
    ) | unique | list -%}
    {%- set final_columns_to_select = [] -%}
    {#- Include source columns in final column selection if true -#}
    {%- if include_source_columns -%}
        {%- if dbtvault.is_nothing(derived_columns) and dbtvault.is_nothing(null_columns) and dbtvault.is_nothing(hashed_columns) and dbtvault.is_nothing(ranked_columns) -%}
            {%- set final_columns_to_select = final_columns_to_select + all_source_columns -%}
        {%- else -%}
            {#- Only include non-overriden columns if not just source columns -#}
            {%- set final_columns_to_select = final_columns_to_select + source_columns_to_select -%}
        {%- endif -%}
    {%- endif %}

    WITH source_data AS (
        SELECT
            {{- "\n\n " ~ dbtvault.print_list(dbtvault.escape_column_names(all_source_columns)) if all_source_columns
            else " *" }}
        FROM
            {{ source_relation }}

            {%- set last_cte = "source_data" %}) {%- if dbtvault.is_something(derived_columns) -%},
                derived_columns AS (
                    SELECT
                        {{ dbtvault.derive_columns(
                            source_relation = source_relation,
                            columns = derived_columns
                        ) | indent(4) }}
                    FROM
                        {{ last_cte }}

                        {%- set last_cte = "derived_columns" -%}
                        {%- set final_columns_to_select = final_columns_to_select + derived_column_names %}
                )
            {%- endif -%}

            {% if dbtvault.is_something(null_columns) -%},
                null_columns AS (
                    SELECT
                        {{ dbtvault.print_list(
                            dbtvault.escape_column_names(derived_columns_to_select)
                        ) }}
                        {{ "," if dbtvault.is_something(derived_columns_to_select)
                        else "" }}
                        {{ dbtvault.null_columns(
                            source_relation = none,
                            columns = null_columns
                        ) | indent(4) }}
                    FROM
                        {{ last_cte }}

                        {%- set last_cte = "null_columns" -%}
                        {%- set final_columns_to_select = final_columns_to_select + null_column_names %}
                )
            {%- endif -%}

            {% if dbtvault.is_something(hashed_columns) -%},
                hashed_columns AS (
                    SELECT
                        {{ dbtvault.print_list(
                            dbtvault.escape_column_names(derived_and_null_columns_to_select)
                        ) }},
                        {% set processed_hash_columns = dbtvault.process_hash_column_excludes(
                            hashed_columns,
                            all_source_columns
                        ) -%}
                        {{- dbtvault.hash_columns(
                            columns = processed_hash_columns
                        ) | indent(4) }}
                    FROM
                        {{ last_cte }}

                        {%- set last_cte = "hashed_columns" -%}
                        {%- set final_columns_to_select = final_columns_to_select + hashed_column_names %}
                )
            {%- endif -%}

            {% if dbtvault.is_something(ranked_columns) -%},
                ranked_columns AS (
                    SELECT
                        *,
                        {{ dbtvault.rank_columns(
                            columns = ranked_columns
                        ) | indent(4) if dbtvault.is_something(ranked_columns) }}
                    FROM
                        {{ last_cte }}

                        {%- set last_cte = "ranked_columns" -%}
                        {%- set final_columns_to_select = final_columns_to_select + ranked_column_names %}
                )
            {%- endif -%},
            columns_to_select AS (
                SELECT
                    {{ dbtvault.print_list(
                        dbtvault.escape_column_names(
                            final_columns_to_select | unique | list
                        )
                    ) }}
                FROM
                    {{ last_cte }}
            )
        SELECT
            *
        FROM
            columns_to_select
{%- endmacro -%}
