/*
 * Copyright (c) Business Thinking Ltd. 2019-2023
 * This software includes code developed by the dbtvault Team at Business Thinking Ltd. Trading as Datavault
 */

{%- macro hash(columns=none, alias=none, is_hashdiff=false, columns_to_escape=none) -%}

    {%- if is_hashdiff is none -%}
        {%- set is_hashdiff = false -%}
    {%- endif -%}

    {{- adapter.dispatch('hash', 'dbtvault')(columns=columns, alias=alias,
                                             is_hashdiff=is_hashdiff, columns_to_escape=columns_to_escape) -}}

{%- endmacro %}

{%- macro default__hash(columns, alias, is_hashdiff, columns_to_escape) -%}

{%- set hash = var('hash', 'md5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hash_alg = dbtvault.select_hash_alg(hash) -%}

{%- set standardise = dbtvault.standard_column_wrapper() %}

{#- Alpha sort columns before hashing if a hashdiff -#}
{%- if is_hashdiff and dbtvault.is_list(columns) -%}
    {%- set columns = columns|sort -%}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}

    {%- if dbtvault.is_something(columns_to_escape) -%}
        {%- if column_str in columns_to_escape -%}
            {%- set column_str = dbtvault.escape_column_name(column_str) -%}
        {%- endif -%}
    {%- endif -%}

    {{ hash_alg | replace('[HASH_STRING_PLACEHOLDER]', standardise | replace('[EXPRESSION]', column_str)) }} AS {{ alias | indent(4) }}

{#- Else a list of columns to hash -#}
{%- else -%}

    {%- set all_null = [] -%}
    {%- set processed_columns = [] -%}

    {%- for column in columns -%}
        {%- if dbtvault.is_something(columns_to_escape) -%}
            {%- if column in columns_to_escape -%}
                {%- set column = dbtvault.escape_column_name(column) -%}
            {%- endif -%}
        {%- endif -%}

        {%- set column_str = dbtvault.as_constant(column) -%}

        {%- set column_expression = dbtvault.null_expression(column_str) -%}

        {%- do all_null.append(null_placeholder_string) -%}
        {%- do processed_columns.append(column_expression) -%}

    {% endfor -%}

    {% if not is_hashdiff -%}

        {%- set concat_sql -%}
        NULLIF({{ dbtvault.concat_ws(processed_columns, separator=concat_string) -}} {{ ', ' -}}
               '{{ all_null | join(concat_string) }}')
        {%- endset -%}

        {%- set hashed_column -%}
        {{ hash_alg | replace('[HASH_STRING_PLACEHOLDER]', concat_sql) }} AS {{ alias }}
        {%- endset -%}

    {%- else -%}
        {% if dbtvault.is_list(processed_columns) and processed_columns | length > 1 %}
            {%- set hashed_column -%}
                {{ hash_alg | replace('[HASH_STRING_PLACEHOLDER]', dbtvault.concat_ws(processed_columns, separator=concat_string)) }} AS {{ alias }}
            {%- endset -%}
        {%- else -%}
            {%- set hashed_column -%}
                {{ hash_alg | replace('[HASH_STRING_PLACEHOLDER]', processed_columns[0]) }} AS {{ alias }}
            {%- endset -%}
        {%- endif -%}
    {%- endif -%}

    {{ hashed_column }}

{%- endif -%}

{%- endmacro -%}


{%- macro bigquery__hash(columns, alias, is_hashdiff, columns_to_escape) -%}

    {{ dbtvault.default__hash(columns=columns, alias=alias, is_hashdiff=is_hashdiff, columns_to_escape=columns_to_escape) }}

{%- endmacro -%}


{%- macro sqlserver__hash(columns, alias, is_hashdiff, columns_to_escape) -%}

    {{ dbtvault.default__hash(columns=columns, alias=alias, is_hashdiff=is_hashdiff, columns_to_escape=columns_to_escape) }}

{%- endmacro -%}


{%- macro postgres__hash(columns, alias, is_hashdiff, columns_to_escape) -%}

    {{ dbtvault.default__hash(columns=columns, alias=alias, is_hashdiff=is_hashdiff, columns_to_escape=columns_to_escape) }}

{%- endmacro -%}

{%- macro redshift__hash(columns, alias, is_hashdiff) -%}


{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{#- Select hashing algorithm -#}
{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5' -%}
{%- elif hash == 'SHA' -%}
    {%- set hash_alg = 'SHA256' -%}
{%- else -%}
    {%- set hash_alg = 'MD5' -%}
{%- endif -%}

{#- Select hashing expression (left and right sides) -#}
{#- * MD5 is simple function call to md5(val) -#}
{#- * SHA256 needs input cast to BYTEA and then its BYTEA result encoded as hex text output -#}
{#-   e.g. ENCODE(SHA256(CAST(val AS BYTEA)), 'hex') -#}
{#- Ref: https://www.postgresql.org/docs/11/functions-binarystring.html  -#}
{%- if hash_alg == 'MD5' -%}
    {%- set hash_expr_left = 'MD5(' -%}
    {%- set hash_expr_right = ")" -%}
{%- elif hash_alg == 'SHA256' -%}
    {%- set hash_expr_left = 'SHA2(CAST(' -%}
    {%- set hash_expr_right = " AS VARCHAR), 256)" -%}
{%- endif -%}

{%- set standardise = "NULLIF(UPPER(TRIM(CAST([EXPRESSION] AS VARCHAR))), '')" -%}

{#- Alpha sort columns before hashing if a hashdiff -#}
{%- if is_hashdiff and dbtvault.is_list(columns) -%}
    {%- set columns = columns|sort -%}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}
    {%- if dbtvault.is_expression(column_str) -%}
        {%- set escaped_column_str = column_str -%}
    {%- else -%}
        {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
    {%- endif -%}

    {{- "TO_VARBYTE(UPPER({}{}{}), 'hex') AS {}".format(hash_expr_left, standardise | replace('[EXPRESSION]', escaped_column_str), hash_expr_right, dbtvault.escape_column_names(alias)) | indent(4) -}}

{#- Else a list of columns to hash -#}
{%- else -%}
    {%- set all_null = [] -%}

    {%- if is_hashdiff -%}
        {{- "TO_VARBYTE(UPPER({}".format(hash_expr_left) | indent(4) -}}
    {%- else -%}
        {{- "TO_VARBYTE(UPPER({}NULLIF(".format(hash_expr_left) | indent(4) -}}
    {%- endif -%}

    {%- for column in columns -%}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- set column_str = dbtvault.as_constant(column) -%}
        {%- if dbtvault.is_expression(column_str) -%}
            {%- set escaped_column_str = column_str -%}
        {%- else -%}
            {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
        {%- endif -%}

        {{- "\nCOALESCE({}, '{}')".format(standardise | replace('[EXPRESSION]', escaped_column_str), null_placeholder_string) | indent(4) -}}

        {{- "|| '{}' ||".format(concat_string) if not loop.last -}}

        {%- if loop.last -%}
            {% if is_hashdiff %}
                {{- "\n){} , 'hex') AS {}".format(hash_expr_right, dbtvault.escape_column_names(alias)) -}}
            {%- else -%}
                {{- "\n, '{}')) {} , 'hex') AS {} ".format(all_null | join(""), hash_expr_right, dbtvault.escape_column_names(alias)) -}}
            {%- endif -%}       

        {% else %} 

            {%- do all_null.append(concat_string) -%} 

        {%- endif -%}
    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}


{%- macro databricks__hash(columns, alias, is_hashdiff, columns_to_escape) -%}

    {{ dbtvault.default__hash(columns=columns, alias=alias, is_hashdiff=is_hashdiff, columns_to_escape=columns_to_escape) }}

{%- endmacro -%}