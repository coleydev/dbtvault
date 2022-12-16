/*
 *  Copyright (c) Business Thinking Ltd. 2019-2022
 *  This software includes code developed by the dbtvault Team at Business Thinking Ltd. Trading as Datavault
 */

{%- macro max_datetime() -%}

    {%- do return(adapter.dispatch('max_datetime', 'dbtvault')()) -%}

{%- endmacro %}

{%- macro default__max_datetime() %}

    {%- do return(var('max_datetime', '9999-12-31 23:59:59.999999')) -%}

{%- endmacro -%}

{%- macro sqlserver__max_datetime() -%}

    {%- do return(var('max_datetime', '9999-12-31 23:59:59.9999999')) -%}

{%- endmacro -%}

{%- macro bigquery__max_datetime() -%}

    {%- do return(var('max_datetime', '9999-12-31 23:59:59.999999')) -%}

{%- endmacro -%}
