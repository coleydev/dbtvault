/*
 * Copyright (c) Business Thinking Ltd. 2019-2023
 * This software includes code developed by the dbtvault Team at Business Thinking Ltd. Trading as Datavault
 */

{%- macro cast_date(column_str, as_string=false, datetime=false, alias=none, date_type=none) -%}
    {%- if datetime -%}
        {{- dbtvault.cast_datetime(column_str=column_str, as_string=as_string, alias=alias, date_type=date_type) -}}
    {%- else -%}
        {{ return(adapter.dispatch('cast_date', 'dbtvault')(column_str=column_str, as_string=as_string, alias=alias)) }}
    {%- endif -%}
{%- endmacro -%}

{%- macro snowflake__cast_date(column_str, as_string=false, alias=none) -%}

    {%- if not as_string -%}
        TO_DATE({{ column_str }})
    {%- else -%}
        TO_DATE('{{ column_str }}')
    {%- endif -%}

    {%- if alias %} AS {{ alias }} {%- endif %}

{%- endmacro -%}


{%- macro sqlserver__cast_date(column_str, as_string=false, alias=none) -%}

    {%- if not as_string -%}
        CONVERT(DATE, {{ column_str }})
    {%- else -%}
        CONVERT(DATE, '{{ column_str }}')
    {%- endif -%}

    {%- if alias %} AS {{ alias }} {%- endif %}


{%- endmacro -%}


{%- macro bigquery__cast_date(column_str, as_string=false, alias=none) -%}

    {%- if not as_string -%}
        DATE({{ column_str }})
    {%- else -%}
        DATE('{{ column_str }}')
    {%- endif -%}

    {%- if alias %} AS {{ alias }} {%- endif %}

{%- endmacro -%}


{%- macro databricks__cast_date(column_str, as_string=false, alias=none) -%}

    {{ dbtvault.snowflake__cast_date(column_str=column_str, as_string=as_string, alias=alias)}}

{%- endmacro -%}


{%- macro postgres__cast_date(column_str, as_string=false, alias=none) -%}

    {%- if not as_string -%}
        TO_DATE({{ column_str }})
    {%- else -%}
        TO_DATE('{{ column_str }}', 'YYY-MM-DD')
    {%- endif -%}

    {%- if alias %} AS {{ alias }} {%- endif %}

{%- endmacro -%}

{%- macro redshift__cast_date(column_str, as_string=false, datetime=false, alias=none) -%}

    {%- if datetime -%}
        {%- if not as_string -%}
            TO_TIMESTAMP({{ column_str }}, 'YYYY-MM-DD HH24:MI:SS')
        {%- else -%}
            TO_TIMESTAMP('{{ column_str }}',  'YYYY-MM-DD HH24:MI:SS')
        {%- endif -%}
    {%- else -%}
        {%- if not as_string -%}
            TO_DATE({{ column_str }}, 'YYYY-MM-DD')
        {%- else -%}
            TO_DATE('{{ column_str }}' , 'YYYY-MM-DD')
        {%- endif -%}
    {%- endif -%}

    {%- if alias %} AS {{ alias }} {%- endif %}

{%- endmacro -%}