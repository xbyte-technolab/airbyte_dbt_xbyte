{{ config (
    materialized="table"
)}}
    sys.`master_item__dbt_tmp`
  as (

with __dbt__cte__master_item_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: sys._airbyte_raw_master_item
select
    json_value(_airbyte_data,
    '$."item_id"' RETURNING CHAR) as item_id,
    json_value(_airbyte_data,
    '$."item_name"' RETURNING CHAR) as item_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from sys._airbyte_raw_master_item as table_alias
-- master_item
where 1 = 1
),  __dbt__cte__master_item_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__master_item_ab1
select
    cast(item_id as
    signed
) as item_id,
    cast(item_name as char(1024)) as item_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from __dbt__cte__master_item_ab1
-- master_item
where 1 = 1
),  __dbt__cte__master_item_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__master_item_ab2
select
    md5(cast(concat(coalesce(cast(item_id as char), ''), '-', coalesce(cast(item_name as char), '')) as char)) as _airbyte_master_item_hashid,
    tmp.*
from __dbt__cte__master_item_ab2 tmp
-- master_item
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__master_item_ab3
select
    item_id,
    item_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at,
    _airbyte_master_item_hashid
from __dbt__cte__master_item_ab3
-- master_item from sys._airbyte_raw_master_item
where 1 = 1
  )
