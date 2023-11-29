create  table
    sys.`payment__dbt_tmp`
  as (

with __dbt__cte__payment_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: sys._airbyte_raw_payment
select
    json_value(_airbyte_data,
    '$."amount"' RETURNING CHAR) as amount,
    json_value(_airbyte_data,
    '$."payment_id"' RETURNING CHAR) as payment_id,
    json_value(_airbyte_data,
    '$."staff_id"' RETURNING CHAR) as staff_id,
    json_value(_airbyte_data,
    '$."last_update"' RETURNING CHAR) as last_update,
    json_value(_airbyte_data,
    '$."customer_id"' RETURNING CHAR) as customer_id,
    json_value(_airbyte_data,
    '$."payment_date"' RETURNING CHAR) as payment_date,
    json_value(_airbyte_data,
    '$."rental_id"' RETURNING CHAR) as rental_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from sys._airbyte_raw_payment as table_alias
-- payment
where 1 = 1
),  __dbt__cte__payment_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__payment_ab1
select
    cast(amount as
    float
) as amount,
    cast(payment_id as
    signed
) as payment_id,
    cast(staff_id as
    signed
) as staff_id,
    cast(nullif(last_update, '') as char(1024)) as last_update,
    cast(customer_id as
    signed
) as customer_id,
        case when payment_date regexp '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*' THEN STR_TO_DATE(SUBSTR(payment_date, 1, 19), '%Y-%m-%dT%H:%i:%S')
        else cast(if(payment_date = '', NULL, payment_date) as datetime)
        end as payment_date
        ,
    cast(rental_id as
    signed
) as rental_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from __dbt__cte__payment_ab1
-- payment
where 1 = 1
),  __dbt__cte__payment_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__payment_ab2
select
    md5(cast(concat(coalesce(cast(amount as char), ''), '-', coalesce(cast(payment_id as char), ''), '-', coalesce(cast(staff_id as char), ''), '-', coalesce(cast(last_update as char), ''), '-', coalesce(cast(customer_id as char), ''), '-', coalesce(cast(payment_date as char), ''), '-', coalesce(cast(rental_id as char), '')) as char)) as _airbyte_payment_hashid,
    tmp.*
from __dbt__cte__payment_ab2 tmp
-- payment
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__payment_ab3
select
    amount,
    payment_id,
    staff_id,
    last_update,
    customer_id,
    payment_date,
    rental_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,

    CURRENT_TIMESTAMP
 as _airbyte_normalized_at,
    _airbyte_payment_hashid
from __dbt__cte__payment_ab3
-- payment from sys._airbyte_raw_payment
where 1 = 1
  )
