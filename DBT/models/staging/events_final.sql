--Test Run Using: dbt build --select events_final --vars '{'is_test_run': 'true'}'
with 
source as (

    select * from {{ source('staging', 'events_stg') }}

),

renamed as (

    select
        globaleventid,
        PARSE_DATE('%Y%m%d', CAST(sqldate AS STRING)) as sqldate, --convert integer to date
        monthyear,
        year,
        fractiondate,
        actor1code,
        actor1name,
        actor1countrycode,
        actor1knowngroupcode,
        actor1ethniccode,
        actor1religion1code,
        actor1religion2code,
        actor1type1code,
        actor1type2code,
        actor1type3code,
        actor2code,
        actor2name,
        actor2countrycode,
        actor2knowngroupcode,
        actor2ethniccode,
        actor2religion1code,
        actor2religion2code,
        actor2type1code,
        actor2type2code,
        actor2type3code,
        isrootevent,
        eventcode,
        eventbasecode,
        eventrootcode,
        quadclass,
        goldsteinscale,
        nummentions,
        numsources,
        numarticles,
        avgtone,
        actor1geo_type,
        actor1geo_fullname,
        actor1geo_countrycode,
        actor1geo_adm1code,
        actor1geo_adm2code,
        actor1geo_lat,
        actor1geo_long,
        actor1geo_featureid,
        actor2geo_type,
        actor2geo_fullname,
        actor2geo_countrycode,
        actor2geo_adm1code,
        actor2geo_adm2code,
        actor2geo_lat,
        actor2geo_long,
        actor2geo_featureid,
        actiongeo_type,
        actiongeo_fullname,
        actiongeo_countrycode,
        actiongeo_adm1code,
        actiongeo_adm2code,
        actiongeo_lat,
        actiongeo_long,
        actiongeo_featureid,
        PARSE_DATETIME('%Y%m%d%H%M%S', CAST(dateadded AS STRING)) as dateadded, --convert integer to datetime
        sourceurl

    from source

)

select * from renamed

{% if var('is_test_run', default=true) %}

  limit 10

{% endif %}