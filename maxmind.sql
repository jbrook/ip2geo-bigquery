WITH raw AS (

SELECT
REGEXP_REPLACE(network, r'/\d+$', '') as net,
CAST(REGEXP_REPLACE(network, r'^\d+\.\d+\.\d+\.\d+/', '') AS int64) as mask,
CAST(CASE WHEN (-2 + POW(2, 32 - CAST(REGEXP_REPLACE(network, r'^\d+\.\d+\.\d+\.\d+/', '') AS int64))) < 1 THEN 1 ELSE (-2 + POW(2, 32 - CAST(REGEXP_REPLACE(network, r'^\d+\.\d+\.\d+\.\d+/', '') AS int64))) END AS INT64) as hosts,
* FROM `DATASET.tmp_country_ip`

), num AS (

select
NET.IPV4_TO_INT64(NET.IP_FROM_STRING(net)) + 1 as start_num,
NET.IPV4_TO_INT64(NET.IP_FROM_STRING(net)) + hosts as end_num,
* from raw

), ip AS (

select
cast(start_num/(256*256*256) as int64) as class_a,
NET.IP_TO_STRING(NET.IPV4_FROM_INT64(start_num)) as start_ip,
NET.IP_TO_STRING(NET.IPV4_FROM_INT64(end_num)) as end_ip,
* from num

), maxmind AS (

select
ip.*,
l.locale_code, l.continent_code, l.continent_name, l.country_iso_code, l.country_name,
a.autonomous_system_number, a.autonomous_system_organization
from ip
left join `DATASET.tmp_country_labels` l on ip.geoname_id = l.geoname_id
left join `DATASET.tmp_asn` a on ip.network = a.network

)
select * from maxmind
