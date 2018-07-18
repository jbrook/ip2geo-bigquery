# IP based geo-location with Google BigQuery #

Create a BigQuery table to allow you to look up the country for an IP address. This work could easily be extended to make use of city data too.

This repository is based on the blog post [here](https://mac-blog.org.ua/maxmind-bigquery/) and allows you to make use of the [Maxmind GeoIP2 databases](https://www.maxmind.com/en/geoip2-databases). These databases are [licensed by Maxmind](https://www.maxmind.com/en/site-license-overview).

Save the dataset that we want to work with in an environment variable:
```
export DATASET="<project_id:dataset>"
```

Clean up any existing data. The Maxmind data is updated regularly and should be reloaded. This whole process could easily be automated with a service like [Google Cloud Composer](https://cloud.google.com/composer/), which is based on Apache Airflow.
```
bq rm -f -t $DATASET.maxmind
rm *.txt *.csv *.zip
```

Download the CSV versions of the Maxmind databases:
```
curl -O http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip
curl -O http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN-CSV.zip
```

Uncompress them:
```
unzip -jo GeoLite2-Country-CSV.zip
unzip -jo GeoLite2-ASN-CSV.zip
```

Load the data into temporary BigQuery tables:
```
bq load --skip_leading_rows=1 $DATASET.tmp_country_ip GeoLite2-Country-Blocks-IPv4.csv "network:string,geoname_id:integer,registered_country_geoname_id:integer,represented_country_geoname_id:integer,is_anonymous_proxy:integer,is_satellite_provider:integer"

bq load --skip_leading_rows=1 $DATASET.tmp_country_labels GeoLite2-Country-Locations-en.csv "geoname_id:integer,locale_code:string,continent_code:string,continent_name:string,country_iso_code:string,country_name:string,is_in_european_union:boolean"

bq load --skip_leading_rows=1 $DATASET.tmp_asn GeoLite2-ASN-Blocks-IPv4.csv "network:string,autonomous_system_number:integer,autonomous_system_organization:string"
```

We will "rematerialize" the data in a useful table format for querying with BigQuery. First update the SQL to reference your own dataset:
```
sed -i 's/DATASET/'$DATASET'/g' maxmind.sql
sed -i 's/:/./g' maxmind.sql
```

or on a Mac:
```
sed -i'' -e 's/DATASET/'$DATASET'/g' maxmind.sql
sed -i'' -e 's/:/./g' maxmind.sql
```

Run the job to create the new 'maxmind' table:
```
bq query --use_legacy_sql=false --allow_large_results --destination_table=$DATASET.maxmind "$(cat maxmind.sql)"
```

Tidy up the temporary data:
```
bq rm -f -t $DATASET.tmp_country_ip
bq rm -f -t $DATASET.tmp_country_labels
bq rm -f -t $DATASET.tmp_asn
```

Run a simple query:
```
SELECT
  country_name,
  autonomous_system_organization
FROM
  `<your dataset>.maxmind`
WHERE
  CAST(NET.IPV4_TO_INT64(NET.IP_FROM_STRING("213.46.237.24"))/(256*256*256) AS INT64) = class_a
  AND NET.IPV4_TO_INT64(NET.IP_FROM_STRING("213.46.237.24")) BETWEEN start_num
  AND end_num
LIMIT
  1
```