URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
URL2="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

# docker network create pg-network
# docker volume create --name ny_taxi_postgres_data --opt type=none --opt device=./ny_taxi_postgres_data --opt o=bind

docker run -d \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v "ny_taxi_postgres_data:/var/lib/postgresql/data:Z" \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13

docker run -it \
    --network=pg-network \
    ingest_greentaxi:v1 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}

docker run -it \
    --network=pg-network \
    ingest_zones:v1 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=taxi_zones \
    --url=${URL2}