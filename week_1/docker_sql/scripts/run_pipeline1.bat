
docker run -d ^
    -e POSTGRES_USER="root" ^
    -e POSTGRES_PASSWORD="root" ^
    -e POSTGRES_DB="ny_taxi" ^
    -v "ny_taxi_postgres_data:/var/lib/postgresql/data:Z" ^
    -p 5432:5432 ^
    --network=pg-network ^
    --name pg-database ^
    postgres:13 ^

