
# dbgrid - DuckDB in Cluster Mode

A **DuckDB cluster** deployed with Docker Compose (designed to scale as a stateless cluster).

---

## Quickstart with Docker Compose

```bash
docker compose up -d
```

Set up MinIO client alias and create a bucket:

```bash
mc alias set localminio http://localhost:9000 minioadmin minioadmin
mc mb localminio/test-bucket
mc cp data/test_data.parquet localminio/test-bucket/
```

---

## Configure DuckDB to access MinIO parquet files

Run the following commands in DuckDB SQL shell or via your backend:

```sql
-- Set MinIO credentials for httpfs extension
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='griddb-minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;

-- Create a table in DuckDB from parquet files stored in MinIO
CREATE TABLE my_table AS
SELECT * FROM read_parquet('s3://test-bucket/*.parquet');
```

---

## Cleanup

```bash
docker compose down --volumes --remove-orphans
```

---

## Notes

- DuckDB runs in cluster mode with multiple stateless backend instances.
- MinIO serves as external S3-compatible object storage for parquet data.
- The backend exposes a FastAPI service to execute SQL queries on DuckDB.
- The frontend uses Streamlit to interact with the backend and display query results.
- Configure your backend URL and SSL options in the frontend.
