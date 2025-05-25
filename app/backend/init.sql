-- Charge l'extension httpfs (si elle est dans ./extensions/httpfs.duckdb_extension)
LOAD 'httpfs';

-- For local minio

SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;



-- Uncomment for S3
-- SET s3_region='eu-west-3'
-- SET s3_url_style='path'  # ou 'vhost' selon le bucket
-- SET s3_use_ssl=true     # vrai S3 = SSL activé


-- Exemple : créer une table temporaire pour test
CREATE TABLE IF NOT EXISTS demo AS SELECT 42 AS answer;

-- D’autres commandes SQL personnalisées ici...
