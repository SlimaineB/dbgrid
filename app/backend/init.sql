-- Charge l'extension httpfs (si elle est dans ./extensions/httpfs.duckdb_extension)
LOAD 'httpfs';

-- Exemple : créer une table temporaire pour test
CREATE TABLE IF NOT EXISTS demo AS SELECT 42 AS answer;

-- D’autres commandes SQL personnalisées ici...
