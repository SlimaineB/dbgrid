import pandas as pd
import numpy as np

# Générer un DataFrame avec 15 colonnes différentes et 100 lignes
num_rows = 100
df = pd.DataFrame({
    "col_int": np.random.randint(0, 1000, num_rows),
    "col_float": np.random.rand(num_rows),
    "col_str": [f"val_{i}" for i in range(num_rows)],
    "col_bool": np.random.choice([True, False], num_rows),
    "col_date": pd.date_range("2023-01-01", periods=num_rows),
    "col_cat": pd.Categorical(np.random.choice(["A", "B", "C"], num_rows)),
    "col_id": np.arange(num_rows),
    "col_text": ["some text"] * num_rows,
    "col_random": np.random.randn(num_rows),
    "col_zip": np.random.choice([75001, 75002, 75003], num_rows),
    "col_city": np.random.choice(["Paris", "Lyon", "Marseille"], num_rows),
    "col_country": ["France"] * num_rows,
    "col_flag": np.random.choice([0,1], num_rows),
    "col_rating": np.random.uniform(1, 5, num_rows),
    "col_comment": [f"comment {i}" for i in range(num_rows)]
})

# Sauvegarde en Parquet
df.to_parquet("test_data.parquet", engine="pyarrow")
print("Parquet file 'test_data.parquet' generated")
