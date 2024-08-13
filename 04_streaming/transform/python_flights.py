#!/usr/bin/env python3

"""
Guardar la tabla en un archivo
"""

from google.cloud import bigquery
import pandas as pd

# pyright: reportAssignmentType = false

SQL = """
SELECT
    *
FROM bigquery-manu-407202.dsongcp.flights
"""

client = bigquery.Client()
df = client.query(SQL).to_dataframe(date_dtype=pd.StringDtype())

CHUNK_SIZE = 300000  # Define el tamaño de cada trozo
num_chunks = len(df) // CHUNK_SIZE + 1  # Calcula el número total de trozos

for i in range(0, len(df), CHUNK_SIZE):
    chunk = df[i:i + CHUNK_SIZE]
    chunk.to_json(
        f"flights/chunks/flights_{str(i // CHUNK_SIZE).zfill(5)}-of-{str(num_chunks).zfill(5)}.jsonl",
        orient="records",
        lines=True,
    )
