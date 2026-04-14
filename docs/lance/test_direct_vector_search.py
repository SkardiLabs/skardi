"""
Test script for the lance-direct-vector-search pipeline.

Reads a real vector from the vec_data.lance dataset and sends it
as the query vector to the API endpoint.

Usage:
    1. Start the server:
       cargo run --bin skardi-server -- \
         --ctx docs/lance/ctx_lance.yaml \
         --pipeline docs/lance/pipelines/ \
         --port 8080

    2. Run this script:
       python docs/lance/test_direct_vector_search.py
"""

import json
import requests
import lance

DATA_PATH = "data/vec_data.lance"
ENDPOINT = "http://localhost:8080/lance-direct-vector-search/execute"


def main():
    # Read a vector from the dataset to use as query
    ds = lance.dataset(DATA_PATH)
    table = ds.to_table(columns=["id", "vector"], limit=1)
    row_id = table.column("id")[0].as_py()
    query_vector = table.column("vector")[0].as_py()

    print(f"Using vector from row id={row_id} (dim={len(query_vector)})")
    print(f"First 5 values: {query_vector[:5]}")

    payload = {
        "query_vector": [float(v) for v in query_vector],
    }

    print(f"\nPOST {ENDPOINT}")
    resp = requests.post(ENDPOINT, json=payload)
    resp.raise_for_status()

    result = resp.json()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
