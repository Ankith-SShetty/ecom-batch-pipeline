#!/bin/bash
docker run --rm --network ecom-batch-pipeline_ecom_network -v $(pwd)/spark/jobs:/jobs -e POSTGRES_HOST=postgres -e POSTGRES_USER=ecom_user -e POSTGRES_PASSWORD=ecom_pass -e POSTGRES_DB=ecom_db python:3.11-slim bash -c "pip install pyspark psycopg2-binary -q && python /jobs/batch_job.py"
