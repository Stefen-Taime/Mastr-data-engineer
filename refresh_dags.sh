#!/bin/bash

# Find the name of the Airflow worker container
worker_container=$(docker ps --format '{{.Names}}' | grep tp_engineer_airflow-worker)

if [ -n "$worker_container" ]; then
  # Restart the Airflow worker container to refresh the DAGs
  docker restart "$worker_container"
  echo "DAGs have been refreshed by restarting the Airflow worker container."
else
  echo "Airflow worker container not found. Make sure it is running."
fi
