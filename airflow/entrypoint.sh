#!/bin/bash
echo "🚀 Initializing Airflow..."

# Source environment variables
source /path/to/.env

# Wait for PostgreSQL to be ready
until pg_isready -h airflow_postgres -p 5432 -U $AIRFLOW_POSTGRES_USER; do
  sleep 5
done

echo "✅ PostgreSQL is ready."

# Initialize Airflow database
airflow db init

# Check if the admin user already exists
if airflow users list | grep -q "admin"; then
  echo "✅ Admin user already exists."
else
  # Create an admin user using environment variables
  airflow users create \
      --username "$AIRFLOW_ADMIN_USER" \
      --password "$AIRFLOW_ADMIN_PASSWORD" \
      --firstname admin \
      --lastname admin \
      --role Admin \
      --email "$AIRFLOW_ADMIN_EMAIL"
fi

# Start the correct Airflow service
if [[ "$1" == "webserver" ]]; then
    echo "🌐 Starting Airflow Webserver..."
    exec airflow webserver
elif [[ "$1" == "scheduler" ]]; then
    echo "📅 Starting Airflow Scheduler..."
    exec airflow scheduler
else
    exec "$@"
fi