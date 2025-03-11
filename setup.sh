#!/bin/bash
clear

echo "🚀 Starting Automated ETL Setup!"

gcloud auth revoke --all

# ✅ Fix permissions on the host machine (Linux/macOS only)
echo "🔹 Fixing permissions for airflow/dags/dbt..."
mkdir -p airflow/dags/dbt  # Ensure the folder exists

# Detect OS type
OS_TYPE=$(uname -s)

if [[ "$OS_TYPE" == "Darwin" ]]; then
    # macOS: Use `id -gn` to get the group name
    PRIMARY_GROUP=$(id -gn)
    chown -R $(whoami):$PRIMARY_GROUP airflow/dags/dbt
    chmod -R 775 airflow/dags/dbt

    mkdir -p dbt/logs
    chown -R $(whoami):$PRIMARY_GROUP dbt/logs
    chmod -R 777 dbt/logs

elif [[ "$OS_TYPE" == "Linux" ]]; then
    # Linux: Use `id -g` to get the group ID
    PRIMARY_GROUP=$(id -g)
    chown -R $(whoami):$PRIMARY_GROUP airflow/dags/dbt
    chmod -R 775 airflow/dags/dbt

    mkdir -p dbt/logs
    chown -R $(whoami):$PRIMARY_GROUP dbt/logs
    chmod -R 777 dbt/logs

elif [[ "$OS_TYPE" == "MINGW64_NT"* || "$OS_TYPE" == "CYGWIN_NT"* || "$OS_TYPE" == "MSYS_NT"* ]]; then
    # Windows (Git Bash, WSL, Cygwin, or MSYS)
    echo "🛑 Skipping permission changes on Windows (chown not supported)."
else
    echo "⚠ Unknown OS: $OS_TYPE - Skipping permission changes."
fi

echo "✅ Permissions set for airflow/dags/dbt and dbt/logs (if applicable)."


# Ensure script runs in its directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit

# Create & Write Environment Variables
echo "🔹 Creating .env file..."
rm -f .env
cat > .env <<EOL
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=airflow
AIRFLOW_DB=airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=zoomcamp
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_DB_HOST=airflow_postgres

TFL_API_URL=https://api.tfl.gov.uk/AccidentStats
START_YEAR=2005
END_YEAR=2019

USE_CLOUD_DB=False
DB_HOST=postgres_db_tfl_accident_data
DB_PORT=5432
DB_NAME=tfl_accidents
DB_USER=admin
DB_PASSWORD=admin

GCS_CSV_PATH=processed_data/raw/csv/
DBT_PROFILES_DIR=/usr/app/dbt
DBT_PROJECT_NAME=tfl_accidents_project

LOCAL_STORAGE=/opt/airflow/processed_data/raw/csv
EOL

echo "✅ .env file created successfully."

# Create secrets folder
mkdir -p secrets
rm -f secrets/gcp_credentials.json

# Authenticate with Google Cloud
echo "🔹 Authenticating with Google Cloud..."
gcloud auth login || { echo "❌ GCP login failed! Exiting..."; exit 1; }

# Get current user email
USER_EMAIL=$(gcloud config get-value account)
echo "🔹 You are logged in as: $USER_EMAIL"

# Select or create GCP project
echo "🔹 Fetching available GCP projects..."
gcloud projects list --format="table(projectId, name)"

while true; do
  read -p "Enter an existing GCP Project ID (or press Enter to create a new one): " GCP_PROJECT_ID

  if [ -z "$GCP_PROJECT_ID" ]; then
    read -p "Enter a new GCP Project ID: " GCP_PROJECT_ID
    echo "🔹 Creating new project: $GCP_PROJECT_ID..."
    
    gcloud projects create $GCP_PROJECT_ID --name="$GCP_PROJECT_ID" --set-as-default
    PROJECT_EXISTS=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectId)" 2>/dev/null)
    
    if [ -z "$PROJECT_EXISTS" ]; then
      echo "❌ Failed to create project. Please check permissions."
      exit 1
    fi
    echo "✅ Project created successfully!"
    break
  else
    PROJECT_EXISTS=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectId)" 2>/dev/null)
    if [ -n "$PROJECT_EXISTS" ]; then
      echo "✅ Using existing project: $GCP_PROJECT_ID"
      break
    else
      echo "❌ Project '$GCP_PROJECT_ID' does not exist."
    fi
  fi
done

# Set the project
gcloud config set project $GCP_PROJECT_ID
echo "GCP_PROJECT_ID=$GCP_PROJECT_ID" >> .env

# Check Billing
echo "🔹 Checking if billing is enabled..."
BILLING_STATUS=$(gcloud billing projects describe $GCP_PROJECT_ID --format="value(billingEnabled)")
if [ "$BILLING_STATUS" != "True" ]; then
   echo "❌ Billing is not enabled for this project!"
  echo "To proceed, you must enable billing manually."

  # Show available billing accounts
  echo "🔹 Available billing accounts:"
  gcloud billing accounts list

  echo "🔹 Follow these steps to enable billing:"
  echo "   1️⃣ Go to the Billing Console: https://console.cloud.google.com/billing"
  echo "   2️⃣ Select or link a billing account to your project: $GCP_PROJECT_ID"
  echo "   3️⃣ Once billing is activated, re-run this script."

  exit 1
fi

echo "✅ Billing is enabled."

# Create Service Account
SERVICE_ACCOUNT_NAME="gcs-access-sa"
SERVICE_ACCOUNT_EMAIL="$SERVICE_ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"
KEY_FILE_PATH="secrets/gcp_credentials.json"

EXISTING_SA=$(gcloud iam service-accounts list --format="value(email)" --filter="email:$SERVICE_ACCOUNT_EMAIL")

if [ -z "$EXISTING_SA" ]; then
    echo "🔹 Creating service account..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --display-name "Service Account for GCS Access"

    echo "🔹 Assigning IAM roles..."
    for role in "roles/storage.admin" "roles/storage.objectAdmin" "roles/storage.objectViewer"; do
        gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
            --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="$role"
    done
fi

# Generate service account key
echo "🔹 Generating service account key..."
if ! gcloud iam service-accounts keys create $KEY_FILE_PATH --iam-account=$SERVICE_ACCOUNT_EMAIL; then
    echo "❌ Failed to create service account key. Precondition check failed."
    echo "Please delete existing keys using the following command:"
    echo "gcloud iam service-accounts keys list --iam-account=$SERVICE_ACCOUNT_EMAIL"
    echo "gcloud iam service-accounts keys delete KEY_ID --iam-account=$SERVICE_ACCOUNT_EMAIL -q"
    exit 1
fi
chmod 644 $KEY_FILE_PATH


# Validate key file
if [ ! -f "$KEY_FILE_PATH" ]; then
    echo "❌ Failed to create service account key."
    exit 1
fi

# Try to activate service account (Detect JWT Error)
echo "🔹 Activating service account..."
if ! gcloud auth activate-service-account --key-file=$KEY_FILE_PATH 2>&1 | tee activation_log.txt | grep -q "invalid_grant"; then
    echo "✅ Service account activated successfully."
else
    echo "❌ Invalid JWT Signature detected! Recreating key..."
    
    # Delete old key and regenerate
    gcloud iam service-accounts keys delete $(jq -r .private_key_id < $KEY_FILE_PATH) --iam-account=$SERVICE_ACCOUNT_EMAIL -q
    rm -f $KEY_FILE_PATH
    
    gcloud iam service-accounts keys create $KEY_FILE_PATH --iam-account=$SERVICE_ACCOUNT_EMAIL
    chmod 644 $KEY_FILE_PATH

    # Retry activation
    if ! gcloud auth activate-service-account --key-file=$KEY_FILE_PATH; then
        echo "❌ Service account activation failed again. Please check permissions manually."
        exit 1
    fi
fi

echo "✅ GCP authentication complete."

# Store service account credentials path in .env
echo "GOOGLE_APPLICATION_CREDENTIALS=$KEY_FILE_PATH" >> .env
echo "SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT_EMAIL" >> .env

# ✅ Load and Export Variables
export $(grep -v '^#' .env | xargs)

# Ensure required variables are set
if [ -z "$SERVICE_ACCOUNT_EMAIL" ] || [ -z "$GCP_PROJECT_ID" ]; then
    echo "❌ Required variables are missing. Exiting..."
    exit 1
fi

# 7️⃣ Deploy Infrastructure with Terraform
echo "🔹 Deploying infrastructure with Terraform..."
cd terraform
terraform init
terraform apply -auto-approve \
  -var="project_id=$GCP_PROJECT_ID" \
  -var="region=us-central1" \
  -var="bucket_name=${GCP_PROJECT_ID}-datalake" \
  -var="service_account_email=$SERVICE_ACCOUNT_EMAIL"

if [ $? -ne 0 ]; then
    echo "❌ Terraform deployment failed! Exiting..."
    exit 1
fi

echo "✅ Terraform deployment complete."

# ✅ Fetch Bucket Name from Terraform Output
GCS_BUCKET_NAME=$(terraform output -raw bucket_name)

# ✅ Ensure `GCS_BUCKET` is added to `.env`
if grep -q "^GCS_BUCKET=" ../.env; then
    sed -i "s|^GCS_BUCKET=.*|GCS_BUCKET=$GCS_BUCKET_NAME|" ../.env
else
    echo "GCS_BUCKET=$GCS_BUCKET_NAME" >> ../.env
fi

echo "✅ GCS_BUCKET added to .env: $GCS_BUCKET_NAME"

cd ..

# 8️⃣ Start Docker & Airflow
echo "🔹 Starting Docker services... THIS WILL TAKE SOMETIME!"
docker-compose build --no-cache
docker-compose up -d


# 9️⃣ Wait for Airflow
echo "⏳ Waiting for Airflow to initialize..."
sleep 30  

# 🔟 Trigger DAG
echo "🔹 Unpausing and triggering Airflow DAG..."
docker exec -it airflow_webserver-tfl airflow dags unpause end_to_end_pipeline
docker exec -it airflow_webserver-tfl airflow dags trigger end_to_end_pipeline

# ✅ Display URLs
echo "✅ Setup Complete!"
echo "📊 Visit Airflow: http://localhost:8082"
echo "🔑 Use credentials:"
echo "   Username: $AIRFLOW_ADMIN_USER"
echo "   Password: $AIRFLOW_ADMIN_PASSWORD"

echo " Visit dashboard: http://localhost:8501"