#!/bin/bash

# Configuration
PROJECT_ID="your-project-id"
SERVICE_NAME="geoparquet-converter"
REGION="us-central1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
OUTPUT_BUCKET="stagedparquet"
SOURCE_BUCKET="your-source-bucket"
TRIGGER_NAME="geoparquet-storage-trigger"

echo "🚀 Deploying GeoParquet Converter to Cloud Run with Eventarc..."

# Enable required APIs
echo "🔧 Enabling required APIs..."
gcloud services enable run.googleapis.com \
  cloudbuild.googleapis.com \
  eventarc.googleapis.com \
  storage.googleapis.com \
  --project ${PROJECT_ID}

# Build and push Docker image
echo "📦 Building Docker image..."
docker build -t ${IMAGE_NAME} .

echo "⬆️  Pushing to Google Container Registry..."
docker push ${IMAGE_NAME}

# Deploy to Cloud Run
echo "🚀 Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --no-allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 10 \
  --min-instances 0 \
  --concurrency 1 \
  --set-env-vars OUTPUT_BUCKET=${OUTPUT_BUCKET} \
  --project ${PROJECT_ID}

# Get service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --project ${PROJECT_ID} --format 'value(status.url)')

echo "✅ Cloud Run deployment complete!"
echo "🔗 Service URL: ${SERVICE_URL}"

# Create Eventarc trigger for Cloud Storage events
echo "📡 Setting up Eventarc trigger for Cloud Storage..."

# Get the Cloud Run service account
SERVICE_ACCOUNT=$(gcloud run services describe ${SERVICE_NAME} \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --format='value(spec.template.spec.serviceAccountName)')

# If no custom service account, get the default one
if [ -z "$SERVICE_ACCOUNT" ]; then
  SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"
fi

echo "🔑 Using service account: ${SERVICE_ACCOUNT}"

# Grant necessary permissions to the service account
echo "🔐 Granting permissions..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/eventarc.eventReceiver"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectCreator"

# Create Eventarc trigger
echo "⚡ Creating Eventarc trigger..."
gcloud eventarc triggers create ${TRIGGER_NAME} \
  --destination-run-service=${SERVICE_NAME} \
  --destination-run-region=${REGION} \
  --destination-run-path="/convert" \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=${SOURCE_BUCKET}" \
  --service-account=${SERVICE_ACCOUNT} \
  --location=${REGION} \
  --project=${PROJECT_ID}

echo "🎯 Setup complete!"
echo ""
echo "📋 Configuration:"
echo "   - Service: ${SERVICE_NAME}"
echo "   - URL: ${SERVICE_URL}"
echo "   - Source bucket: ${SOURCE_BUCKET}"
echo "   - Output bucket: ${OUTPUT_BUCKET}"
echo "   - Eventarc trigger: ${TRIGGER_NAME}"
echo "   - Service account: ${SERVICE_ACCOUNT}"
echo ""
echo "🧪 Test by uploading a zip file:"
echo "gsutil cp your-file.zip gs://${SOURCE_BUCKET}/"
echo ""
echo "📊 Monitor with:"
echo "gcloud run logs read ${SERVICE_NAME} --region=${REGION}"
echo "gcloud eventarc triggers describe ${TRIGGER_NAME} --location=${REGION}"