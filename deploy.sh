#!/bin/bash

# Configuration
PROJECT_ID="ut-dnr-ugs-backend-tools"
SERVICE_NAME="geoparquet-converter"
REGION="us-central1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"


echo "🚀 Deploying GeoParquet Converter to Cloud Run"

# Build and push Docker image
echo "📦 Building with Cloud Build..."
gcloud builds submit --tag ${IMAGE_NAME} .

# Deploy to Cloud Run
echo "🚀 Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} 


# Get service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --project ${PROJECT_ID} --format 'value(status.url)')

echo "✅ Cloud Run deployment complete!"
echo "🔗 Service URL: ${SERVICE_URL}"

