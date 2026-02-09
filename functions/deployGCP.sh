gcloud functions deploy avg4 \
  --gen2 \
  --region=us-east1 \
  --runtime=go125 \
  --source=. \
  --entry-point=Average \
  --trigger-http \
  --allow-unauthenticated