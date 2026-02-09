go build -o server server.go
gcloud compute scp --zone "us-east1-c" ./server server:~/server