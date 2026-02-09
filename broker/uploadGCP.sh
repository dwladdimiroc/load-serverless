go build -o broker broker.go
gcloud compute scp --zone "us-east1-c" ./broker load-balancing:~/broker