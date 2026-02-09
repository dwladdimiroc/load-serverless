go build -o server broker.go
gcloud compute scp --zone "us-east1-c" ./broker load-balancing:~/broker