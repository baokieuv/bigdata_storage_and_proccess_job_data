# ktr cluster 
kubectl cluster-info

# xac dinh contex
kubectl config get-contexts

kubectl create namespace bigdata

kubectl apply -f cassandra.yaml -n bigdata
kubectl apply -f minio.yaml -n bigdata
kubectl apply -f spark.yaml -n bigdata

kubectl port-forward -n bigdata svc/minio 9001:9001