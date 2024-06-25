___

Kubernetes Deployment Guide

https://github.com/airflow-helm/charts/blob/main/charts/airflow/docs/guides/quickstart.md

```
helm install airflow airflow-stable/airflow --namespace airflow --version "8.7.1" --values ./deployment/values.yaml
```
to uninstall
```
helm uninstall airflow --namespace airflow
```
___

use external DB for easier deployment

https://github.com/airflow-helm/charts/blob/main/charts/airflow/docs/faq/database/external-database.md

___


If you want to use embeded postgres, this might help

https://stackoverflow.com/questions/75758115/persistentvolumeclaim-is-stuck-waiting-for-a-volume-to-be-created-either-by-ex

___

Setting up git sync

https://hungngphhelm.medium.com/airflow-on-kubernetes-with-helm-c795545325dc

Setup user password git (git connection using ssh seems very hard to setup, using http instead)
```
echo -n 'username' | base64
echo -n 'password' | base64
```

save into airflow-git-credential.yaml then
```
kubectl apply -f deployment/airflow-git-credential.yaml
```

finally
```
helm upgrade airflow airflow-stable/airflow --namespace airflow --version "8.7.1" --values ./deployment/values.yaml
```


___

Enable Log to S3

https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/s3-task-handler.html

example usage

https://github.com/airflow-helm/charts/discussions/833#discussioncomment-8617171