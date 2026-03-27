Due to a bug with the airflow deployment, the ingressClassName field of the 
airflow-ingress handling the webserver needs to be manually set via the CLI.
After a terraform deploy do the following:
1: kubectl edit ingress  airflow-ingress -n opera-dev
2: Insert "ingressClassName: nginx" Directly below "spec:"
