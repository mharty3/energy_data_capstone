FROM python:3-slim

WORKDIR /mlflow/
RUN apt-get update && pip install -U pip
RUN apt-get install postgresql-client -y
COPY requirements.txt .
RUN pip install -r requirements.txt

EXPOSE 5000

CMD mlflow server --backend-store-uri postgresql://michael:admin@${DB_PRIVATE_IP}:5432/${DB_NAME} \
	          --default-artifact-root gs://${BUCKET_NAME}/mlruns \
                  --host 0.0.0.0 \
                  --port 5000 
