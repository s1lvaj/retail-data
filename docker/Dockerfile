FROM bitnami/spark:3.5

WORKDIR /app

COPY . .

RUN pip install pyspark

CMD ["python", "run_pipeline.py"]

# Then, run this:
# docker build -t olist-pipeline .
# docker run olist-pipeline