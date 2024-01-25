FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

COPY pipeline_2.py pipeline_2.py

ENTRYPOINT [ "python", "pipeline_2.py"]