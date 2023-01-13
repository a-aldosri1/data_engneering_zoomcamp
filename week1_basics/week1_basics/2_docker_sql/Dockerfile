FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 requests pyarrow

WORKDIR /app

COPY import_dataset.py import_dataset.py

ENTRYPOINT [ "python", "import_dataset.py" ]