
from time import time
import pandas as pd
import requests as req

from sqlalchemy import create_engine


import argparse




def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    filename = params.filename

    response = req.get(url)
    with open(filename, 'wb') as fd:
        for chunk in response.iter_content(chunk_size=100000):
            fd.write(chunk)

    if filename.endswith('.csv'):
        df_iter = pd.read_csv(filename,
                              iterator=True, chunksize=100000)

    elif filename.endswith('.parquet'):
        df = pd.read_parquet(filename, engine='pyarrow',
                             )

        # convert to csv
        csv_filename = filename.replace('parquet', 'csv')
        df.to_csv(csv_filename, index=False)

        df_iter = pd.read_csv(csv_filename,
                              iterator=True, chunksize=100000)
    else:
        raise ValueError(
            "file extension is not provided or not supported, please provide csv, or parquet file.")

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = next(df_iter)

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    for df in df_iter:
        t_start = time()

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Ingest csv data to the database')

    parser.add_argument('--user',  help='user name of the database')
    parser.add_argument('--password', help='password of database user')
    parser.add_argument('--host', help='ip address of the database server')
    parser.add_argument('--port', help='database port')
    parser.add_argument('--db', help='database name')
    parser.add_argument('--table_name', help='table name')
    parser.add_argument('--url', help='url of the file to download')
    parser.add_argument('--filename', help='filename')

    args = parser.parse_args()
    # print(args.accumulate(args.integers))

    main(args)
