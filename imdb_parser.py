import concurrent.futures
import csv
from os import path, getcwd
import gzip
import shutil
import pandas as pd
from sqlalchemy import create_engine
import logging
from config import Config


def unpack(file_name: str):
    """ :param file_name is name of tsv file """
    with gzip.open("{}.gz".format(file_name), 'rb') as file_in:
        with open(file_name, 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)


def tsv_to_df(file_name) -> pd.DataFrame:
    return pd.read_csv(file_name, sep='\t')


if __name__ == "__main__":

    TITLE_FN = 'title.basics.tsv'
    NAME_FN = 'name.basics.tsv'

    if path.isfile("{}.gz".format(TITLE_FN)) and path.isfile("{}.gz".format(NAME_FN)):
        try:
            unpack(TITLE_FN)
            unpack(NAME_FN)
        except Exception as ex:
            logging.error("Can't unpack files")
        dfTitle = tsv_to_df(TITLE_FN)

        nameTitle = []
        name = tsv_to_df(NAME_FN)
        for index, row in name.iterrows():
            for it in row.knownForTitles.split(","):
                nameTitle.append({"nconst": row['nconst'], "tconst": it})
        nameTitle = pd.DataFrame(nameTitle)

        engine = create_engine(Config.DATABASE_URI)

        try:
            name.to_sql("name", engine, index=False)
            nameTitle.to_sql("nameTitle", engine, index=True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                executor.submit(dfTitle.to_sql, "title", engine)
                executor.submit(name.to_sql, "name", engine)
            with engine.connect() as con:
                con.execute('ALTER TABLE "title" ADD PRIMARY KEY (tconst);')
            with engine.connect() as con:
                con.execute('ALTER TABLE "name" ADD PRIMARY KEY (nconst);')
            with engine.connect() as con:
                con.execute(
                    'ALTER TABLE "nameTitle" ADD CONSTRAINT fk_nameTitle_name FOREIGN KEY (nconst) REFERENCES "name"(nconst);')
        except Exception as ex:
            logging.error("Can't insert data to db")
    else:
        logging.error("Files don't exist")
