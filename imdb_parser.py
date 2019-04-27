from os import path, getcwd
import gzip
import shutil
import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def unpack(file_name: str):
    """ :param file_name is name of tsv file """
    with gzip.open("{}.gz".format(file_name), 'rb') as file_in:
        with open(file_name, 'wb') as file_out:
            shutil.copyfileobj(file_in, file_out)


def tsv_to_df(file_name) -> pd.DataFrame:
    return pd.read_csv(file_name, sep='\t')


TITLE_FILE_NAME = 'title.basics.tsv'

if path.isfile("{}.gz".format(TITLE_FILE_NAME)):
    print(getcwd())
    unpack(TITLE_FILE_NAME)
    print(tsv_to_df(TITLE_FILE_NAME).head())
    engine = create_engine('')
