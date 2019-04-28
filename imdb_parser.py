import concurrent.futures
from os import path, getcwd
import gzip
import shutil
import pandas as pd
from sqlalchemy import create_engine
import logging


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
        name = tsv_to_df(NAME_FN)

        engine = create_engine('postgresql://test:test@localhost:5432/nozbe')
        dfTitleDescription = dfTitle[['tconst', 'titleType', 'primaryTitle', 'originalTitle']]
        dfTitleDetails = dfTitle[['tconst', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']]
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                executor.submit(dfTitleDescription.to_sql, "titleDescription", engine, index=False)
                executor.submit(dfTitleDetails.to_sql, "titleDetails", engine, index=False)
                executor.submit(name.to_sql, "name", engine, index=False)
        except Exception as ex:
            logging.error("Can't insert data to db")
    else:
        logging.error("Files don't exist")
