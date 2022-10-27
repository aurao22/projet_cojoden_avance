
# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Module to export the cojoden database in CSV files

Project: Cojoden avance
=======

Usage:
======
    python cojoden_dao_export_csv.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"

# %% import
from os.path import join, exists
import sys
import pandas as pd
import mysql.connector
from tqdm import tqdm
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from data_preprocessing.cojoden_functions import convert_df_string_to_search_string
from dao.cojoden_dao import create_engine, TABLES_NAME

# ----------------------------------------------------------------------------------
# %% EXPORT DE LA BDD
# ----------------------------------------------------------------------------------
def export_database(export_path, verbose=0):
    for table_name in TABLES_NAME:
        pt = join(export_path, f'cojoden_{table_name}_export.csv')
        get_table_df(table_name=table_name, file_path=pt, verbose=verbose)

# ----------------------------------------------------------------------------------
# %%     RECUPERATION DES TABLES SOUS FORME DE DF
# ----------------------------------------------------------------------------------
def get_table_df(table_name, file_path=None, verbose=0):
    """Read the table, create a DataFrame with it and write the DF CSV file

    Args:
        table_name (str): table name
        dataset_path (str): the path to write the CSV file for the table
        file_name (str, optional): The CSV file name. Defaults to None => 'cojoden_bdd_{table_name}.csv'
        write_file (bool, optional): To write the CSV file or not. Defaults to True.
        verbose (int, optional): Log level. Defaults to 0.

    Returns:
        DataFrame: The table DataFrame
    """
    short_name = "get_table_df"
    
    dbConnection =create_engine(verbose=verbose)

    df = pd.read_sql(sql=f'SELECT * FROM {table_name}', con=dbConnection)

    if df is not None and file_path is not None:
        df.to_csv(file_path, index=False)
        if verbose>0:
            print(f'[{short_name}] File write ---> {file_path}')
    return df

# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    dataset_path=r'C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance\dataset'
    export_database(export_path=dataset_path, verbose=1)
    

