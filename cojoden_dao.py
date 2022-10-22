
# %% import
import mysql.connector
import sqlalchemy as sa
import pandas as pd
from mysql.connector import errorcode
from os import getcwd
from os.path import join
from dotenv import dotenv_values
from tqdm import tqdm

# ----------------------------------------------------------------------------------
# %%                        DATABASE INFORMATIONS
# ----------------------------------------------------------------------------------

# Récupère le répertoire du programme
execution_path = getcwd() + "\\"
if "PROJETS" not in execution_path:
    execution_path = join(execution_path, "PROJETS")
if "projet_joconde" not in execution_path:
    execution_path = join(execution_path, "projet_cojoden_avance")

# recupere les données du dotenv
DENV = dotenv_values(join(execution_path,"local_mysql.env"))
db_user = DENV['db_user']
db_pwd = DENV['db_pwd']
db_host = DENV['db_host']
db_name = DENV['db_name']
db_client="mysql"

# ----------------------------------------------------------------------------------
# %%                        DATABASE INITIALISATION
# ----------------------------------------------------------------------------------
def initialize_data_base(script_path=r'dataset/cojoden_avance_creation_script.sql'):
    """Create the database with the SQL creation script.

    Args:
        script_path (str, optional): the SQL creation script. Defaults to 'dataset/cojoden_avance_creation_script.sql'.

    Returns:
        (connection, cursor): The database connection and the cursor
    """
    
    short_name = "initialize_data_base"
    connection = mysql.connector.connect(
        user=db_user,
        password=db_pwd,
        host=db_host)
    cursor = connection.cursor()

    with open(script_path, 'r') as sql_file:

        for line in sql_file.split(";"):
            try:
                cursor.execute(line)
            except Exception as msg:
                print(f"[{short_name}] \tERROR : \n\t- {line} \n\t- {msg}")

    return connection, cursor

# %% data_base_connection
def data_base_connection():
    """Create the database connection and return it.

    Returns:
        connection
    """
    short_name = "data_base_connection"
    connection = None
    try:
        connection = mysql.connector.connect(
            user=db_user,
            password=db_pwd,
            host=db_host,
            database=db_name)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print(f"[{short_name}] \tERROR : Something is wrong with your user name or password : {err}")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"[{short_name}] \tERROR : Database does not exist : {err}")
        else:
            print(f"[{short_name}] \tERROR : {err}")

    return connection 

# %% executer_sql
def executer_sql(sql, verbose=0):
    conn = None
    cur = None
    # Séparation des try / except pour différencier les erreurs
    try:
        conn = data_base_connection()
        cur = conn.cursor()
        if verbose > 1:
            print("[cojoden_dao > execute] INFO : Base de données crée et correctement.")
        try:
            if verbose > 1 :
                print("[cojoden_dao > execute] INFO :", sql, end="")
            cur.execute(sql)
            conn.commit()
            if "INSERT" in sql:
                res = cur.lastrowid
            else:
                res = cur.fetchall()
            if verbose:
                print(" =>",res)

        except Exception as error:
            print("[cojoden_dao > execute] ERROR : Erreur exécution SQL", error)
            raise error
    except Exception as error:
        print("[cojoden_dao > execute] ERROR : Erreur de connexion à la BDD", error)
        raise error
    finally:
        try:
            if verbose > 1:
                print("[cojoden_dao > execute] DEBUG : Le curseur est fermé")
            cur.close()
        except Exception:
            pass
        try:
            if verbose > 1:
                print("[cojoden_dao > execute] DEBUG : La connexion est fermée")
            conn.close()
        except Exception:
            pass       
    return res

# %% create_engine
def create_engine(verbose=0):
    # connect_args={'ssl':{'fake_flag_to_enable_tls': True}, 'port': 3306}
    connection_url = _create_sql_url(verbose=verbose)
    db_connection = sa.create_engine(connection_url, pool_recycle=3600) # ,connect_args= connect_args)
    return db_connection
    
# ----------------------------------------------------------------------------------
# %%     RECUPERATION DES TABLES SOUS FORME DE DF
# ----------------------------------------------------------------------------------
def get_table_df(table_name, dataset_path, file_name=None, write_file=True, verbose=0):
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

    if df is not None and write_file:
        if file_name is None:
            file_name=f'cojoden_bdd_{table_name}.csv'
        file_path = join(dataset_path, file_name)
        df.to_csv(file_path, index=False)
        if verbose>0:
            print(f'[{short_name}] File write ---> {file_path}')
    return df
# ----------------------------------------------------------------------------------
#                        PRIVATE
# ----------------------------------------------------------------------------------
# %% _create_sql_url
def _create_sql_url(verbose=0):
    connection_url = sa.engine.URL.create(
        drivername=db_client,
        username=db_user,
        password=db_pwd,
        host=db_host,
        database=db_name
    )
    if verbose > 1:
        print(f"[cojoden_dao > sql URL] DEBUG : {connection_url}")
    return connection_url



# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    pass

