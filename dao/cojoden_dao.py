
# %% import
from tabnanny import verbose
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
if "dao" not in execution_path:
    execution_path = join(execution_path, "dao")

# recupere les données du dotenv
DENV = dotenv_values(join(execution_path,"local_mysql.env"))
db_user = DENV['db_user']
db_pwd = DENV['db_pwd']
db_host = DENV['db_host']
db_name = DENV['db_name']
db_client="mysql"

# Les tables sont dans l'ordre de création
table_names = ["metier","ville", "domaine","artiste",  "musee", "materieaux_technique", "oeuvre", "composer", "creer", "concerner"]

# ----------------------------------------------------------------------------------
# %%                        DATABASE STATUS
# ----------------------------------------------------------------------------------
def database_missing_tables(verbose=0):
    missing_table = table_names.copy()
    res = executer_sql(sql="SHOW TABLES FROM cojoden_avance;", verbose=verbose)
    
    for row in res:
        table_name = row[0]
        if table_name in missing_table:
            missing_table.remove(table_name)

    return missing_table

def database_exist(verbose=0):
    return len(database_missing_tables(verbose=verbose)) == 0

# ----------------------------------------------------------------------------------
# %%                        DATABASE INITIALISATION
# ----------------------------------------------------------------------------------
def initialize_data_base(reset_if_exist=False, verbose=0):
    """Create the database with the SQL creation script.

    Args:
        script_path (str, optional): the SQL creation script. Defaults to 'dataset/cojoden_avance_creation_script.sql'.

    Returns:
        (connection, cursor): The database connection and the cursor
    """
    short_name = "initialize_data_base"
    if reset_if_exist:
        drop_database(verbose=verbose)
        if verbose > 0:
            print(f"[{short_name}] INFO : Database ----- DROP")
    
    missing_table = database_missing_tables(verbose=verbose)
    for table in missing_table:
        locals()["_create_table_"+table](verbose=verbose)
        globals()["_create_table_"+table](verbose=verbose)
    if verbose > 0:
        print(f"[{short_name}] INFO : Tables {missing_table} ----- CREATED")

def initialize_data_base_old(reset_if_exist=False, verbose=0):
    """Create the database with the SQL creation script.

    Args:
        script_path (str, optional): the SQL creation script. Defaults to 'dataset/cojoden_avance_creation_script.sql'.

    Returns:
        (connection, cursor): The database connection and the cursor
    """
    
    short_name = "initialize_data_base"
    if reset_if_exist:
        reset_database
    if not database_exist():
        connection = None
        cursor = None
        try:
            connection = mysql.connector.connect(
                user=db_user,
                password=db_pwd,
                host=db_host)
            cursor = connection.cursor()
            with open(script_path, 'r') as sql_file:
                lines = sql_file.readlines()
                request = ""
                for line in lines:
                    if not line.startswith("--"):
                        request += line.strip()
                        if request.endswith(";"):
                            try:
                                res = cursor.execute(request)
                                connection.commit()
                            except Exception as msg:
                                print(f"[{short_name}] \tERROR : \n\t- {line} \n\t- {msg}")
                            request = ""
        finally:
            try:
                if verbose > 1:
                    print(f"[{short_name}] DEBUG : Le curseur est fermé")
                cursor.close()
            except Exception:
                pass
            try:
                if verbose > 1:
                    print(f"[{short_name}] DEBUG : La connexion est fermée")
                connection.close()
            except Exception:
                pass
    elif verbose>0:
        print(f"[{short_name}] \tINFO : the database ever exist")

    return connection, cursor

def _create_table_metier(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`metier` (
    `metier_search` VARCHAR(100) NOT NULL,
    `metier` VARCHAR(255) NULL,
    `categorie` VARCHAR(255) NULL,
    PRIMARY KEY (`metier_search`))
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_ville(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`VILLE` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `ville_search` VARCHAR(100) NOT NULL,
    `ville` VARCHAR(100) NOT NULL,
    `departement` VARCHAR(100) NULL,
    `region1` VARCHAR(100) NULL,
    `region2` VARCHAR(100) NULL,
    `pays` VARCHAR(100) NULL,
    PRIMARY KEY (`id`))
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_domaine(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`DOMAINE` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `dom_search` VARCHAR(255) NOT NULL,
    `domaine` VARCHAR(255) NOT NULL,
    PRIMARY KEY (`id`))
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_artiste(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`artiste` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `nom_search` VARCHAR(255) NOT NULL,
    `nom_naissance` VARCHAR(255) NOT NULL,
    `nom_dit` VARCHAR(255) NULL,
    `commentaire` TEXT NULL,
    PRIMARY KEY (`id`))
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_musee(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`MUSEE` (
    `museo` VARCHAR(100) NOT NULL,
    `nom_search` VARCHAR(100) NULL,
    `nom` VARCHAR(100) NULL,
    `latitude` VARCHAR(100) NULL,
    `longitude` VARCHAR(100) NULL,
    `plaquette_url` VARCHAR(1000) NULL,
    `ville` INT NULL,
    PRIMARY KEY (`museo`),
    CONSTRAINT `fk_musee_ville` FOREIGN KEY (`ville`) REFERENCES `cojoden`.`VILLE` (`id`)
    )
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_materiaux_technique(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`materiaux_technique` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `mat_search` VARCHAR(100) NOT NULL,
    `materiaux_technique` VARCHAR(100) NULL,
    PRIMARY KEY (`id`))
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_oeuvre(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`oeuvre` (
    `ref` VARCHAR(100) NOT NULL,
    `titre` VARCHAR(1000) NULL,
    `type` VARCHAR(1000) NULL,
    `domaine` VARCHAR(1000) NULL,
    `texte` TEXT(10000) NULL,
    `annee_debut` VARCHAR(45) NULL,
    `annee_fin` VARCHAR(45) NULL,
    `inscriptions` TEXT NULL,
    `commentaires` TEXT NULL,
    `largeur_cm` INT NULL,
    `hauteur_cm` INT NULL,
    `profondeur_cm` INT NULL,
    `img_url` VARCHAR(1000) NULL,
    `lieux_conservation` VARCHAR(100) NOT NULL,
    `creation_lieux` INT NULL,
    PRIMARY KEY (`ref`),
    CONSTRAINT `fk_oeuvre_musee` FOREIGN KEY (`lieux_conservation`) REFERENCES `cojoden`.`MUSEE` (`museo`),
    CONSTRAINT `fk_oeuvre_ville` FOREIGN KEY (`creation_lieux`) REFERENCES `cojoden`.`VILLE` (`id`)
    )
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_composer(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`composer` (
    `oeuvre` VARCHAR(100) NOT NULL,
    `materiaux` INT NOT NULL,
    `complement` VARCHAR(1000) NULL,
    PRIMARY KEY (`oeuvre`, `materiaux`),
    INDEX `fk_composer_matiere_idx` (`materiaux` ASC) INVISIBLE,
    INDEX `fk_composer_oeuvre_idx` (`oeuvre` ASC) INVISIBLE,
    CONSTRAINT `fk_composer_oeuvre`
        FOREIGN KEY (`oeuvre`)
        REFERENCES `cojoden`.`OEUVRE` (`ref`),
    CONSTRAINT `fk_composer_matiere`
        FOREIGN KEY (`materiaux`)
        REFERENCES `cojoden`.`MATERIEAUX_TECHNIQUE` (`id`)
    )
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_creer(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`creer` (
    `oeuvre` VARCHAR(100) NOT NULL,
    `artiste` INT NOT NULL,
    `role` VARCHAR(100) NOT NULL,
    PRIMARY KEY (`oeuvre`, `artiste`),
    INDEX `fk_creer_artiste_idx` (`artiste` ASC) VISIBLE,
    INDEX `fk_creer_oeuvre_idx` (`oeuvre` ASC) VISIBLE,
    INDEX `fk_creer_metier_idx` (`role` ASC) VISIBLE,
    CONSTRAINT `fk_creer_oeuvre`
        FOREIGN KEY (`oeuvre`)
        REFERENCES `cojoden`.`OEUVRE` (`ref`),
    CONSTRAINT `fk_creer_artiste`
        FOREIGN KEY (`artiste`)
        REFERENCES `cojoden`.`ARTISTE` (`id`),
    CONSTRAINT `fk_creer_metier`
        FOREIGN KEY (`role`)
        REFERENCES `cojoden`.`METIER` (`metier_search`)
        )
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

def _create_table_concerner(self, verbose=False):
    res = executer_sql("""CREATE TABLE IF NOT EXISTS `cojoden`.`concerner` (
    `domaine` INT NOT NULL,
    `oeuvre` VARCHAR(100) NOT NULL,
    PRIMARY KEY (`domaine`, `oeuvre`),
    INDEX `fk_concerner_oeuvre_idx` (`oeuvre` ASC) VISIBLE,
    INDEX `fk_converner_domaine_idx` (`domaine` ASC) INVISIBLE,
    CONSTRAINT `fk_concerner_domaine`
        FOREIGN KEY (`domaine`)
        REFERENCES `cojoden`.`DOMAINE` (`id`),
    CONSTRAINT `fk_concerner_oeuvre`
        FOREIGN KEY (`oeuvre`)
        REFERENCES `cojoden`.`OEUVRE` (`ref`)
    )
    ENGINE = InnoDB;
    """, verbose=verbose)
    return res

# ----------------------------------------------------------------------------------
# %%                        DATABASE RESET
# ----------------------------------------------------------------------------------
def reset_database_old(verbose=0):
    short_name = "reset_database"
    for i in range(len(table_names)-1, 0, -1):
        table_name = table_names[i]
        sql = f"DROP TABLE IF EXISTS {table_name}; "
        executer_sql(sql=sql, verbose=verbose)
        if verbose>0:
            print(f"[{short_name}] \tINFO : {table_name} \t---> remove")
    initialize_data_base()
    if verbose>0:
        print(f"[{short_name}] \tINFO : database initialise")

def reset_database(verbose=0):
    try:
        # Prise en compte du cas où la BDD n'existe pas
        drop_database(verbose=verbose)
    except:
        pass
    initialize_data_base(verbose=verbose)

def drop_database(verbose=0):
    short_name = "drop_database"
    sql = f"drop database if exists cojoden_avance; "
    executer_sql(sql=sql, verbose=verbose)


# ----------------------------------------------------------------------------------
# %%                        DATABASE CONNECTION
# ----------------------------------------------------------------------------------
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

# ----------------------------------------------------------------------------------
# %%                        DATABASE REQUEST
# ----------------------------------------------------------------------------------
def executer_sql(sql, verbose=0):
    short_name = "executer_sql"
    conn = None
    cur = None
    # Séparation des try / except pour différencier les erreurs
    try:
        conn = data_base_connection()
        cur = conn.cursor()
        if verbose > 1:
            print(f"[{short_name}] DEBUG : connexion à la BDD")
        try:
            if verbose > 1 :
                print(f"[{short_name}] DEBUG :", sql, end="")
            cur.execute(sql)
            if "INSERT" in sql or "UPDATE" in sql:
                conn.commit()

            if "INSERT" in sql:
                res = cur.lastrowid
            else:
                res = cur.fetchall()
            if verbose>1:
                print(" =>",res)

        except Exception as error:
            print(f"[{short_name}] ERROR : Erreur exécution SQL", error)
            raise error
    except Exception as error:
        print(f"[{short_name}] ERROR : Erreur de connexion à la BDD", error)
        raise error
    finally:
        try:
            if verbose > 1:
                print(f"[{short_name}] DEBUG : Le curseur est fermé")
            cur.close()
        except Exception:
            pass
        try:
            if verbose > 1:
                print(f"[{short_name}] DEBUG : La connexion est fermée")
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
def _test_database_missing_table(verbose=1):
    res = database_missing_tables()
    assert len(res) == 0

# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    reset_database(verbose=1)

    _test_database_missing_table()

