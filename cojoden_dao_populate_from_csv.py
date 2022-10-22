
# %% import
import pandas as pd
import mysql.connector
from os.path import join, exists
from cojoden_functions import convert_df_string_to_search_string
from cojoden_dao import create_engine
from tqdm import tqdm

# ----------------------------------------------------------------------------------
# %% PEUPLEMENT DE LA BDD
# ----------------------------------------------------------------------------------

def populate_musees(dataset_path, file_name=r'cojoden_musees.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        df = pd.read_csv(file_path)
        if 'nom_search' not in list(df.columns):
            df['nom_search'] = df['nom']
            df['ville_src'] = df['ville']
            df = convert_df_string_to_search_string(df, col_name='nom_search', stop_word_to_remove=['écomusée','ecomusee','écomusé','ecomuse',"Musées", "Musees","Musée","Musee","Muse",  'Museon', 'muséum', "museum"])
            df = df.sort_values('nom_search')
            df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)

        dbConnection = create_engine(verbose=verbose)
        try:
            nb_pop = df[['museo', 'nom', 'nom_search', 'ville', 'latitude', 'longitude']].to_sql(name='musee', con=dbConnection, if_exists='append', index=False, chunksize=10)
        except mysql.connector.IntegrityError as error:
            nb_pop = 0
            if verbose > 0:
                print(f"[cojoden_dao > populate_musees] WARNING : la table est déjà peuplée.\n\t- {error}")
        except Exception as error:
            if  "IntegrityError" in str(error):
                nb_pop = 0
                if verbose > 0:
                    print(f"[cojoden_dao > populate_musees] WARNING : la table est déjà peuplée.\n\t- {error}")
            else:
                raise error
    return nb_pop

def populate_metiers(dataset_path, file_name=r'cojoden_metiers_uniques.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        df = pd.read_csv(file_path)
        if 'search' not in list(df.columns):
            df['search'] = df['metier']
            df = convert_df_string_to_search_string(df, col_name='search')
            df = df[['search', 'metier']]
            df = df.sort_values('search')
            df = df.drop_duplicates('search')
            df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)
        try:
            df = df.rename(columns={"search":"metier_search"})
        except:
            pass
        
        dbConnection =create_engine(verbose=verbose)
        try:
            nb_pop = df.to_sql(name='metier', con=dbConnection, if_exists='append', index=False, chunksize=10)
        except mysql.connector.IntegrityError as error:
            nb_pop = 0
            if verbose > 0:
                print(f"[cojoden_dao > populate_villes] WARNING : la table est déjà peuplée.\n\t- {error}")
        except Exception as error:
            if  "IntegrityError" in str(error):
                nb_pop = 0
                if verbose > 0:
                    print(f"[cojoden_dao > populate_villes] WARNING : la table est déjà peuplée.\n\t- {error}")
            else:
                raise error
    return nb_pop

def populate_villes(dataset_path, file_name=r'cojoden_villes_departement_region_pays.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        df = pd.read_csv(file_path)
        if 'ville_search' not in list(df.columns):
            df['ville_search'] = df['ville']
            df = convert_df_string_to_search_string(df, col_name='ville_search')
            df = df[['id', 'ville_search', 'ville', 'departement', 'region1']]
            df = df.sort_values('ville_search')
            df = df.drop_duplicates('ville_search')
            df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)
        dbConnection =create_engine(verbose=verbose)
        try:
            nb_pop = df[['id', 'ville_search', 'ville', 'departement', 'region1']].to_sql(name='ville', con=dbConnection, if_exists='append', index=False)
        except mysql.connector.IntegrityError as error:
            nb_pop = 0
            if verbose > 0:
                print(f"[cojoden_dao > populate_villes] WARNING : la table est déjà peuplée.\n\t- {error}")
        except Exception as error:
            if  "IntegrityError" in str(error):
                nb_pop = 0
                if verbose > 0:
                    print(f"[cojoden_dao > populate_villes] WARNING : la table est déjà peuplée.\n\t- {error}")
            else:
                raise error
    return nb_pop

def populate_artistes(dataset_path, file_name=r'cojoden_artistes.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        # csv : nom_naissance,nom_dit,nom_search
        # BDD : id, nom_naissance,nom_dit,nom_search, commentaire
        df = pd.read_csv(file_path)
        if 'nom_search' not in list(df.columns):
            df['nom_search'] = df['nom_naissance']
            df = convert_df_string_to_search_string(df, col_name='nom_search')
        df = df[['id','nom_search', 'nom_naissance', 'dit']]
        df = df.sort_values('nom_search')
        df = df.drop_duplicates('nom_search')
        df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)
        dbConnection =create_engine(verbose=verbose)
        try:
            nb_pop = df.to_sql(name='artiste', con=dbConnection, if_exists='append', index=False, chunksize=1)
        except mysql.connector.IntegrityError as error:
            nb_pop = 0
            if verbose > 0:
                print(f"[cojoden_dao > populate_artistes] WARNING : la table est déjà peuplée.\n\t- {error}")
        except Exception as error:
            if  "IntegrityError" in str(error):
                nb_pop = 0
                if verbose > 0:
                    print(f"[cojoden_dao > populate_artistes] WARNING : la table est déjà peuplée.\n\t- {error}")
            else:
                raise error
    return nb_pop

def populate_materiaux(dataset_path, file_name=r'cojoden_materiaux_techniques.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = "populate_materiaux"
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        df = pd.read_csv(file_path)
        if 'mat_search' not in list(df.columns):
            df['mat_search'] = df['materiaux_technique']
            df = convert_df_string_to_search_string(df, col_name='mat_search')
            df = df.sort_values('mat_search')
            df = df.drop_duplicates('mat_search')
            df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)
        dbConnection =create_engine(verbose=verbose)
        try:
            nb_pop = df.to_sql(name='MATERIEAUX_TECHNIQUE'.lower(), con=dbConnection, if_exists='append', index=False, chunksize=10)
        except mysql.connector.IntegrityError as error:
            nb_pop = 0
            if verbose > 0:
                print(f"[cojoden_dao > {short_name}] WARNING : la table est déjà peuplée.\n\t- {error}")
        except Exception as error:
            if  "IntegrityError" in str(error):
                nb_pop = 0
                if verbose > 0:
                    print(f"[cojoden_dao > {short_name}] WARNING : la table est déjà peuplée.\n\t- {error}")
            else:
                raise error
    return nb_pop


def populate_oeuvres(dataset_path, file_name=r'cojoden_oeuvres.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = "populate_oeuvres"
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        df = pd.read_csv(file_path,low_memory=False)
        for col in ["largeur_cm", "hauteur_cm", "profondeur_cm"]:
            df[col] = df[col].astype(float)
        
        for col in ['annee_debut', 'annee_fin']:
            df[col] = df[col].fillna(0)
            try:
                df[col] = df[col].astype(int)
            except:
                pass

        df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)

        dbConnection =create_engine(verbose=verbose)
        try:
            nb_pop = df.to_sql(name='OEUVRE'.lower(), con=dbConnection, if_exists='append', index=False, chunksize=10)
        except mysql.connector.IntegrityError as error:
            nb_pop = 0
            if verbose > 0:
                print(f"[cojoden_dao > {short_name}] WARNING : la table est déjà peuplée.\n\t- {error}")
        except Exception as error:
            if  "IntegrityError" in str(error):
                nb_pop = 0
                if verbose > 0:
                    print(f"[cojoden_dao > {short_name}] WARNING : la table est déjà peuplée.\n\t- {error}")
            else:
                raise error
    return nb_pop

def populate_creation_oeuvres(dataset_path, file_name=r'cojoden_creation_oeuvres.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = _populate_generic(dataset_path=dataset_path, file_name=file_name, table_name='CREER', verbose=verbose)
    return nb_pop

def populate_composer(dataset_path, file_name=r'cojoden_materiaux_techniques_compose.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = _populate_generic(dataset_path=dataset_path, file_name=file_name, table_name='COMPOSER', verbose=verbose)
    return nb_pop

def populate_domaine(dataset_path, file_name=r'cojoden_domaines.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = _populate_generic(dataset_path=dataset_path, file_name=file_name, table_name='DOMAINE', verbose=verbose)
    return nb_pop

def populate_concerner(dataset_path, file_name=r'cojoden_domaines_concerner.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = _populate_generic(dataset_path=dataset_path, file_name=file_name, table_name='CONCERNER', verbose=verbose)
    return nb_pop


def populate_composer(dataset_path, file_name=r'cojoden_materiaux_techniques_compose.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    nb_pop = _populate_generic(dataset_path=dataset_path, file_name=file_name, table_name='COMPOSER', verbose=verbose)
    return nb_pop

_datas_populate_functions = {
    "metiers"   : populate_metiers,
    "villes"    : populate_villes,
    "musees"    : populate_musees,
    "auteurs"   : populate_artistes,
    "oeuvres"   : populate_oeuvres,
    "creer"     : populate_creation_oeuvres,
    "materiaux" : populate_materiaux,
    "composer"  : populate_composer,
    "domaines"  : populate_domaine,
    "concerner" : populate_concerner,
}
_datas_table_names = ["metiers" ,"villes", "musees" , "auteurs", "oeuvres", "creer", "materiaux", "composer", "domaines", "concerner"]

def populate_database(dataset_path=r'dataset', verbose=0):
    """Populate all the database in the order :
    - metiers
    - villes
    - musees
    - artistes
    - oeuvres
    - matériaux
    - domaine
    - creation_oeuvres
    - composer
    - concerner

    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = "populate_database"
    tot = 0
    
    for table_name in tqdm(_datas_table_names, desc=f"[{short_name}]", disable=(verbose<1)):
        function = _datas_populate_functions.get(table_name, None)
        nb = function(dataset_path=dataset_path, verbose=verbose)
        if verbose>0: print(f"[{short_name}]\tINFO : {nb} {table_name} inserted")
        tot += nb

    if verbose>0: print(f"[{short_name}]\tINFO : {tot} datas inserted (all table included)")
    return tot

# ----------------------------------------------------------------------------------
#                        PRIVATE
# ----------------------------------------------------------------------------------

# %% _populate_generic
def _populate_generic(dataset_path, file_name, table_name, verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = f"populate_generic_{table_name}"
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        df = pd.read_csv(file_path)
        
        df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)
        dbConnection =create_engine(verbose=verbose)
        try:
            nb_pop = df.to_sql(name=table_name.lower(), con=dbConnection, if_exists='append', index=False, chunksize=10)
        except mysql.connector.IntegrityError as error:
            nb_pop = 0
            if verbose > 0:
                print(f"[cojoden_dao > {short_name}] WARNING : la table {table_name} est déjà peuplée.\n\t- {error}")
        except Exception as error:
            if  "IntegrityError" in str(error):
                nb_pop = 0
                if verbose > 0:
                    print(f"[cojoden_dao > {short_name}] WARNING : la table {table_name} est déjà peuplée.\n\t- {error}")
            else:
                raise error
    return nb_pop

# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------
_datas_populate_expected = {
    "metiers"   :    229,
    "villes"    :    410,
    "musees"    :    482,
    "auteurs"   :  46235,
    "oeuvres"   : 605157,
    "creer"     : 639497,
    "materiaux" :   8462,
    "composer"  :1247151,
    "domaines"  :    142,
    "concerner" : 868730,
}

def _test_populate(dataset_path, verbose=1):
    tot = populate_database(dataset_path=dataset_path, verbose=verbose)
    assert tot == sum(_datas_populate_expected.values())

def _test_populate_table(table_name,dataset_path, verbose=1):
    function = _datas_populate_functions.get(table_name, None)
    assert function is not None, f"[FAIL] No function for {table_name}"
    nb = function(dataset_path=dataset_path, verbose=verbose)
    assert nb == _datas_populate_expected.get(table_name, 0), f"[FAIL] {table_name} : {nb} rows inserted where {_datas_populate_expected.get(table_name, 0)} expected"


# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    dataset_path=r'C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance\dataset'
    # populate_metiers(dataset_path=dataset_path, verbose=verbose)
    # populate_villes(dataset_path=dataset_path, verbose=verbose)
    # populate_musees(dataset_path=dataset_path, verbose=verbose)
    # populate_artistes(dataset_path=dataset_path, verbose=verbose)
    # populate_oeuvres(dataset_path=dataset_path, verbose=verbose)
    # populate_creation_oeuvres(dataset_path=dataset_path, verbose=verbose)
    # populate_materiaux(dataset_path=dataset_path, verbose=verbose)
    # populate_composer(dataset_path=dataset_path, verbose=verbose)
    # populate_domaine(dataset_path=dataset_path, verbose=verbose)
    # populate_concerner(dataset_path=dataset_path, verbose=verbose)

    _datas_table_names = ["metiers" ,"villes", "musees" , "auteurs", "oeuvres", "creer", "materiaux", "composer", "domaines", "concerner"]
    for table_name in tqdm(_datas_table_names, desc="TEST populate_table"):
        _test_populate_table(table_name=table_name,dataset_path=dataset_path)
        
    # _test_populate(dataset_path=dataset_path)
    

