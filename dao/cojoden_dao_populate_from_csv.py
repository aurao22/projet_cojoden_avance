
# %% import
import pandas as pd
import mysql.connector
from os.path import join, exists
import sys
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from data_preprocessing.cojoden_functions import convert_df_string_to_search_string
from dao.cojoden_dao import executer_sql, create_engine, TABLES_NAME
from tqdm import tqdm

# ----------------------------------------------------------------------------------
# %% PEUPLEMENT DE LA BDD
# ----------------------------------------------------------------------------------

def populate_musee(dataset_path, file_name=r'cojoden_musees.csv', verbose=0):
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

        nb_pop = _populate_df_generic(df=df[['museo', 'nom', 'nom_search', 'ville', 'latitude', 'longitude']], table_name='musee', verbose=verbose)
    return nb_pop

def populate_metier(dataset_path, file_name=r'cojoden_metiers_uniques.csv', verbose=0):
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
        
        nb_pop = _populate_df_generic(df=df, table_name='metier', verbose=verbose)
    return nb_pop

def populate_ville(dataset_path, file_name=r'cojoden_villes_departement_region_pays.csv', verbose=0):
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
        
        nb_pop = _populate_df_generic(df=df[['id', 'ville_search', 'ville', 'departement', 'region1']], table_name='ville', verbose=verbose)
        
    return nb_pop

def populate_artiste(dataset_path, file_name=r'cojoden_artistes.csv', verbose=0):
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
        
        nb_pop = _populate_df_generic(df=df, table_name='artiste'.lower(), verbose=verbose)

    return nb_pop

def populate_materiaux_technique(dataset_path, file_name=r'cojoden_materiaux_techniques.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = "populate_materiaux_technique"
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
        
        nb_pop = _populate_df_generic(df=df, table_name='materiaux_technique'.lower(), verbose=verbose)
    return nb_pop


def populate_oeuvre(dataset_path, file_name=r'cojoden_oeuvres.csv', verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = "populate_oeuvre"
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

        nb_pop = _populate_df_generic(df=df, table_name='OEUVRE'.lower(), verbose=verbose)

    return nb_pop

def populate_creer(dataset_path, file_name=r'cojoden_creation_oeuvres.csv', verbose=0):
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



def populate_database(dataset_path=r'dataset', verbose=0):
    """Populate all the database in the order :
    - dao.TABLES_NAME

    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = "populate_database"
    tot = 0
    
    for table_name in tqdm(TABLES_NAME, desc=f"[{short_name}]", disable=(verbose<1)):
        nb = globals()["populate_"+table_name](dataset_path=dataset_path, verbose=verbose)
        if verbose>0: print(f"[{short_name}] \tINFO : {nb} {table_name} inserted")
        tot += nb

    if verbose>0: print(f"[{short_name}] \tINFO : {tot} datas inserted (all table included)")
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
    nb_pop = -1
    file_path = join(dataset_path, file_name)

    if exists(file_path):
        df = pd.read_csv(file_path)
        
        df.to_csv(join(dataset_path,file_name.replace(".csv", "-v2.csv")), index=False)
        nb_pop = _populate_df_generic(df=df, table_name=table_name, verbose=verbose)
    return nb_pop

def _populate_df_generic(df, table_name, verbose=0):
    """Populate the table with the precise CSV file
    
    Args:
        dataset_path (str, optional): the dataset path, path where all CSV files are store. Defaults to r'dataset'.
        file_name (str, optional):the CSV file name
        verbose (int, optional): Log level. Defaults to 0.
    """
    short_name = f"_populate_df_generic{table_name}"
    nb_pop = -1  
        
    dbConnection =create_engine(verbose=verbose)
    try:
        nb_pop = df.to_sql(name=table_name.lower(), con=dbConnection, if_exists='replace', index=False, chunksize=10)
    except mysql.connector.IntegrityError as error:
        nb_pop = 0
        if verbose > 0:
            print(f"[{short_name}] WARNING : la table {table_name} est déjà peuplée.\n\t- {error}")
    except Exception as error:
        if  "IntegrityError" in str(error):
            nb_pop = 0
            if verbose > 0:
                print(f"[{short_name}] WARNING : la table {table_name} est déjà peuplée.\n\t- {error}")
        else:
            raise error
    return nb_pop

# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------
# /!\ le nombre de lignes affichées dans le plugin VScode est faux, il est préférable de vérifier via les requêtes SQL.
_datas_populate_expected = {
    "metier"   :    229,    # OK
    "ville"    :    410,    # OK
    "domaine"  :    142,    # OK
    "musee"    :    482,    # OK
    "materiaux_technique" :   8462, # OK
    "artiste"   :  46235,   # 2 324 are missing in BDD => L'affichage dans VS code est faux, il y a bien 46235 en BDD
    "oeuvre"   : 605157,    # 16 510 de plus  in BDD ??? => 605157 en BDD
    "composer"  :1247151,   # 3 063 en moins in BDD => 1247151
    "creer"     : 639497,   # 2 062 en moins in BDD => 639497
    "concerner" : 868730,   # 625 828 en moins in DSS => 868730
}

def _test_populate(dataset_path, verbose=1):
    tot = populate_database(dataset_path=dataset_path, verbose=verbose)
    assert tot == sum(_datas_populate_expected.values())
    _test_check_nb_data(verbose=verbose)


def _test_check_nb_data(verbose=1):
    # Vérification du nombre de données
    for table_name in tqdm(TABLES_NAME, desc=f'[check_nb_data]'):
        sql = f'SELECT COUNT(*) FROM {table_name};'
        res = executer_sql(sql=sql, verbose=verbose)
        assert res is not None and len(res)>0 and len(res[0])>0
        assert res[0][0] == _datas_populate_expected.get(table_name, 0)


# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    dataset_path=r'C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance\dataset'
    # populate_metier(dataset_path=dataset_path, verbose=verbose)
    # populate_ville(dataset_path=dataset_path, verbose=verbose)
    # populate_musee(dataset_path=dataset_path, verbose=verbose)
    # populate_artiste(dataset_path=dataset_path, verbose=verbose)
    # populate_oeuvre(dataset_path=dataset_path, verbose=verbose)
    # populate_creer(dataset_path=dataset_path, verbose=verbose)
    # populate_materiaux_technique(dataset_path=dataset_path, verbose=verbose)
    # populate_composer(dataset_path=dataset_path, verbose=verbose)
    # populate_domaine(dataset_path=dataset_path, verbose=verbose)
    # populate_concerner(dataset_path=dataset_path, verbose=verbose)
    _test_populate(dataset_path=dataset_path, verbose=1)
    _test_check_nb_data(verbose=1)
    

