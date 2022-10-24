
# %% import
import pandas as pd
import mysql.connector
from os.path import join, exists
import sys
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from dao.cojoden_dao import executer_sql
from tqdm import tqdm

# ----------------------------------------------------------------------------------
# %% RECHERCHE EN BDD
# ----------------------------------------------------------------------------------
def search_oeuvre(search_str, search_level=1, increase_level=True, limit=0, verbose=0):
    cols_by_level = {
        1 : ["titre"],
        2 : ["inscriptions", "texte"],
        3 : ["type", "domaine"],
    }

    res = _search_generic(table_name="oeuvre", search_str=search_str, cols_by_level=cols_by_level, search_level=search_level, increase_level=increase_level, limit=limit, verbose=verbose)
    return res

def search_artiste(search_str, search_level=1, increase_level=True, limit=0, verbose=0):
    cols_by_level = {
        1 : ["nom_search"],
        2 : ["nom_naissance"],
        3 : ["dit"],
    }

    res = _search_generic(table_name="artiste", search_str=search_str, cols_by_level=cols_by_level, search_level=search_level, increase_level=increase_level, limit=limit, verbose=verbose)
    return res


def search_ville(search_str, search_level=1, increase_level=True, limit=0, verbose=0):
    cols_by_level = {
        1 : ["nom_search"],
        2 : ["nom_naissance"],
        3 : ["dit"],
    }

    res = _search_generic(table_name="artiste", search_str=search_str, cols_by_level=cols_by_level, search_level=search_level, increase_level=increase_level, limit=limit, verbose=verbose)
    return res

# ----------------------------------------------------------------------------------
#  %%                      PRIVATE FUNCTIONS
# ----------------------------------------------------------------------------------
def _search_generic(table_name, search_str, cols_by_level, search_level=1, increase_level=True, limit=0, verbose=0):
    short_name = f"search_generic_{table_name}"
    sql = f"SELECT * FROM {table_name.lower()} WHERE "

    nb_cond = 0
    for level, cols in cols_by_level.items():
        if level <= search_level:
            for col in cols:
                if nb_cond>0:
                    sql+= f"OR "            
                sql+= f"{col} LIKE '%{search_str}%' "
                nb_cond += 1

    if limit > 0:
        sql += f"LIMIT {limit};"
    else:
        sql += ";"
    
    if verbose>1:
        print(f"[{short_name}]  DEBUG: {sql}")

    res = executer_sql(sql=sql, verbose=verbose)

    if res is None and increase_level and search_level < 3:
        if verbose>0:
            print(f"[{short_name}]  INFO: no result with leve {search_level}, trying next level.")
        res = _search_generic(table_name=table_name, search_str=search_str, cols_by_level=cols_by_level, search_level=search_level+1, increase_level=increase_level, limit=limit, verbose=verbose)
    return res


# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------
def _test_search_artiste(verbose=1):
    
    # SELECT * FROM oeuvre WHERE titre LIKE '%HUGUES%' OR inscriptions LIKE '%HUGUES%' OR texte LIKE '%HUGUES%' OR type LIKE '%HUGUES%' OR domaine LIKE '%HUGUES%' LIMIT 100;
    # SELECT * FROM oeuvre WHERE titre LIKE '%RAOUL%' OR inscriptions LIKE '%RAOUL%' OR texte LIKE '%RAOUL%' OR type LIKE '%RAOUL%' OR domaine LIKE '%RAOUL%' LIMIT 100;
    to_test = {
        ("AURELIE", 3, False, 100):47,
        ("AURELIE", 1, False, 100):15,
        ("AURELIE", 2, False, 100):47,
        ("GOULVEN", 3, False, 100):5,
        ("HUGUES", 3, False, 100):100,  # / 138
        ("HUGUES", 2, False, 100):100,  # / 138
        ("HUGUES", 1, False, 100):53,      
        ("THURIANE", 1, False, 100):0,
        ("THURIANE", 1, True, 100):0,
        ("THURIANE", 2, True, 100):0,
        ("THURIANE", 3, True, 100):0,
        ("RAOUL", 2, False, 0):227,     # / 227
        ("RAOUL", 1, False, 0):58,      # / 58
        ("PEINTURE", 1, False, 0):918,  # / 918
    }

    for (search_str, search_level, increase_level, limit), expected in tqdm(to_test.items(), desc="[TEST search_oeuvre]"):
        res = search_oeuvre(search_str=search_str, search_level=search_level, increase_level=increase_level, limit=limit, verbose=verbose)
        nb = len(res)
        assert nb == expected

def _test_search_oeuvre(verbose=1):
    
    # SELECT * FROM oeuvre WHERE titre LIKE '%HUGUES%' OR inscriptions LIKE '%HUGUES%' OR texte LIKE '%HUGUES%' OR type LIKE '%HUGUES%' OR domaine LIKE '%HUGUES%' LIMIT 100;
    # SELECT * FROM oeuvre WHERE titre LIKE '%RAOUL%' OR inscriptions LIKE '%RAOUL%' OR texte LIKE '%RAOUL%' OR type LIKE '%RAOUL%' OR domaine LIKE '%RAOUL%' LIMIT 100;
    to_test = {
        ("AURELIE", 3, False, 100):47,
        ("AURELIE", 1, False, 100):15,
        ("AURELIE", 2, False, 100):47,
        ("GOULVEN", 3, False, 100):5,
        ("HUGUES", 3, False, 100):100,  # / 138
        ("HUGUES", 2, False, 100):100,  # / 138
        ("HUGUES", 1, False, 100):53,      
        ("THURIANE", 1, False, 100):0,
        ("THURIANE", 1, True, 100):0,
        ("THURIANE", 2, True, 100):0,
        ("THURIANE", 3, True, 100):0,
        ("RAOUL", 2, False, 0):227,     # / 227
        ("RAOUL", 1, False, 0):58,      # / 58
        ("PEINTURE", 1, False, 0):918,  # / 918
    }

    for (search_str, search_level, increase_level, limit), expected in tqdm(to_test.items(), desc="[TEST search_oeuvre]"):
        res = search_oeuvre(search_str=search_str, search_level=search_level, increase_level=increase_level, limit=limit, verbose=verbose)
        nb = len(res)
        assert nb == expected

# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    verbose = 1
    _test_search_oeuvre()

