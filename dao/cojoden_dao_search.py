
# %% import
import pandas as pd
import sys
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from dao.cojoden_dao import executer_sql
from tqdm import tqdm

# ----------------------------------------------------------------------------------
# %% RECHERCHE EN BDD
# ----------------------------------------------------------------------------------

LEVEL_COLS = {  
                'ville': {
                            1 : ["ville_search"],
                            2 : ["ville"],
                            3 : ["departement", "region1"],
                        },
                'oeuvre': {
                            1 : ["titre"],
                            2 : ["inscriptions", "texte"],
                            3 : ["type", "domaine"],
                        },
                'metier':{
                            1 : ["metier_search"],
                            2 : ["metier"],
                },
                'materiaux_technique':{
                            1 : ["mat_search"],
                            2 : ["materiaux_technique"],
                },
                'domaine':{
                            1 : ["dom_search"],
                            2 : ["domaine"],
                },
                'artiste':{
                            1 : ["nom_search"],
                            2 : ["nom_naissance"],
                            3 : ["dit"],
                        },
    }


def search_multicriteria(ville=None, oeuvre=None, musee=None, metier=None, materiaux=None, domaine=None, artiste=None, type_oeuvre=None, search_strategie='AND', search_level=1, verbose=0):
    short_name = "search_multicriteria"
    end_sql = ""
    nb_cond = 0

    join_table = set()

    params = {'ville':ville, 'oeuvre':oeuvre, 'musee':musee, 'metier':metier, 'materiaux_technique':materiaux, 'domaine':domaine, 'artiste':artiste}

    for table_name, value in params.items():
        if value is not None and len(value)>0:
            (sub_sql, nb_cond2) = _create_cond(table_name=table_name, value=ville, nb_cond=nb_cond, search_strategie=search_strategie, search_level=search_level, verbose=verbose)
            if len(sub_sql)>0:
                end_sql += sub_sql
                join_table.add(table_name)
                nb_cond = nb_cond2
                if verbose>1:
                    print(f'[{short_name}] \tDEBUG : {table_name} : condition {value} -- ADDED')
            elif verbose>0:
                print(f'[{short_name}] \tWARN : {table_name} : condition {value} -- NOT ADDED')
        elif verbose>1:
            print(f'[{short_name}] \DEBUG : {table_name} : no condition value')
    
    if type_oeuvre is not None and len(type_oeuvre)>0:
        sql += f"type LIKE '{type_oeuvre}' "
        if nb_cond>0:
            sql+= f"{search_strategie} "        
        join_table.add('oeuvre')
        nb_cond += 1
    


#  %% search_on_table
def search_on_table(table_name, search_str, search_level=1, increase_level=True, limit=0, verbose=0):
    short_name = f"search_on_table_{table_name}"
    sql = f"SELECT * FROM {table_name.lower()} WHERE "

    nb_cond = 0
    for level, cols in LEVEL_COLS.get(table_name, {}).items():
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
        res = search_on_table(table_name=table_name, search_str=search_str, cols_by_level=cols_by_level, search_level=search_level+1, increase_level=increase_level, limit=limit, verbose=verbose)
    return res


# ----------------------------------------------------------------------------------
#  %%                      PRIVATE FUNCTIONS
# ----------------------------------------------------------------------------------
def _create_cond(table_name, value, nb_cond=0, search_strategie='AND', search_level=1, verbose=0):
    sql = ""
    if value is not None and len(value)>0:
        for level, cols in LEVEL_COLS.get(table_name, {}).items():
            if level <= search_level:
                for col in cols:
                    if nb_cond>0:
                        sql+= f"{search_strategie} "            
                    sql+= f"{col} LIKE '%{value}%' "
                    nb_cond += 1
    return (sql, nb_cond)

# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------
def _test_search_artiste(verbose=1):
    
    # SELECT * FROM artiste WHERE nom_search LIKE '%HUGUES%' OR nom_naissance LIKE '%HUGUES%' OR dit LIKE '%HUGUES%' LIMIT 100;
    # SELECT * FROM artiste WHERE nom_search LIKE '%RAOUL%' OR nom_naissance LIKE '%RAOUL%' OR dit LIKE '%RAOUL%' LIMIT 100;
    to_test = {
        ("HUGUES", 3, False, 100):32,  
        ("HUGUES", 2, False, 100):32,  
        ("HUGUES", 1, False, 100):32,      
        ("AURELIE", 1, False, 100):7,
        ("GOULVEN", 3, False, 100):0,
        ("THURIANE", 3, True, 100):0,
        ("RAOUL", 1, False, 0):46,
    }

    for (search_str, search_level, increase_level, limit), expected in tqdm(to_test.items(), desc="[TEST search_artiste]"):
        res = search_on_table(table_name='artiste', search_str=search_str, search_level=search_level, increase_level=increase_level, limit=limit, verbose=verbose)
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
        res = search_on_table(table_name='oeuvre', search_str=search_str, search_level=search_level, increase_level=increase_level, limit=limit, verbose=verbose)
        nb = len(res)
        assert nb == expected

# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    verbose = 1
    _test_search_oeuvre()
    _test_search_artiste()

