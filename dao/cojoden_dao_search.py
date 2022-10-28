
# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Module to search specifics datas

Project: Cojoden avance
=======

Usage:
======
    python cojoden_dao_search.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"

# %% import
import pandas as pd
import sys
import re

sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from dao.cojoden_dao import executer_sql
from data_preprocessing.cojoden_functions import convert_string_to_search_string
from tqdm import tqdm

# ----------------------------------------------------------------------------------
# %% CONSTANTS
# ----------------------------------------------------------------------------------

LEVEL_COLS = {  
                'ville': {
                            1 : ["ville_search"],
                            2 : ["departement", "region1"],
                        },
                'oeuvre': {
                            1 : ["titre"],
                            2 : ["inscriptions", "texte"],
                            3 : ["type"],
                        },
                'metier':{
                            1 : ["metier_search"],
                },
                'materiaux_technique':{
                            1 : ["mat_search"],
                },
                'domaine':{
                            1 : ["dom_search"],
                },
                'artiste':{
                            1 : ["nom_search"],
                            2 : ["dit"],
                        },
    }

SEARCH_RESULT_COLS_BY_TABLE = {  
        'ville'     :'ville.id as ville_id, ville.ville, ville.departement, ville.region1',
        'oeuvre'    :'oeuvre.ref, oeuvre.titre, oeuvre.type, oeuvre.annee_debut, oeuvre.annee_fin, oeuvre.lieux_conservation, oeuvre.inscriptions, oeuvre.texte',
        'metier'    :'metier.metier',
        'materiaux_technique':'materiaux_technique.materiaux_technique',
        'domaine'   :'domaine.domaine',
        'artiste'   :'artiste.nom_naissance, artiste.dit',
        'musee'     :"musee.museo, musee.nom, musee.ville as ville_musee, musee.latitude, musee.longitude"
    }


SEARCH_RESULT_COLS2 = {  
        'ville'     :'ville.id as ville_id, ville.ville, ville.departement, ville.region1',
        'oeuvre'    :'oeuvre.ref, oeuvre.titre, oeuvre.type, oeuvre.annee_debut, oeuvre.annee_fin, oeuvre.lieux_conservation, oeuvre.inscriptions, oeuvre.texte',
        'metier'    :'metier.metier',
        'materiaux_technique':'materiaux_technique.materiaux_technique',
        'domaine'   :'domaine.domaine',
        'artiste'   :'artiste.nom_naissance, artiste.dit',
        'musee'     :"musee.museo, musee.nom, musee.ville as ville_musee, musee.latitude, musee.longitude"
    }

JOIN_TABLE = {
    "metier"                :[("creer","role")],
    "ville"                 :[("oeuvre","creation_lieux"), ("musee","ville")],
    "domaine"               :[("concerner","domaine"),],
    "artiste"               :[("creer","artiste")],
    "musee"                 :[("oeuvre","lieux_conservation"), ('ville', 'id')],
    "materiaux_technique"   :[("composer","materiaux")],
    "oeuvre"                :[("creer","oeuvre"), ("concerner","oeuvre"), ("composer","oeuvre"), ("musee","museo"), ("ville","id")],
    "composer"              :[("oeuvre","ref"),  ("materiaux_technique","id")],
    "creer"                 :[("metier","metier_search"), ("oeuvre","ref"),  ("artiste","id")],
    "concerner"             :[("oeuvre","ref"),  ("domaine","id")],
}

# ----------------------------------------------------------------------------------
# %% RECHERCHE EN BDD -- CIBLEE
# ----------------------------------------------------------------------------------
# %% sql_oeuvre_domaine
def sql_oeuvre_domaine(domaine, search_strategie='AND', search_level=1, cols=["oeuvre"], verbose=0):
    short_name = "sql_oeuvre_domaine"
    sql_sub_select = ""
    if domaine is not None:

        for i in range(0, len(cols)):
            if i > 0:
                sql_sub_select += f", "
            if len(cols)==1:
                sql_sub_select += f"distinct({cols[i]})"
            else:
                sql_sub_select += f"{cols[i]}"



        sql_sub_select = f"""SELECT {sql_sub_select} 
                            FROM concerner 
                            INNER JOIN domaine
                            ON domaine.id = concerner.domaine
                            """
        
        for level, cols in LEVEL_COLS.get("domaine", {}).items():
            if level <= search_level:
                for col in cols:
                    if "domaine" != col:
                        sql_sub_select += f" {search_strategie} "
                        search_value = domaine
                        if "search" in col:
                            search_value = convert_string_to_search_string(search_value)
                        sql_sub_select += f"{col} LIKE '%{search_value}%' "
    sql_sub_select = sql_sub_select.replace("\n", "")
    sql_sub_select = re.sub(' +', ' ', sql_sub_select)
    if verbose>1:
        print(f'[{short_name}] \tDEBUG : domaine => {sql_sub_select}')
    return sql_sub_select

# %% sql_oeuvre_materiaux
def sql_oeuvre_materiaux(value, search_strategie='AND', search_level=1, cols=["oeuvre"], verbose=0):
    short_name = "sql_oeuvre_materiaux"
    sql_sub_select = ""
    if value is not None:

        for i in range(0, len(cols)):
            if i > 0:
                sql_sub_select += f", "
            if len(cols)==1:
                sql_sub_select += f"distinct({cols[i]})"
            else:
                sql_sub_select += f"{cols[i]}"


        sql_sub_select = f"""SELECT {sql_sub_select} 
                            FROM composer 
                            INNER JOIN materiaux_technique
                            ON materiaux_technique.id = composer.materiaux
                            """
        
        for level, cols in LEVEL_COLS.get("materiaux_technique", {}).items():
            if level <= search_level:
                for col in cols:
                    if "materiaux_technique" != col:
                        sql_sub_select += f" {search_strategie} "
                    
                        search_value = value
                        if "search" in col:
                            search_value = convert_string_to_search_string(search_value)
                        sql_sub_select += f"{col} LIKE '%{search_value}%' "
    sql_sub_select = sql_sub_select.replace("\n", "")
    sql_sub_select = re.sub(' +', ' ', sql_sub_select)
    if verbose>1:
        print(f'[{short_name}] \tDEBUG : materiaux_technique => {sql_sub_select}')
    return sql_sub_select

# %% sql_oeuvre_oeuvre
def sql_oeuvre_oeuvre(value, type_oeuvre=None, search_strategie='AND', search_level=1, cols=["ref"], verbose=0):
    short_name = "sql_oeuvre_oeuvre"
    sql_sub_select = ""
    if value is not None:

        for i in range(0, len(cols)):
            if i > 0:
                sql_sub_select += f", "
            if len(cols)==1:
                sql_sub_select += f"distinct({cols[i]})"
            else:
                sql_sub_select += f"{cols[i]}"


        sql_sub_select = f"""SELECT {sql_sub_select} FROM oeuvre """
        nb_cond = 0
        for level, cols in LEVEL_COLS.get("oeuvre", {}).items():
            if level <= search_level:
                for col in cols:
                    if nb_cond > 0:
                        sql_sub_select += f" {search_strategie} "
                    else : 
                        sql_sub_select += f" WHERE "
                    sql_sub_select += f"{col} LIKE '%{value}%' "
    
    if type_oeuvre is not None:
        if value is not None:
            sql_sub_select += f" {search_strategie} "
        else:
            sql_sub_select += f" WHERE "
        sql_sub_select += f" oeuvre.type LIKE '%{type_oeuvre}%' "

    sql_sub_select = sql_sub_select.replace("\n", "")
    sql_sub_select = re.sub(' +', ' ', sql_sub_select)
    if verbose>1:
        print(f'[{short_name}] \tDEBUG : oeuvres => {sql_sub_select}')
    return sql_sub_select

# %% sql_oeuvre_artiste
def sql_oeuvre_artiste(value, role=None, search_strategie='AND', search_level=1, cols=["oeuvre"], verbose=0):
    short_name = "sql_oeuvre_artiste"
    sql_sub_select = ""
    if value is not None or role is not None:

        for i in range(0, len(cols)):
            if i > 0:
                sql_sub_select += f", "
            if len(cols)==1:
                sql_sub_select += f"distinct({cols[i]})"
            else:
                sql_sub_select += f"{cols[i]}"

        sql_sub_select = f"SELECT {sql_sub_select}  FROM creer  INNER JOIN artiste ON artiste.id = creer.artiste "
        nb_cond = 0

        if role is not None:
            search_value = convert_string_to_search_string(role)
            sql_sub_select += f" WHERE role LIKE '%{search_value}%' "
            nb_cond = 1

        if value is not None:
            for level, cols in LEVEL_COLS.get("artiste", {}).items():
                if level <= search_level:
                    for col in cols:
                        if nb_cond > 0:
                            sql_sub_select += f" {search_strategie} "
                        else : 
                            sql_sub_select += f" WHERE "
                        search_value = value
                        if "search" in col:
                            search_value = convert_string_to_search_string(search_value)
                        sql_sub_select += f"{col} LIKE '%{search_value}%' "
    
    sql_sub_select = sql_sub_select.replace("\n", "")
    sql_sub_select = re.sub(' +', ' ', sql_sub_select)
    if verbose>1:
        print(f'[{short_name}] \tDEBUG : oeuvres => {sql_sub_select}')
    return sql_sub_select

# %% sql_musee_ville
def sql_musee_ville(value, search_strategie='AND', search_level=1, cols=["museo"], verbose=0):
    short_name = "sql_musee_ville"
    sql_sub_select = ""
    if value is not None:

        for i in range(0, len(cols)):
            if i > 0:
                sql_sub_select += f", "
            if len(cols)==1:
                sql_sub_select += f"distinct({cols[i]})"
            else:
                sql_sub_select += f"{cols[i]}"

        search_value = convert_string_to_search_string(value)

        sql_sub_select = f"SELECT {sql_sub_select} FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%{search_value}%' "
    
    sql_sub_select = re.sub(' +', ' ', sql_sub_select)
    if verbose>1:
        print(f'[{short_name}] \tDEBUG : oeuvres => {sql_sub_select}')
    return sql_sub_select

# ----------------------------------------------------------------------------------
# %% RECHERCHE EN BDD -- MULTICRITERES
# ----------------------------------------------------------------------------------
#  %% search_multicriteria
def search_multicriteria(ville=None, oeuvre=None, musee=None, metier=None, materiaux=None, domaine=None, artiste=None, type_oeuvre=None, search_strategie='AND', search_level=1, limit=None, verbose=0):
    short_name = "search_multicriteria"
    sql_end = ""
    sql_begin = ""
    sql_from = " FROM "
    nb_cond = 0

    join_table = set()

    params = {'ville':ville, 'oeuvre':oeuvre, 'musee':musee, 'metier':metier, 'materiaux_technique':materiaux, 'domaine':domaine, 'artiste':artiste}

    for table_name, value in params.items():
        if value is not None and len(value)>0:
            if nb_cond >0:
                sql_from += ", "
            sql_from += table_name
            
            (sub_sql, nb_cond2, sub_sql_begin) = _create_cond(table_name=table_name, value=value, nb_cond=nb_cond, search_strategie=search_strategie, search_level=search_level, verbose=verbose)

            if len(sub_sql)>0:
                sql_end += sub_sql
                join_table.add(table_name)
                nb_cond = nb_cond2
                if verbose>1:
                    print(f'[{short_name}] \tDEBUG : {table_name} : condition {value} -- ADDED')
            elif verbose>0:
                print(f'[{short_name}] \tWARN : {table_name} : condition {value} -- NOT ADDED')
            if len(sub_sql_begin)>0:
                if len(sql_begin)>0:
                    sql_begin += ","
                sql_begin += " "+sub_sql_begin
                if verbose>1:
                    print(f'[{short_name}] \tDEBUG : {table_name} : cols {sub_sql_begin} -- ADDED')
            elif verbose>1:
                print(f'[{short_name}] \tDEBUG : {table_name} : no cols -- ADDED')
            
        elif verbose>1:
            print(f'[{short_name}] \tDEBUG : {table_name} : no condition value')
    
    if type_oeuvre is not None and len(type_oeuvre)>0:
        sql += f"type LIKE '{type_oeuvre}' "
        if nb_cond>0:
            sql+= f"{search_strategie} "        
        join_table.add('oeuvre')
        nb_cond += 1

    sql_where, to_add_from = _build_join(join_table=join_table, verbose=verbose)
    if len(sql_where) == 0:
        sql_end = " WHERE "+sql_end
    else:
        sql_end = " AND "+sql_end

    for t in to_add_from:
        if nb_cond >0:
                sql_from += ", "
        sql_from += t

    sql = "SELECT "+ sql_begin + " " + sql_from + " " +sql_where+ " " + sql_end
    if limit is not None:
        sql += f" LIMIT {limit}"

    if verbose>0:
        print(f'[{short_name}] \tDEBUG : {sql}')

    res = executer_sql(sql=sql, verbose=verbose)
    return res




#  %% search_multicriteria
def search_musees(ville=None, oeuvre=None, musee=None, metier=None, materiaux=None, domaine=None, artiste=None, type_oeuvre=None, search_strategie='AND', search_level=1, limit=None, verbose=0):
    short_name = "search_musees"
    sql_begin = """SELECT museo, nom, ville.ville, latitude, longitude, count(ref) as nb_oeuvres, count(artiste) as nb_artistes
                    FROM musee
                    inner join ville
                    on ville.id = musee.ville

                    inner join oeuvre
                    on museo = lieux_conservation

                    inner join creer
                    on ref = creer.oeuvre

                   """
                   # LIMIT 100;
    sql_where = ""
    sql_end = "GROUP BY museo  ORDER BY nb_oeuvres DESC"
    nb_cond = 0

    sub_oeuvre = ""

    if oeuvre is not None or type_oeuvre is not None:
        sub_oeuvre = sql_oeuvre_oeuvre(value=oeuvre, type_oeuvre=type_oeuvre, verbose=verbose, search_strategie=search_strategie, cols=["lieux_conservation"], search_level=search_level)
        if sub_oeuvre is not None and len(sub_oeuvre)>0:
            if len(sql_where)==0:
                sql_where += f" WHERE"
            else:
                sql_where += f" {search_strategie}"
            sql_where += f" museo in ({sub_oeuvre}) "
            nb_cond += 1

    if ville is not None:
        ville



    params = {'ville':ville, 'oeuvre':oeuvre, 'musee':musee, 'metier':metier, 'materiaux_technique':materiaux, 'domaine':domaine, 'artiste':artiste}

    for table_name, value in params.items():
        if value is not None and len(value)>0:
            sql_sub_select = f"SELECT musee FROM {table_name} WHERE "
            if "oeuvre" == table_name:
                sql_sub_select = f"SELECT lieux_conservation FROM {table_name} WHERE "

            nb_sub_cond = 0
            for level, cols in LEVEL_COLS.get(table_name, {}).items():
                if level <= search_level:
                    for col in cols:
                        if nb_sub_cond>0:
                            sql_where += f" {search_strategie} "
                        sql_sub_select += f"{col} LIKE '{value}' "

            

            (sub_sql, nb_cond2, sub_sql_begin) = _create_cond(table_name=table_name, value=value, nb_cond=nb_cond, search_strategie=search_strategie, search_level=search_level, verbose=verbose)

            if len(sub_sql)>0:
                sql_end += sub_sql
                join_table.add(table_name)
                nb_cond = nb_cond2
                if verbose>1:
                    print(f'[{short_name}] \tDEBUG : {table_name} : condition {value} -- ADDED')
            elif verbose>0:
                print(f'[{short_name}] \tWARN : {table_name} : condition {value} -- NOT ADDED')
            if len(sub_sql_begin)>0:
                if len(sql_begin)>0:
                    sql_begin += ","
                sql_begin += " "+sub_sql_begin
                if verbose>1:
                    print(f'[{short_name}] \tDEBUG : {table_name} : cols {sub_sql_begin} -- ADDED')
            elif verbose>1:
                print(f'[{short_name}] \tDEBUG : {table_name} : no cols -- ADDED')
            
        elif verbose>1:
            print(f'[{short_name}] \tDEBUG : {table_name} : no condition value')

        sql_where += f" WHERE museo in ({sql_sub_select})"
        if verbose>1:
            print(f'[{short_name}] \tDEBUG : {table_name} => {sql_sub_select}')
            print(f'[{short_name}] \tDEBUG : {sql_where}')
    
    if type_oeuvre is not None and len(type_oeuvre)>0:
        sql += f"type LIKE '{type_oeuvre}' "
        if nb_cond>0:
            sql+= f"{search_strategie} "        
        join_table.add('oeuvre')
        nb_cond += 1

    sql_where, to_add_from = _build_join(join_table=join_table, verbose=verbose)
    if len(sql_where) == 0:
        sql_end = " WHERE "+sql_end
    else:
        sql_end = " AND "+sql_end

    for t in to_add_from:
        if nb_cond >0:
                sql_from += ", "
        sql_from += t

    sql = "SELECT "+ sql_begin + " " + sql_from + " " +sql_where+ " " + sql_end
    if limit is not None:
        sql += f" LIMIT {limit}"

    if verbose>0:
        print(f'[{short_name}] \tDEBUG : {sql}')

    res = executer_sql(sql=sql, verbose=verbose)
    return res
    
    

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
        res = search_on_table(table_name=table_name, search_str=search_str, search_level=search_level+1, increase_level=increase_level, limit=limit, verbose=verbose)
    return res

# ----------------------------------------------------------------------------------
#  %%                      PRIVATE FUNCTIONS
# ----------------------------------------------------------------------------------
def _create_cond(table_name, value, nb_cond=0, search_strategie='AND', search_level=1, verbose=0):
    sql = ""
    sql_begin = ""
    if value is not None and len(value)>0:
        for level, cols in LEVEL_COLS.get(table_name, {}).items():
            if level <= search_level:
                for col in cols:
                    if nb_cond>0:
                        sql+= f"{search_strategie} "            
                        sql_begin += ','
                    sql+= f"{col} LIKE '%{value}%' "
                    nb_cond += 1
                    # TODO ce ne sont pas ces colonnes qu'il faut récupérer
                    sql_begin = SEARCH_RESULT_COLS_BY_TABLE.get(table_name, "")

    return (sql, nb_cond, sql_begin)

def _build_join(join_table, verbose=0):
    ever_join = set()
    sql_join = ""
    to_add_from = set()

    for table_name in join_table:
        for to_join in join_table:
            if table_name != to_join and (table_name, to_join) not in ever_join and (to_join, table_name) not in ever_join:

                joinable = _joinable_via(table_name=table_name, table_name2=to_join)

                if joinable is not None and len(joinable)>0:
                    if joinable == to_join:
                        sql_join_tp = _join_for_table(table_name=table_name, to_join=to_join, verbose=verbose)
                        if len(sql_join_tp)>0:
                            if len(sql_join) == 0:
                                sql_join += " WHERE "
                            else:
                                sql_join += f" AND "
                            sql_join += f"{sql_join_tp}"
                            ever_join.add((table_name, to_join))
                            ever_join.add((to_join, table_name))
                    # Gestion des relations N:N
                    else:
                        sql_join_tp = _join_for_table(table_name=table_name, to_join=joinable, verbose=verbose)
                        if len(sql_join_tp)>0:
                            if len(sql_join) == 0:
                                sql_join += " WHERE "
                            else:
                                sql_join += f" AND "
                            sql_join += f"{sql_join_tp}"
                            ever_join.add((table_name, joinable))
                            ever_join.add((joinable, table_name))
                            to_add_from.add(joinable)
                            
                            sql_join_tp = _join_for_table(table_name=to_join, to_join=joinable, verbose=verbose)
                            if len(sql_join_tp)>0:
                                if len(sql_join) == 0:
                                    sql_join += " WHERE "
                                else:
                                    sql_join += f" AND "
                                sql_join += f"{sql_join_tp}"
                                ever_join.add((to_join, joinable))
                                ever_join.add((joinable, to_join))
                                ever_join.add((table_name, to_join)) 
                                ever_join.add((to_join, table_name))
    return sql_join,to_add_from 

def _joinable_via(table_name, table_name2, verbose=0):
    """Regarde si une jointure est possible pour les 2 tables soit en direct, soit via une troisième table pour les liaisons N:N

    Args:
        table_name (_type_): _description_
        table_name2 (_type_): _description_
        verbose (int, optional): _description_. Defaults to 0.

    Returns:
        _type_: _description_
    """
    join1 = JOIN_TABLE.get(table_name)
    join2 = JOIN_TABLE.get(table_name2)
    res = ''
    for tb_name, _ in join1:
        if tb_name == table_name2:
            res = table_name2
        elif tb_name.endswith("er"):
            for tb_name2, _ in join2:
                if tb_name2 == tb_name:
                    res = tb_name2
        if len(res)>0:
            break
    return res




def _join_for_table(table_name, to_join, verbose=0):
    """Build the equals part between the 2 tables

    Args:
        table_name (str): the table name
        to_join (str): the second table name

    Returns:
        str: the equals str
    """
    join_str = ""
    joins = JOIN_TABLE.get(table_name)
    for tb_name, col_name in joins:
        
        if tb_name == to_join:
            join_str += f"{tb_name}.{col_name} = "
    
            to_join_table_join = JOIN_TABLE.get(tb_name)
            for tb_name2, col_name2 in to_join_table_join:
                if tb_name2 == table_name:
                    join_str += f"{tb_name2}.{col_name2} "
                    break
            break
    return join_str

# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------
def _test_search_artiste(verbose=1):
    """Teste la recherche d'artiste avec plusieurs niveaux de recherche

    Args:
        verbose (int, optional): Log level. Defaults to 1.
    """
    
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
    """Teste la recherche d'oeuvre avec plusieurs niveaux de recherche

    Args:
        verbose (int, optional): Log level. Defaults to 1.
    """
    
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

def _test_search_multicriteria(verbose=1):
    """Test la recherche multicritères

    Args:
        verbose (int, optional): Log level. Defaults to 1.
    """
    to_test={ 
        ('ville', 'oeuvre', 'musee', 'metier', 'materiaux', 'domaine', 'artiste', 'type_oeuvre', 'search_strategie', 'search_level'):45,
        ('BRIEUC', None,   None,   None,       'agate',    None, None, None, 'OR', 1):[],
        ('BRIEUC', 'RAOUL',   None,   'peintre',  'agate',    None, None, None, 'AND', 1):[],
        ('BRIEUC', 'GOULVEN',   None,   'peintre',  'agate',    None, None, None, 'OR', 1):[],
        ('LANNION', None,   None,   None,       None,        None, None, None, 'AND', 1):[],
        ('BRIEUC', None,   None,   None,       None,        None, None, None, 'AND', 1):[(319, 'Saint-Brieuc', "Côtes-d'Armor", 'Bretagne')],
        ('BRIEUC', None,   None,   'peintre',  None,        None, None, None, 'AND', 1):[],
    }
    limit=100
    for (ville, oeuvre, musee, metier, materiaux, domaine, artiste, type_oeuvre, search_strategie, search_level), expected in tqdm(to_test.items(), desc="[TEST search_multicriteria]"):
        # On passe la première ligne qui sert uniquement d'entête pour les valeurs
        if 'ville' != ville:
            res = search_multicriteria(ville=ville, oeuvre=oeuvre, musee=musee, metier=metier, 
                                materiaux=materiaux, domaine=domaine, artiste=artiste, 
                                type_oeuvre=type_oeuvre, search_strategie=search_strategie, search_level=search_level, limit=limit, verbose=verbose)
            assert len(res) == len(expected) or len(res) == limit
            if len(res)>0 and len(res)<limit:
                assert res == expected

def _test_join_for_table(verbose=1):
    """Teste la construction automatique des jointures entre les tables

    Args:
        verbose (int, optional): Log level. Defaults to 1.
    """
    to_test = {
        ("domaine", "concerner") :'concerner.domaine = domaine.id ',
        ("domaine", "artiste")  :"",
        ("ville", "creer")      :"",
        ("musee", "ville")      :'ville.id = musee.ville ',
        ("ville", "musee")      :'musee.ville = ville.id ',
        ("oeuvre", "ville")     :'ville.id = oeuvre.creation_lieux ',
        ("oeuvre", "musee")     :'musee.museo = oeuvre.lieux_conservation ',
        ("musee", "oeuvre")     :'oeuvre.lieux_conservation = musee.museo ',
        ("artiste", "creer")    :'creer.artiste = artiste.id ',
        ("creer", "artiste")    :'artiste.id = creer.artiste ',
        ("materiaux_technique", "composer")    :'composer.materiaux = materiaux_technique.id ',
        ("oeuvre", "creer")     :'creer.oeuvre = oeuvre.ref ',
        ("oeuvre", "concerner") :'concerner.oeuvre = oeuvre.ref ',
        ("oeuvre", "composer")  :'composer.oeuvre = oeuvre.ref ',
        ("metier", "creer")     :'creer.role = metier.metier_search ',
    }

    for (table_name, join_table), expected in tqdm(to_test.items(), desc="[TEST join_for_table]"):
        res = _join_for_table(table_name=table_name, to_join=join_table, verbose=verbose)
        assert res == expected

def _test_build_join(verbose=1):
    """Teste la construction automatique des jointures pour plusieurs tables

    Args:
        verbose (int, optional): Log level. Defaults to 1.
    """
    to_test = {
        ("oeuvre", "artiste")           :(' WHERE creer.oeuvre = oeuvre.ref  AND creer.artiste = artiste.id ', {'creer'}),
        ("oeuvre", "artiste", "ville")  :(' WHERE creer.oeuvre = oeuvre.ref  AND creer.artiste = artiste.id  AND ville.id = oeuvre.creation_lieux ', {'creer'}),
        ("oeuvre", "domaine")           :(' WHERE concerner.oeuvre = oeuvre.ref  AND concerner.domaine = domaine.id ', {'concerner'}),
        ("oeuvre", "domaine", "artiste"):(' WHERE concerner.oeuvre = oeuvre.ref  AND concerner.domaine = domaine.id  AND creer.oeuvre = oeuvre.ref  AND creer.artiste = artiste.id ', {'concerner', 'creer'}),
    }
    for to_join, expected in tqdm(to_test.items(), desc="[TEST build_join]"):
        res = _build_join(to_join, verbose=0)
        assert res == expected

def _test_sql_oeuvre_domaine(verbose=1):
    to_test = {
        "afrique"   :"SELECT distinct(oeuvre) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine AND dom_search LIKE '%AFRIQUE%' ", 
        "amérique"  :"SELECT distinct(oeuvre) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine AND dom_search LIKE '%AMERIQUE%' ",
        "céramique" :"SELECT distinct(oeuvre) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine AND dom_search LIKE '%CERAMIQUE%' ",
        "broderie"  :"SELECT distinct(oeuvre) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine AND dom_search LIKE '%BRODERIE%' ",
        "basque"    :"SELECT distinct(oeuvre) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine AND dom_search LIKE '%BASQUE%' ",
        "épigraphie":"SELECT distinct(oeuvre) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine AND dom_search LIKE '%EPIGRAPHIE%' ",
        "miniature" :"SELECT distinct(oeuvre) FROM concerner INNER JOIN domaine ON domaine.id = concerner.domaine AND dom_search LIKE '%MINIATURE%' ",
    }
    
    for domaine, expected in tqdm(to_test.items(), desc="[TEST sql_oeuvre_domaine]"):
        sql = sql_oeuvre_domaine(domaine=domaine, cols=["oeuvre"], verbose=verbose)
        assert sql == expected

def _test_sql_oeuvre_materiaux(verbose=1):
    to_test = {
        "papier baryté"   :"SELECT distinct(oeuvre) FROM composer INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux AND mat_search LIKE '%PAPIER BARYTE%' ", 
        "verre"           :"SELECT distinct(oeuvre) FROM composer INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux AND mat_search LIKE '%VERRE%' ", 
        "acajou"          :"SELECT distinct(oeuvre) FROM composer INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux AND mat_search LIKE '%ACAJOU%' ", 
        "acétate"         :"SELECT distinct(oeuvre) FROM composer INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux AND mat_search LIKE '%ACETATE%' ", 
        "acier"           :"SELECT distinct(oeuvre) FROM composer INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux AND mat_search LIKE '%ACIER%' ", 
        "albâtre"         :"SELECT distinct(oeuvre) FROM composer INNER JOIN materiaux_technique ON materiaux_technique.id = composer.materiaux AND mat_search LIKE '%ALBATRE%' ", 
    }
    
    for domaine, expected in tqdm(to_test.items(), desc="[TEST sql_oeuvre_materiaux]"):
        sql = sql_oeuvre_materiaux(value=domaine, cols=["oeuvre"], verbose=verbose)
        assert sql == expected

def _test_sql_oeuvre_oeuvre(verbose=1):
    to_test = {
        ("hugues", "violon")     :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%hugues%' AND oeuvre.type LIKE '%violon%' ", 
        ("hugues", "tableau")    :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%hugues%' AND oeuvre.type LIKE '%tableau%' ", 
        ("hugues", None)         :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%hugues%' ",  
        ("goulven", None)        :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%goulven%' ",  
        ("goulven", "encre")     :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%goulven%' AND oeuvre.type LIKE '%encre%' ",  
        ("raoul", "violon")      :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%raoul%' AND oeuvre.type LIKE '%violon%' ",  
        ("raoul", "buste")       :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%raoul%' AND oeuvre.type LIKE '%buste%' ",  
        ("raoul", "tableau")     :"SELECT distinct(ref) FROM oeuvre WHERE titre LIKE '%raoul%' AND oeuvre.type LIKE '%tableau%' ",  
    }
    
    for (value, type_oeuvre), expected in tqdm(to_test.items(), desc="[TEST sql_oeuvre_oeuvre]"):
        sql = sql_oeuvre_oeuvre(value=value, type_oeuvre=type_oeuvre, verbose=verbose)
        assert sql == expected


def _test_sql_oeuvre_artiste(verbose=1):
    to_test = {
        ("hugues", "sculpteur")     :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%SCULPTEUR%' AND nom_search LIKE '%HUGUES%' ", 
        ("hugues", "PEINTRE")       :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%PEINTRE%' AND nom_search LIKE '%HUGUES%' ", 
        ("hugues", "ORFEVRE")       :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%ORFEVRE%' AND nom_search LIKE '%HUGUES%' ", 
        ("hugues", None)            :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE nom_search LIKE '%HUGUES%' ", 
        ("roméo", None)             :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE nom_search LIKE '%ROMEO%' ", 
        ("goulven", None)           :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE nom_search LIKE '%GOULVEN%' ", 
        ("raoul", None)             :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE nom_search LIKE '%RAOUL%' ", 
        ("raoul", "peintre")        :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%PEINTRE%' AND nom_search LIKE '%RAOUL%' ", 
        ("raoul", "GRAVEUR")        :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%GRAVEUR%' AND nom_search LIKE '%RAOUL%' ", 
        ("raoul", "sculpteur")      :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%SCULPTEUR%' AND nom_search LIKE '%RAOUL%' ", 
        ("raoul", "illustrateur")   :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%ILLUSTRATEUR%' AND nom_search LIKE '%RAOUL%' ", 
        ("raoul", "ARCHITECTE")     :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%ARCHITECTE%' AND nom_search LIKE '%RAOUL%' ", 
        ("raoul", "dessinateur")    :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%DESSINATEUR%' AND nom_search LIKE '%RAOUL%' ", 
        ("raoul", "AQUARELLISTE")   :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%AQUARELLISTE%' AND nom_search LIKE '%RAOUL%' ", 
        ("raoul", "createur")       :"SELECT distinct(oeuvre) FROM creer INNER JOIN artiste ON artiste.id = creer.artiste WHERE role LIKE '%CREATEUR%' AND nom_search LIKE '%RAOUL%' ", 
    }
    
    for (value, role), expected in tqdm(to_test.items(), desc="[TEST sql_oeuvre_artiste]"):
        sql = sql_oeuvre_artiste(value=value, role=role, verbose=verbose)
        assert sql == expected

def _test_sql_musee_ville(verbose=1):
    to_test = {
        "Brest"         :"SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%BREST%' ", 
        "lannion"       :"SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%LANNION%' ", 
        "Saint brieuc"  :"SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%SAINT BRIEUC%' ", 
        "Paris"         :"SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%PARIS%' ", 
        "acier"         :"SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%ACIER%' ", 
        "albâtre"       :"SELECT distinct(museo) FROM musee INNER JOIN ville ON ville.id = musee.ville WHERE ville_search LIKE '%ALBATRE%' ", 
    }
    
    for value, expected in tqdm(to_test.items(), desc="[TEST sql_musee_ville]"):
        sql = sql_musee_ville(value=value, verbose=verbose)
        assert sql == expected

# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    verbose = 1
    _test_sql_musee_ville(verbose=verbose)
    _test_sql_oeuvre_artiste(verbose=verbose)
    _test_sql_oeuvre_oeuvre(verbose=verbose)
    _test_sql_oeuvre_materiaux(verbose=verbose)
    _test_sql_oeuvre_domaine(verbose=verbose)
    _test_join_for_table(verbose=verbose)
    _test_build_join(verbose=verbose)
    _test_search_multicriteria(verbose=verbose)
    _test_search_oeuvre(verbose=verbose)
    _test_search_artiste(verbose=verbose)


