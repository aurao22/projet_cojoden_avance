# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" The project demonstrator

Project: Cojoden avance
=======

Usage:
======
    python demonstrator.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "GNU GENERAL PUBLIC LICENSE"
__date__        = "2022-10-01"
__version__     = "1.0.0"
# ----------------------------------------------------------------------------------
# %% import
import sys
from os import getcwd
from os.path import join

execution_path = getcwd()

if 'PROJETS' not in execution_path:
    execution_path = join(execution_path, "PROJETS")
if 'projet_cojoden_avance' not in execution_path:
    execution_path = join(execution_path, "projet_cojoden_avance")

print(f"[demonstrator] execution path= {execution_path}")
sys.path.append(execution_path)

import pandas as pd
from dao.cojoden_dao import *
import dao.cojoden_dao_search as dao_search
from geopy.distance import geodesic
from tqdm import tqdm

CITIES_COORDONATES_SAMPLE = {
    "pédernec" : (48.856614,-2.3522219),
    "lannion" : (48.7587757,-3.471542),
    "st_malo" : (48.649337,-2.025674),
    "st_brieuc" : (48.51418,-2.765835),
    "brest" : (48.390394,-4.486076),
    "paris" : (48.856614,2.3522219),
}

DEFAULT_VALUES = {
            'oeuvre_text'       :'RAOUL',
            'oeuvre_year'       :'',
            'oeuvre_type'       :'',
            'oeuvre_domaine'    :'',
            'artiste_text'      :'',
            'artiste_role'      :'',
            'musee_city'        :'BRETAGNE',
            'musee_nb'          :100,
            # Lannion : 48.7587757,-3.471542
            'coord_lat'         :48.7587757, 
            'coord_long'        :-3.471542,  
            'search_level'      :1,
        }

MUSEES_LABELS = {
            'oeuvre_text'       :("Oeuvre : mot clé", "(mot clé contenu dans le titre, inscription ou texte de l'oeuvre)"),      
            'artiste_text'      :("Artiste : nom ou prénom", "(Nom ou Prénom de l'artiste)"),
            'artiste_role'      :("Artiste : rôle", "(Métier / Rôle de l'artiste)"),
            'musee_city'        :("Lieu (ville, région)", "(ville recherchée)"),
            'coord_lat'         :("Coordonnées : latitude"  , "(°)"),
            'coord_long'        :("Coordonnées : longitude" , "(°)"),
        }

OEUVRES_LABELS = {
            'oeuvre_text'       :("Oeuvre : mot clé", "(mot clé contenu dans le titre, inscription ou texte de l'oeuvre)"),
            # 'oeuvre_year'       :("Oeuvre : année", "(année de création de l'oeuvre)"),
            # 'oeuvre_type'       :("Oeuvre : type", "(type d'oeuvre)"),
            # 'oeuvre_domaine'    :("Oeuvre : domaine", "(Thème de l'oeuvre)"),
            'coord_lat'         :("Coordonnées : latitude"  , "(°)"),
            'coord_long'        :("Coordonnées : longitude" , "(°)"),
            'artiste_text'      :("Artiste : mot clé", "(Nom ou Prénom de l'artiste)"),
            'artiste_role'      :("Artiste : role", "(Métier / Rôle de l'artiste)"),
            # 'search_level'      :("Niveau de recherche :" , "(de 1 à 3)"),
        }

# ---------------------------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------------------------
# %% convert_musee_search_result_to_df
def convert_musee_search_result_to_df(sql_result, columns = ["museo" , "nom" , "ville" , "latitude", "longitude", "nb_oeuvres" , "nb_artistes", "distance"], coord_lat=None, coord_long=None, verbose=0):
    shortname = 'convert_musee_search_result_to_df'
    if verbose>0:
        print(f"[{shortname}] \tINFO : {len(sql_result)} rows received, starting conversion...")    
    df = convert_search_result_to_df(sql_result, columns, verbose=verbose)
    if df.shape[0]>0:
        if verbose>1:
            print(f"[{shortname}] \tDEBUG : calculating the distance...")
        df["distance"] = df[['latitude', 'longitude']].apply(lambda x: distance(lat_1=coord_lat, long_1=coord_long, lat_2=x['latitude'], long_2=x['longitude'], verbose=verbose), axis=1)
        if verbose>1:
            print(f"[{shortname}] \tDEBUG : calculating the distance... DONE")
        df = df.sort_values(by=['distance'])
        # Pour convertir en float il faut des valeurs
        df['latitude'] = df['latitude'].fillna(0.0)
        df['longitude'] = df['longitude'].fillna(0.0)
        df['distance'] = df['distance'].fillna(0.0)
        
        # Conversion en float
        df['latitude'] = df['latitude'].astype('float')
        df['longitude'] = df['longitude'].astype('float')
        df['distance'] = df['distance'].astype('float')
    elif verbose>0:
        print(f"[{shortname}] \tINFO : no result : {len(sql_result)} row received")    

    if verbose>1:
        print(f"[{shortname}] \tDEBUG : sql result to DF DONE")
    return df

# %% convert_search_result_to_df
def convert_search_result_to_df(sql_result, columns, verbose=0):
    shortname = 'convert_search_result_to_df'
    if verbose>1:
        print(f"[{shortname}] \tDEBUG : sql result to DF....")
    search_result = []
    col_pos = {}
    for i in range(0, len(columns)):
        col_pos[columns[i]] = i
    for sql_row in sql_result:
        data2 = []
        for col in columns:
            if col != "distance":
                data2.append(sql_row[col_pos.get(col, 0)])
            
        search_result.append(data2)
    df = pd.DataFrame(data=search_result,columns=columns[:-1])
    if verbose>1:
        print(f"[{shortname}] \tDEBUG : {type(df)} : {df.shape}")
        print(f"[{shortname}] \tDEBUG : sql result to DF....DONE")
    return df

# %% distance
def distance(lat_1, long_1 , lat_2 , long_2, verbose=0):
    """Distance à vol d'oiseau

    Args:
        lat_1 (_type_): _description_
        long_1 (_type_): _description_
        lat_2 (_type_): _description_
        long_2 (_type_): _description_
        verbose (int, optional): _description_. Defaults to 0.

    Returns:
        _type_: _description_
    """
    if None in [lat_1, long_1, lat_1, lat_2] or pd.isna(lat_1) or pd.isna(long_1) or pd.isna(lat_1) or pd.isna(lat_2):
        return 0.0
    
    ville1=(float(lat_1) , float(long_1))
    ville2=(float(lat_2) , float(long_2))
    return round(geodesic(ville1, ville2).km, 2)

# %% search_musees
def search_musees(input_datas, verbose=0):
    short_name = 'search_musees'
    oeuvre          =input_datas.get('oeuvre_text', None)
    domaine         =input_datas.get('oeuvre_domaine', None)
    # 'oeuvre_year'       :1781,
    # 'oeuvre_type'       :10,
    type_oeuvre     =input_datas.get("oeuvre_type", None)
    
    metier          =input_datas.get('artiste_role', None)
    artiste         =input_datas.get('artiste_text', None)

    ville           =input_datas.get('musee_city', None)
    musee           =input_datas.get('', None)
    materiaux       =input_datas.get('', None)

    search_strategie='AND'
    try:
        search_level    =int(input_datas.get('search_level', 1))
    except:
        search_level    =input_datas.get('search_level', 1)
    try:
        limit           =int(input_datas.get('musee_nb', 100))
    except:
        limit           =input_datas.get('musee_nb', 100)

    if verbose > 0:
        print(f"[{short_name}] INFO : Lancement de la recherche...")

    if verbose > 3:
        print(f"VILLE : {type(ville)} => {ville}")
        print(f"oeuvre : {type(oeuvre)} => {oeuvre}")
        print(f"musee : {type(musee)} => {musee}")
        print(f"metier : {type(metier)} => {metier}")
        print(f"materiaux : {type(materiaux)} => {materiaux}")
        print(f"domaine : {type(domaine)} => {domaine}")
        print(f"artiste : {type(artiste)} => {artiste}")
        print(f"type_oeuvre : {type(type_oeuvre)} => {type_oeuvre}")
        print(f"search_level : {type(search_level)} => {search_level}")
        print(f"limit : {type(limit)} => {limit}")

    sql_result = dao_search.search_musees(ville=ville, oeuvre=oeuvre, musee=musee, metier=metier, materiaux=materiaux, domaine=domaine, artiste=artiste, type_oeuvre=type_oeuvre, search_strategie=search_strategie, search_level=search_level, limit=limit, verbose=verbose)
    if verbose > 0:
        print(f"[{short_name}] INFO : Recherche terminée, conversion du résultat en DF")

    coord_lat       = input_datas.get('coord_lat', None)
    coord_long      = input_datas.get('coord_long', None)

    columns = ["museo" , "nom" , "ville" , "latitude", "longitude", "nb_oeuvres" , "nb_artistes", "distance"]

    df = convert_musee_search_result_to_df(sql_result=sql_result, columns =columns, coord_lat=coord_lat, coord_long=coord_long, verbose=verbose)
    if verbose > 0:
        print(f"[{short_name}] INFO : Traitement terminé")
    return df

# ----------------------------------------------------------------------------------
# %%                        TESTS
# ----------------------------------------------------------------------------------



def _test_distance(verbose=1):
    short_name = "[TEST distance]"
    lannion = CITIES_COORDONATES_SAMPLE.get("lannion", (0,0))
    st_malo = CITIES_COORDONATES_SAMPLE.get("st_malo", (0,0))
    st_brieuc = CITIES_COORDONATES_SAMPLE.get("st_brieuc", (0,0))
    brest = CITIES_COORDONATES_SAMPLE.get("brest", (0,0))
    paris = CITIES_COORDONATES_SAMPLE.get("paris", (0,0))
    to_test = {
        # Lannion - Lannion
        (DEFAULT_VALUES["coord_lat"], DEFAULT_VALUES["coord_long"], DEFAULT_VALUES["coord_lat"], DEFAULT_VALUES["coord_long"]) : 0.0,
        (lannion[0],lannion[1], lannion[0],lannion[1]) : 0.0,
        (st_malo[0],st_malo[1], st_malo[0],st_malo[1]) : 0.0,
        (st_malo[0],st_malo[1], lannion[0],lannion[1]) : round(107.11407651053311,2), # 156.7 par la route, correcte à vol d'oiseau
        (st_malo[0],st_malo[1], st_brieuc[0],st_brieuc[1]) : round(56.64109945832999,2),  # 92.9 par la route, correcte à vol d'oiseau
        (lannion[0],lannion[1], st_brieuc[0],st_brieuc[1]) : round(58.69492626868507,2), 
        (lannion[0],lannion[1], brest[0],brest[1]) : round(85.339063061735,2),
        (lannion[0],lannion[1], paris[0],paris[1]) : round(427.8072018867463,2), 
    }

    for (lat_1, long_1 , lat_2 , long_2), expected in tqdm(to_test.items(), desc=short_name):
        dis = distance(lat_1, long_1 , lat_2 , long_2, verbose=verbose)
        assert dis == expected, f"{short_name} \t FAIL : {expected} expected and not {dis}"

def _test_search_musees(verbose=2):
    lannion = CITIES_COORDONATES_SAMPLE.get("lannion", (0,0))
    default_input = {
        # 'oeuvre_text'       : None,    
        # 'oeuvre_type'       : None,
        # 'oeuvre_domaine'    : None,
        # 'artiste_role'      : None,
        # 'artiste_text'      : None,
        # 'musee_city'       : None,
        'search_level'      : 1,
        'musee_nb'         : 100,
        'coord_lat'         : lannion[0],
        'coord_long'        : lannion[1],
    }

    to_test = [
        {'artiste_text':'RAOUL'},
        {'musee_city':'Bretagne'},
        {'oeuvre_text': 'Goulven'},
        {'oeuvre_text': 'Raoul'},
        {'artiste_text':'RAOUL', 'artiste_role':'peintre'},
        {'musee_city':'Bretagne', 'oeuvre_text': 'Raoul'},
        {'musee_city':'Bretagne', 'oeuvre_text': 'Raoul', 'artiste_role':'peintre'},
    ]

    for val_dic in tqdm(to_test, desc="search_musees"):
        params = default_input.copy()
        for key, v in val_dic.items():
            params[key] = v
        df_res = search_musees(input_datas=params, verbose=verbose)
        print(df_res.shape)
        assert df_res.shape[0] > 0    
 


# ---------------------------------------------------------------------------------------------
# TODO faire une version en mode console
# ---------------------------------------------------------------------------------------------

            

# ----------------------------------------------------------------------------------
#                        MAIN
# ----------------------------------------------------------------------------------
# %% main
if __name__ == '__main__':
    short_name = "demonstrator"
    print(f"[{short_name}]------------------------------------------------------ START")
    verbose = 2
    if verbose>0:
        _test_search_musees(verbose=verbose)
        _test_distance(verbose=verbose)

    print(f"[{short_name}]------------------------------------------------------ END")