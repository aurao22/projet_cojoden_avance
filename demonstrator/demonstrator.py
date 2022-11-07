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
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"
# ----------------------------------------------------------------------------------
# %% import
import sys
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from dao.cojoden_dao import *
from dao.cojoden_dao_search import search_multicriteria, search_musees
from geopy.distance import geodesic
from tqdm import tqdm

DEFAULT_VALUES = {
            'oeuvre_text'       :'',
            'oeuvre_year'       :1781,
            'oeuvre_type'       :10,
            'oeuvre_domaine'    :'Sculpture',
            'artiste_text'      :'RAOUL',
            'artiste_role'      :'Sculpteur',
            'mussee_city'       :'Bretagne',
            'mussee_nb'         :3,
            # Lannion : 48.7587757,-3.471542
            'coord_lat'         :48.7587757, 
            'coord_long'        :-3.471542,  
            'search_level'      :1,
        }

LABELS = {
        'oeuvre_text'       :("Oeuvre : mot clé", "(mot clé contenu dans le titre, inscription ou texte de l'oeuvre)"),
        'oeuvre_year'       :("Oeuvre : année", "(année de création de l'oeuvre)"),
        'oeuvre_type'       :("Oeuvre : type", "(type d'oeuvre)"),
        'oeuvre_domaine'    :("Oeuvre : domaine", "(Thème de l'oeuvre)"),
        'artiste_text'      :("Artiste : mot clé", "(Nom ou Prénom de l'artiste)"),
        'artiste_role'      :("Artiste : role", "(Métier / Rôle de l'artiste)"),
        'mussee_city'       :("Musée : ville", "(ville recherchée)"),
        'mussee_nb'         :("Musée : nb", "(nombre de musée recherché à proximité)"),
        'coord_lat'         :("Coordonnées : latitude"  , "(°)"),
        'coord_long'        :("Coordonnées : longitude" , "(°)"),
        'search_level'      :("Niveau de recherche :" , "(de 1 à 3)"),
        }

# ---------------------------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------------------------
# %% convert_musee_search_result_to_df
def convert_musee_search_result_to_df(sql_result, columns = ["museo" , "nom" , "ville" , "latitude", "longitude", "nb_oeuvres" , "nb_artistes", "distance"], coord_lat=None, coord_long=None, verbose=0):
    df = convert_search_result_to_df(sql_result, columns, verbose=verbose-1)
    df["distance"] = df[["latitude", "longitude"]].apply(lambda x: distance(lat_1=coord_lat, long_1=coord_long, lat_2=x['latitude'], long_2=x['longitude'], verbose=verbose))
    df = df.sort_values(by=['distance'])
    # Pour convertir en float il faut des valeurs
    df['latitude'] = df['latitude'].fillna(0.0)
    df['longitude'] = df['longitude'].fillna(0.0)
    df['distance'] = df['distance'].fillna(0.0)
    
    # Conversion en float
    df['latitude'] = df['latitude'].astype('float')
    df['longitude'] = df['longitude'].astype('float')
    df['distance'] = df['distance'].astype('float')
    return df

# %% convert_search_result_to_df
def convert_search_result_to_df(sql_result, columns, verbose=0):
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
    if lat_1 is None or long_1 is None or lat_2 is None or long_2 is None:
        return 0.0
    ville1=(float(lat_1) , float(long_1))
    ville2=(float(lat_2) , float(long_2))
    return geodesic(ville1, ville2).km

# %% search_musees
def search_musees(input_datas, verbose=0):
    short_name = 'search'
    oeuvre          =input_datas.get('oeuvre_text', None)
    domaine         =input_datas.get('oeuvre_domaine', None)
    # 'oeuvre_year'       :1781,
    # 'oeuvre_type'       :10,
    type_oeuvre     =input_datas.get("oeuvre_type", None)
    
    metier          =input_datas.get('artiste_role', None)
    artiste         =input_datas.get('artiste_text', None)

    ville           =input_datas.get('mussee_city', None)
    musee           =input_datas.get('', None)
    materiaux       =input_datas.get('', None)

    search_strategie='AND'
    search_level    =int(input_datas.get('search_level', 1))
    limit           =input_datas.get('mussee_nb', 100)
    
    if verbose > 0:
        print(f"[{short_name}] INFO : Lancement de la recherche...")

    sql_result = search_musees(ville=ville, oeuvre=oeuvre, musee=musee, metier=metier, materiaux=materiaux, domaine=domaine, artiste=artiste, type_oeuvre=type_oeuvre, search_strategie=search_strategie, search_level=search_level, limit=limit, verbose=verbose)
    # sql_result = search_multicriteria(ville=ville, oeuvre=oeuvre, musee=musee, metier=metier, materiaux=materiaux, domaine=domaine, artiste=artiste, type_oeuvre=type_oeuvre, search_strategie=search_strategie, search_level=search_level, limit=limit, verbose=verbose)

    coord_lat       = input_datas.get('coord_lat', None)
    coord_long      = input_datas.get('coord_long', None)

    columns = ["museo" , "nom" , "ville" , "latitude", "longitude", "nb_oeuvres" , "nb_artistes", "distance"]

    df = convert_musee_search_result_to_df(sql_result=sql_result, columns =columns, coord_lat=coord_lat, coord_long=coord_long, verbose=verbose)
    return df


def _test_distance(verbose=1):
    short_name = "[TEST distance]"
    lannion = (48.7587757,-3.471542)
    st_malo = (48.649337,-2.025674)
    st_brieuc = (48.51418,-2.765835)
    brest = (48.390394,-4.486076)
    paris = (48.856614,2.3522219)
    to_test = {
        # Lannion - Lannion
        (DEFAULT_VALUES["coord_lat"], DEFAULT_VALUES["coord_long"], DEFAULT_VALUES["coord_lat"], DEFAULT_VALUES["coord_long"]) : 0.0,
        (lannion[0],lannion[1], lannion[0],lannion[1]) : 0.0,
        (st_malo[0],st_malo[1], st_malo[0],st_malo[1]) : 0.0,
        (st_malo[0],st_malo[1], lannion[0],lannion[1]) : 107.11407651053311, # 156.7 par la route, correcte à vol d'oiseau
        (st_malo[0],st_malo[1], st_brieuc[0],st_brieuc[1]) : 56.64109945832999,  # 92.9 par la route, correcte à vol d'oiseau
        (lannion[0],lannion[1], st_brieuc[0],st_brieuc[1]) : 58.69492626868507, 
        (lannion[0],lannion[1], brest[0],brest[1]) : 85.339063061735,
        (lannion[0],lannion[1], paris[0],paris[1]) : 427.8072018867463, 
    }

    for (lat_1, long_1 , lat_2 , long_2), expected in tqdm(to_test.items(), desc=short_name):
        dis = distance(lat_1, long_1 , lat_2 , long_2, verbose=verbose)
        assert dis == expected, f"{short_name} \t FAIL : {expected} expected and not {dis}"


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
    verbose = 1
    if verbose>0:
        _test_distance(verbose=verbose)

    print(f"[{short_name}]------------------------------------------------------ END")