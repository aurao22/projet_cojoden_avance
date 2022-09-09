# %% import
from os import getcwd
from os.path import join
import pandas as pd
import numpy as np
import re
from cojoden_functions import convert_df_string_to_search_string


# ---------------------------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------------------------

# %% extract_villes
def extract_villes(df, dest_path, dest_file_name='villes_departement_region_pays.csv',index=True, verbose=0):
    short_name = "extract_villes"
    
    df_cities = df[['geo_ville', 'geo_departement', 'geo_region']]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_cities.shape} on origin df")
    df_cities = df_cities.rename(columns={'geo_ville':'ville', 'geo_departement':'departement', 'geo_region':'region1'})
    df_cities = df_cities.drop_duplicates()
    df_cities = df_cities[df_cities['ville'].notna()]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_cities.shape} after drop duplicates and NA")

    # Correction de certaines valeurs
    df_cities.loc[(df_cities['ville'].notna()), 'ville'] = df_cities.loc[(df_cities['ville'].notna()), 'ville'].str.replace("Â", "")
    df_cities.loc[(df_cities['departement'].isna()) & (df_cities['ville']=='Tahiti'), 'departement'] = "Polynésie française"
    df_cities.loc[(df_cities['region1'].isna()) & (df_cities['ville']=='Tahiti'), 'region1'] = "Polynésie française"
    
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_cities.shape} after affect Polynésie française")

    df_cities["ville_search"] = df_cities["ville"]
    df_cities = convert_df_string_to_search_string(input_df=df_cities, col_name="ville_search", stop_word_to_remove=[])
    df_cities = df_cities[["ville_search", "ville", "departement", "region1"]]
    df_cities['NB_NAN'] = df_cities.isna().sum(axis=1)
    df_cities = df_cities.sort_values(by="NB_NAN")
    df_cities = df_cities.drop_duplicates(subset=['ville_search'], keep='first')

    if index:
        df_cities = df_cities.sort_values(by="ville_search")
        df_cities = df_cities.reset_index()
        df_cities = df_cities.drop(columns=['index', "NB_NAN"])
        df_cities = df_cities.reset_index()
        df_cities["index"] = df_cities["index"]+1
        df_cities = df_cities.rename(columns={"index":"id"})
        df_cities = df_cities.set_index("id")
        
    df_cities.to_csv(join(dest_path,dest_file_name), index=index)
    print(f"[{short_name}]\t INFO : {df_cities.shape} données sauvegardées in ------> {join(dest_path,dest_file_name)}")
    return df_cities

# %% extract_data
def extract_data(df, src_col_name, dest_col_name, clean_function, separator=";", dest_path="", dest_file_name=None, verbose=0):
    short_name = "extract_data"
    df_1 = df.copy()
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_1.shape} on origin df")
    df_1 = df_1[df_1[src_col_name].notna()]
    df_1 = df_1[[src_col_name]]
    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {df_1.shape} on origin not null")
    df_1 = df_1[df_1["auteur"].str.contains(separator)]
    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {df_1.shape} with parenthesis")
    
    df_1 = df_1["auteur"].str.split(separator, expand=True)
    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {df_1.shape} after split")
    cols = list(df_1.columns)
    df_2 = None
    for col in cols:
        df_1.loc[df_1[col].notna(),col]=df_1.loc[df_1[col].notna(),col].apply(lambda x: clean_function(input_str=x, verbose=verbose-1))
        if df_2 is None:
            df_2 = df_1[df_1[[0]].notna()]
            df_2= df_2[[0]]
        else:
            df_tp = df_1[df_1[col].notna()]
            df_tp = df_tp[[col]]
            df_tp = df_tp.rename(columns={col: 0})
            df_2 = pd.concat([df_2, df_tp], axis=0)
            if verbose > 1:
                print(f"[{short_name}]\t DEBUG : {df_2.shape} after add {col}")
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_2.shape} after add columns in rows")
    
    df_3 = df_2.copy()
    df_3["search"] = df_2[0]

    # Ajout de la colonne search et suppression des doublons
    df5 = convert_df_string_to_search_string(df_3, "search", stop_word_to_remove=[])
    df5 = df5[df5[0].notna()]
    df5 = df5.drop_duplicates(subset=["search"])
    df5 = df5.sort_values(by=['search'])
    df5 = df5.reset_index()
    df5 = df5[["search", 0]]
    df5 = df5.rename(columns={0:dest_col_name})
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df5.shape} after drop duplicates")

    if dest_file_name is not None:
        df5.to_csv(join(dest_path,dest_file_name), index=False)
        if verbose>0:
            print(f"[{short_name}]\t INFO : {df5.shape} données sauvegardées in ------> {join(dest_path,dest_file_name)}")

    return df5

# %% extract_musees
def extract_musees(df, dest_path, dest_file_name='musees.csv', verbose=0):
    short_name = "extract_musees"
    df_musee = df[['museo','nom_officiel_musee','geo_ville', 'latitude', 'longitude']]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_musee.shape} on origin df")
    df_musee = df_musee.drop_duplicates()
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_musee.shape} after remove duplicates")
    df_musee = df_musee[df_musee['museo'].notna()]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_musee.shape} after remove NA on museo")
    
    filtre = (df_musee['latitude']==0.0)&(df_musee['longitude']==0.0)

    # Remplacement des valeurs 0 par nan
    df_musee.loc[filtre,'latitude'] = np.nan
    df_musee.loc[filtre,'longitude'] = np.nan
    
    # Calcul des nan
    df_musee['NB_NAN_1'] = df_musee.isna().sum(axis=1)
    df_musee_sort = df_musee.sort_values(['NB_NAN_1', 'museo'])
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_musee_sort.shape} with NB_NAN columns")
    df_musee_sort = df_musee_sort.drop_duplicates(['museo'], keep='first')
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_musee_sort.shape} after drop duplicates on museo")
    
    df_musee_clean = df_musee_sort[['museo', 'nom_officiel_musee', 'geo_ville', 'latitude', 'longitude']]
    df_musee_clean = df_musee_clean.rename(columns={
        'geo_ville' : 'ville',
        'nom_officiel_musee' : 'nom',
    })
    
    df_musee_clean = df_musee_clean.sort_values(by=['ville', 'nom'])
    df_musee_clean = df_musee_clean.reset_index()
    df_musee_clean = df_musee_clean.drop(columns=['index'], axis=1)
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_musee_clean.shape} musees.")
    
    df_musee_clean.to_csv(join(dest_path,dest_file_name), index=False)
    print(f"[{short_name}]\t INFO : {df_musee_clean.shape} données sauvegardées in ------> {join(dest_path,dest_file_name)}")
    return df_musee_clean
    
# %% extract_artistes
def extract_artistes(df, dest_path, dest_file_name='artistes.csv', verbose=0):
    short_name = "extract_artistes"
    df_aut1 = df[['auteur']]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut1.shape} on origin df")
    df_aut1 = df_aut1.sort_values('auteur')
    df_aut1 = df_aut1.drop_duplicates()
    df_aut1 = df_aut1[df_aut1['auteur'].notna()]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut1.shape} without na and duplicates")
    
    # Suppression des termes spécifiques rencontrés lors de l'exploration des données
    to_replace = {
        '"' : "",
        "Établissements "       : "",
        "établissement "        : "",
        'Établissement '        : "",
        "\? (copie d\'après);"  : "",
        "? (d\'après)"          : "",
        'attribué Ã )'          :"",
        'atelier, genre de)'    :"",
        '?)'                    :"",
        "d'après)"              :"",
    }
    for str_1, str_2 in to_replace.items():
        df_aut1.loc[df_aut1['auteur'].notna(), 'auteur'] = df_aut1.loc[df_aut1['auteur'].notna(), 'auteur'].str.replace(str_1, str_2, regex=False)

    df_aut1.loc[df_aut1['auteur'].notna(), 'auteur'] = df_aut1.loc[df_aut1['auteur'].notna(), 'auteur'].str.strip()
    df_aut1.loc[(df_aut1['auteur'].notna()) & (df_aut1['auteur'])=='', 'auteur'] = np.nan
    df_aut1 = df_aut1[df_aut1['auteur'].notna()]
    df_aut1 = df_aut1.drop_duplicates()
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut1.shape} after remove specific words")

    df_aut2 = df_aut1['auteur'].dropna().str.split(r";", expand=True)
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut2.shape} after split ';'")
    
    # Extraction de tous les artistes de manière unique
    set_aut2 = None
    for col in df_aut2.columns:
        if set_aut2 is None:
            set_aut2 = set(df_aut2[col].dropna().unique())
        else:
            for v in df_aut2[col].dropna().unique():
                if len(v)>0:
                    set_aut2.add(v)
    set_aut2.remove('')
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {len(set_aut2)} unique after extraction")
    
    # Création d'une DF avec le set
    auth3 = pd.DataFrame(set_aut2)
    df_aut4 = auth3[0].dropna().str.split(r"(", expand=True)
    df_aut4 = df_aut4.sort_values(0)
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut4.shape} after split '('")
    
    # Nettoyage des données
    df_aut4['nom_naissance'] =df_aut4[0]
    df_aut4['dit'] =np.nan

    dit_2_bool = (df_aut4[1].notna()) & ((df_aut4[1].str.contains('dit)', regex=False)) | (df_aut4[1].str.contains('dite)', regex=False)))
    df_aut4.loc[dit_2_bool, 'dit'] = df_aut4.loc[dit_2_bool, 0]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut4[dit_2_bool].shape} artistes with 'dit' name")
    
    ne_2_bool = (df_aut4[1].notna()) & ((df_aut4[2].str.contains('né)', regex=False)) | (df_aut4[2].str.contains('née)', regex=False)))
    df_aut4.loc[ne_2_bool, 'nom_naissance'] = df_aut4.loc[ne_2_bool, 1].str.replace('dit), ', '', regex=False)
    df_aut4.loc[ne_2_bool, 'nom_naissance'] = df_aut4.loc[ne_2_bool, 'nom_naissance'].str.replace('dite), ', '', regex=False)
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut4[ne_2_bool].shape} artistes with 'née' or 'né' name")
    
    # Suppression des valeurs traitées
    df_aut4.loc[df_aut4[1]=='dit)', 1]=np.nan
    df_aut4.loc[df_aut4[1]=='dite)', 1]=np.nan
    df_aut4.loc[df_aut4[1]==df_aut4.loc[8115, 1], 1]=np.nan
    df_aut4.loc[df_aut4['nom_naissance']==df_aut4.loc[8115, 'nom_naissance'], 'nom_naissance']=df_aut4.loc[df_aut4['nom_naissance']==df_aut4.loc[8115, 'nom_naissance'], 0]
    df_aut4.loc[df_aut4[1]=='dit\)', 1]=np.nan
    df_aut4.loc[df_aut4[1]=='d’après, dit)', 1]=np.nan
    df_aut4.loc[df_aut4[2]=='née)', 2]=np.nan
    df_aut4.loc[df_aut4[2]==df_aut4.loc[43882, 2], 2]=np.nan
    df_aut4.loc[df_aut4[2]==df_aut4.loc[22108, 2], 2]=np.nan
    df_aut4.loc[df_aut4[2]=='née\)', 2]=np.nan
    df_aut4.loc[df_aut4[2]=='né\)', 2]=np.nan
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut4.shape} artistes after proceed dit and né")
        print(f"[{short_name}]\t INFO : {df_aut4.isna().sum()} NA after proceed dit and né")
    
    # 
    df_aut5 = df_aut4[['nom_naissance', 'dit', 0, 1, 2, 3]]
    df_aut5 = df_aut5.sort_values('nom_naissance')

    to_replace = {
        'attribué Ã )':"",
        'atelier, genre de)':"",
        '?)':"",
        "d'après)":"",
    }
    for str_1, str_2 in to_replace.items():
        for col in [0,1,2,3]:
            df_aut5.loc[df_aut5[col].notna(), col] = df_aut5.loc[df_aut5[col].notna(), col].str.replace(str_1, str_2, regex=False)

    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut5.shape} artistes after proceed specific string to replace")

    for col in ["nom_naissance", "dit", 0,1,2,3]:
        if verbose > 0:
            print(f"[{short_name}]\t INFO : {col}={df_aut5[col].isna().sum()} nan before ", end="")
        if col not in ["nom_naissance", "dit"]:
        # df_aut5.loc[(df_aut5[col].notna()) & (df_aut4.loc[55537, 1]==df_aut5[col]), col] = np.nan
            df_aut5.loc[(df_aut5[col].notna()) & (df_aut5['nom_naissance']==df_aut5[col]), col] = np.nan
            df_aut5.loc[(df_aut5[col].notna()) & (df_aut5['dit']==df_aut5[col]), col] = np.nan
        df_aut5.loc[(df_aut5[col].notna()), col] = df_aut5.loc[(df_aut5[col].notna()), col].str.strip()
        df_aut5.loc[(df_aut5[col].notna()) & (df_aut5[col].str.len()==0), col] = np.nan
        if verbose > 0:
            print(f"and {df_aut5[col].isna().sum()} nan after")
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut5.shape} artistes after cleaning same name and dit name")
    
    df_aut6 = df_aut5[~((df_aut5["nom_naissance"].isna()) & (df_aut5["dit"].isna()))]
    df_aut6 = df_aut6[['nom_naissance', 'dit', 1, 2, 3]]
    df_aut6 = df_aut6.rename(columns={
        1 : "metier"
    })
    df_aut6 = df_aut6.drop_duplicates()
    df_aut6.loc[(df_aut6["metier"].notna()) & (df_aut6["metier"].str.startswith("dit)")), "metier"] = np.nan
    df_aut6.loc[(df_aut6["metier"].notna()) & (df_aut6["metier"].str.startswith("dite)")), "metier"] = np.nan
    df_aut6 = df_aut6.drop_duplicates()
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut6.shape} artistes cleaning dit and dite and drop duplicates")
    
    # A ce stade il manquait des artistes, il a donc fallu les extraires en ligne de commande pour les ajouter à la DF.
    df_aut7 = _add_missing_artists(df_aut6.copy(), verbose=verbose-1)
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut7.shape} artistes after adding missing artistes")  

    df_aut7 = df_aut7.sort_values('nom_naissance')
    to_replace = {
        'attribué Ã )':"",
        'attribué à)':"",
        'atelier, genre de)':"",
        '?)':"",
        "d'après)":"",
        'dit)':'',
        ', LE GUERCHIN':'',
        'dit), GUERCINO':'',
        's))':'',
    }
    for str_1, str_2 in to_replace.items():
        for col in ['metier', 2,3]:
            df_aut7.loc[df_aut7[col].notna(), col] = df_aut7.loc[df_aut7[col].notna(), col].str.replace(str_1, str_2, regex=False)
            df_aut7.loc[df_aut7[col].notna(), col] = df_aut7.loc[df_aut7[col].notna(), col].str.replace(")", "", regex=False)

    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_aut7.shape} artistes cleaning in other columns")  

    for col in ["nom_naissance", "dit",'metier', 2,3]:
        if verbose > 0:
            print(f"[{short_name}]\t INFO :{col}={df_aut7[col].isna().sum()} nan before ", end="")
        if col not in ["nom_naissance", "dit"]:
            df_aut7.loc[(df_aut7[col].notna()) & (df_aut7['nom_naissance']==df_aut7[col]), col] = np.nan
            df_aut7.loc[(df_aut7[col].notna()) & (df_aut7['dit']==df_aut7[col]), col] = np.nan
        df_aut7.loc[(df_aut7[col].notna()), col] = df_aut7.loc[(df_aut7[col].notna()), col].str.strip()
        df_aut7.loc[(df_aut7[col].notna()) & (df_aut7[col].str.len()==0), col] = np.nan
        if verbose > 0: print(f"and {df_aut7[col].isna().sum()} nan after")

    df_aut7 = df_aut7.reset_index()
    df_aut7 = df_aut7.drop('index', axis=1)
    # Suppression des 7 premières lignes qui ne sont pas des auteurs
    df_aut7 = df_aut7[7:]
    df_aut8 = df_aut7.drop(range(8, 17), axis=0)
    if verbose > 0:
            print(f"[{short_name}]\t INFO :{df_aut8.shape}")

    df_aut8.loc[(df_aut8["nom_naissance"].notna()),"nom_naissance"] = df_aut8.loc[(df_aut8["nom_naissance"].notna()),"nom_naissance"].str.replace("« ", "", regex=False)
    df_aut8.loc[(df_aut8["nom_naissance"].notna()),"nom_naissance"] = df_aut8.loc[(df_aut8["nom_naissance"].notna()),"nom_naissance"].str.replace(" »", "", regex=False)
    df_aut10 = df_aut8.drop_duplicates(["nom_naissance", "dit"])
    if verbose > 0:
            print(f"[{short_name}]\t INFO :{df_aut10.shape}")

    df_aut10["upper_search_name"] = df_aut10["upper_name"]
    df_aut10 = convert_df_string_to_search_string(input_df=df_aut10, col_name="upper_search_name")
    if verbose > 0:
        print(f"[{short_name}]\t INFO :{df_aut10.shape}")
    df_aut10 = df_aut10.sort_values(["upper_search_name", 'NB_NAN'])
    df_aut10 = df_aut10.drop_duplicates(['upper_search_name'], keep='first')
    if verbose > 0:
        print(f"[{short_name}]\t INFO :{df_aut10.shape}")
    df_aut10 = df_aut10.sort_values(["upper_search_name", 'NB_NAN'])
    df_aut10 = df_aut10.drop_duplicates(['upper_search_name'], keep='first')
    if verbose > 0:
        print(f"[{short_name}]\t INFO :{df_aut10.shape}")
    
    df_aut10.to_csv(join(dest_path,dest_file_name), index=False)
    print(f"[{short_name}]\t INFO :{df_aut10.shape} données sauvegardées in ------> {join(dest_path,dest_file_name)}")
    return df_aut10


# %% extract_materiaux_technique
def extract_materiaux_technique(df, dest_path, dest_file_name='materiaux_techniques.csv', verbose=0):
    df1 = df[['materiaux_technique']]
    if verbose > 0:
        print(f"[extract_materiaux_technique]\t INFO : {df1.shape} on origin df")
    df1 = df1.sort_values('materiaux_technique')
    df1 = df1.drop_duplicates()
    # df_aut1['auteur'] = df_aut1['auteur'].fillna(df_aut1['auteur_precisions'])
    df1 = df1[df1['materiaux_technique'].notna()]
    if verbose > 0:
        print(f"[extract_materiaux_technique]\t INFO : {df1.shape} without na and duplicates")
    
    # Séparation des matériaux
    df2 = df1[['materiaux_technique']]
    for sep in [', ',',', ';', " (", '/', '.', ' : ']:
        df2 = df2['materiaux_technique'].dropna().str.split(sep, expand=True, regex=False)
        set_mat1 = set()
        for col in df2.columns:
            for v in df2[col].dropna().unique():
                if len(v)>1 and v !='?))'and v !='?)':
                    v2 = v.replace(')', '')
                    v2 = v2.replace('(', ' ')
                    v2 = v2.replace(':', ' ')
                    v2 = v2.replace('Ã ', 'à')
                    v2 = v2.replace(' ?', ' ')
                    v2 = v2.replace('?', ' ')
                    v2 = v2.replace('   ', ' ')
                    v2 = v2.replace('  ', ' ')
                    try:
                        int(v2)
                    except:
                        v2 = v2.strip()
                        if len(v2)>1:
                            set_mat1.add(v2)
        try:
            set_mat1.remove('')
        except:
            pass
        df2 = pd.DataFrame(set_mat1, columns=['materiaux_technique'])

    # Création du nom pour les recherches
    df_fin = df2.copy()
    df_fin["mat_search"] = df_fin['materiaux_technique']
    df_fin = convert_df_string_to_search_string(input_df=df_fin, col_name="mat_search")

    if verbose > 0:
        print(f"[extract_materiaux_technique]\t INFO : {df_fin.shape}")
    df_fin = df_fin.sort_values(["mat_search"])
    df_fin = df_fin.drop_duplicates(['mat_search'], keep='first')
    if verbose > 0:
        print(f"[extract_materiaux_technique]\t INFO : {df_fin.shape}")

    df_fin.to_csv(join(dest_path,dest_file_name), index=False)
    print(f"[extract_materiaux_technique]\t INFO : {df_fin.shape} données sauvegardées in ------> {join(dest_path,dest_file_name)}")
    return df_fin



# %% extract_oeuvres
def extract_oeuvres(df, dest_path, dest_file_name='oeuvres.csv', verbose=0):
    # `ref` VARCHAR(100) NOT NULL,
    # `titre` VARCHAR(1000) NULL DEFAULT NULL,
    # `type` VARCHAR(100) NULL DEFAULT NULL,
    # `domaine` VARCHAR(100) NULL DEFAULT NULL,
    # `texte` TEXT(1000) NULL DEFAULT NULL,
    # `lieux_conservation` VARCHAR(100) NOT NULL,
    # `creation_millesime` VARCHAR(100) NULL DEFAULT NULL,
    # `annee_debut` VARCHAR(45) NULL DEFAULT NULL,
    # `annee_fin` VARCHAR(45) NULL DEFAULT NULL,
    # `inscriptions` TEXT NULL DEFAULT NULL,
    # `commentaires` TEXT NULL DEFAULT NULL,
    # `largeur_cm` INT(11) NULL DEFAULT NULL,
    # `hauteur_cm` INT(11) NULL DEFAULT NULL,
    # `profondeur_cm` INT(11) NULL DEFAULT NULL,
    # `creation_lieux` VARCHAR(100) NULL DEFAULT NULL,
    # PRIMARY KEY (`ref`)
    # ------------------------------------
    # TODO Statut
    # `statut` VARCHAR(45) NULL DEFAULT NULL,
    # `annee_debut` VARCHAR(45) NULL DEFAULT NULL,
    # `annee_fin` VARCHAR(45) NULL DEFAULT NULL,
    short_name = "extract_oeuvres"
    df1 = df[['ref', 'titre','type_oeuvre', 'domaine', 'texte', 'museo', 'creation_millesime','inscription_precisions', 'commentaires', 'dimensions', 'creation_lieux',
        'auteur', 'materiaux_technique']]
    
    # Pour correspondre aux champs de la BDD
    df1 = df1.rename(columns={'museo':'lieux_conservation',
                              'type_oeuvre':'type',
                             'inscription_precisions':'inscriptions'})
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df1.shape} on origin df")
    df1 = df1.sort_values('ref')
    
    # Il faut récupérer les dimensions
    # `largeur_cm` INT(11) NULL DEFAULT NULL, `hauteur_cm` INT(11) NULL DEFAULT NULL, `profondeur_cm` INT(11) NULL DEFAULT NULL,
    df1["dimensions_clean"] = np.nan
    df1.loc[df1["dimensions"].notna(),"dimensions_clean"] = df1.loc[df1["dimensions"].notna(),"dimensions"].apply(lambda x: standardize_dimension(str_dim=x, verbose=verbose))
    i = 0
    for col in ["largeur_cm", "hauteur_cm", "profondeur_cm"]:
        df1.loc[df1["dimensions_clean"].notna(),col] = df1.loc[df1["dimensions_clean"].notna(),"dimensions_clean"].apply(lambda x: _convert_to_float(x[i]))
        df1[col] = df1[col].fillna(0)
        try:
            df1[col] = df1[col].astype(float)
        except Exception as error:
            print(error)
        i += 1

    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df1.shape} without na and duplicates")
    # TODO vérifier les traitements

    #Suppression des colonnes non utilisées
    df_fin = df1[['ref', 'titre', 'type', 'domaine', 'texte', 'lieux_conservation', 
        'annee_debut', 'annee_fin', 'inscriptions', 'commentaires',
        'largeur_cm', 'hauteur_cm', 'profondeur_cm', 'creation_lieux']]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df_fin.shape}")

    df_fin.to_csv(join(dest_path,dest_file_name), index=False)
    print(f"[{short_name}]\t INFO : {df_fin.shape} données sauvegardées in ------> {join(dest_path,dest_file_name)}")
    return df_fin

# %% extract_creation_oeuvres
def extract_creation_oeuvres(df, dest_path, dest_file_name='creation_oeuvres.csv', verbose=0):
    short_name = "extract_creation_oeuvres"
    df1 = df[['ref', 'auteur']]
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df1.shape} on origin df")
    
    df1.loc[df1["auteur"].notna(),'auteur_list'] =  df1.loc[df1["auteur"].notna(),'auteur'].apply(lambda x: clean_artiste(input_artiste=x, clean_dit_at_begin=True, join=True, verbose=verbose-1))
    df1['auteur_list2'] = df1['auteur_list']
    df1.loc[df1["auteur"].notna(),'auteur_list']
    df2 = df1.loc[df1['auteur_list2'].notna(), 'auteur_list2'].str.split(r";", expand=True)
    df3 = pd.merge(df1,df2, left_index=True, right_index=True)

    cols = list(df3.columns)
    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {cols}")
    df4 = df3[['ref', 'auteur', 'auteur_list', 0]]
    cols.remove('ref')
    cols.remove('auteur')
    cols.remove('auteur_list')
    cols.remove('auteur_list2')
    cols.remove(0)
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {cols}")

    if verbose > 0:
            print(f"[{short_name}]\t INFO : {df4.shape} on origin df")
    for col in cols:
        dft = df3[['ref', 'auteur', 'auteur_list', col]]
        dft = dft[dft[col].notna()]
        dft = dft.rename(columns={col: 0})
        df4 = pd.concat([df4, dft], axis=0)
        if verbose > 1:
            print(f"[{short_name}]\t DEBUG : {df4.shape} after add {col}")

    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df4.shape} with artistes")

    df4.loc[(df4['auteur'].notna()) &(df4['auteur'].str.contains("(", regex=False)), "metier"] = df4.loc[(df4['auteur'].notna()) &(df4['auteur'].str.contains("(", regex=False)), ["auteur", 0]].apply(lambda x: extract_metier_for_artiste_and_oeuvre(str_line=x["auteur"], auteur_name=x[0], verbose=verbose-1), axis=1)

    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df4.shape} with metier by artiste")

    df5 = df4.drop_duplicates()
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df5.shape} after drop duplicates")

    # alignement avec la BDD
    df5 = df5[[0,'ref', 'metier']]
    df5 = df5.rename(columns={
        "ref":"oeuvre",
        0:"artiste",
        'metier':'role',
        }
        )
    df5 = convert_df_string_to_search_string(df5, "artiste", stop_word_to_remove=[])
    df5 = df5.drop_duplicates()
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df5.shape} after drop duplicates")

    df_creation = df5.copy()
    df_creation.to_csv(join(dest_path,dest_file_name), index=False)
    if verbose>0:
        print(f"[{short_name}]\t INFO : {df_creation.shape} données sauvegardées in ------> {join(dest_path,dest_file_name)}")
    
    return df_creation

# ----------------------------------------------------------------------------------
# PRIVATE FUNCTIONS - Text generation to fill na values
# ----------------------------------------------------------------------------------
# %% extract_metier_for_artiste_and_oeuvre
def extract_metier_for_artiste_and_oeuvre(str_line, auteur_name,verbose=0):
    res = np.nan
    if auteur_name is not None and len(auteur_name)>0:
        str_rest = str_line.split(auteur_name) 
        str_rest = str_rest[-1].strip()
        if str_rest.startswith("("):
            str_res = str_rest.split(")")
            for s in str_res[0:2]:
                s2 = s.replace("(", "") 
                s2 = s2.strip()
                if len(s2)>3:
                    return s2
    return res

# %% clean_artiste
def clean_artiste(input_artiste, clean_dit_at_begin=True, join=True, verbose=0):
    short_name = "clean_artiste"
    res = []
    proc_str = input_artiste
    # Suppression des termes spécifiques rencontrés lors de l'exploration des données
    to_replace = {
        '"' : "",
        "établissement " : "",
        'Établissement ' : "",
        "Établissements ": "",
        # '(fabricant)' : "",
        # '(imprimeur)': "",
        # '(constructeur)': "",
        # '(émetteur)': "",
        "\? (copie d\'après);": "",
        "? (d\'après)": "",
        'attribué Ã )':"",
        'atelier, genre de)':"",
        '?)':"",
        "d'après)":"",
    }
    if clean_dit_at_begin:
        to_replace['dit)']=""
        to_replace['dite)']=""
        to_replace['né)']=""
        to_replace['née)']=""
        to_replace['dit\)']=""
        to_replace['d’après, dit)']=""
        to_replace['née\)']=""
        to_replace['né\)']=""
        to_replace['attribué Ã )']=""
        to_replace['attribué à)']=""
        to_replace['atelier, genre de)']=""
        to_replace['?)']=""
        to_replace[ "d'après)"]=""
        to_replace['dit)']=""
        to_replace[', LE GUERCHIN']=""
        to_replace['dit), GUERCINO']=""
        to_replace['s))']=""
        
    for str_1, str_2 in to_replace.items():
        proc_str = proc_str.replace(str_1, str_2)

    proc_str = proc_str.strip()
    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {input_artiste} clean to became {proc_str}")

    proc_list = proc_str.split(";")
    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {input_artiste} split to {proc_list}")
    for tp in proc_list:
        # Ajout des noms
        name = tp.split("(")
        res.append(name[0].strip())

    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {input_artiste} became {res} after splits")  

    if verbose > 0:
        print(f"[{short_name}]\t INFO :{input_artiste} became ------> {res}")
    if join:
        return ";".join(res)
    return res

# %% clean_metier
def clean_metier(input_str, verbose=0):
    res = np.nan
    if input_str is not None:
        sp = input_str.split(")")[0]
        sp = sp.split("(")
        if len(sp)>1:
            res = sp[1].strip()
            # Permet de filtrer les particules
            seps = [";",","]
            for sep in seps:
                res = res.split(sep)[0].strip()
            
            if re.search(r'([0-9]+)', res) is not None:
                res = np.nan

    if res is not None and isinstance(res, str):
        if len(res)<4:
            res = np.nan
        else:
            exclude_list = ["manière","après", "agence", "usine", "attribu","attibu","attr.","chantier","bureau","tabac", "autour",
            "Ancienne Maison", "anciennement attribué à", "anonyme", "divinisé", "Antonia", "atelier", "COMPAGNIE", "école", "édition", "établissement", 
            "fabrique", "FAUX", "faïencerie", "FAIENCERIES", "famille", "FAMILLLE", "épouse", "entreprise", "Mère", "genre de", "imitation", "inspiré", "laboratoire", "père", "patronyme", "style", 
            "casino", "bazar", "Beaucourt", "Bernard", "Besançon", "Félix", "KLINGER", "Saecularis", "collection", "Collectivité", "COMBIER", "commencé", "dit ", "dite",
            "ECOLE", "élève","ELEVE", "enfant", "Eug.", "entourage", "fils", "Faustine", "firme", "FLAVIUS", "France", "François", "fratelli", "Frères", "RICHARD", "Giandomenico"
            "fours", "G.S.", "Giandomenico", "Groupe", "H.M.", "Italie", "J.R.D.", "JDEP", "Baptiste", "Jules","Justin", "KRATZ", "LANTZ", "JEUNE", "LEGROS", "Léon",
            "MAGNUS", "maison", "manufacture", " par", "OU ", "Paris", "Pauline", "pour ", "pseudonyme", "QING", "R.M.", "restaurant", "RICCA", "robert", "S.C.",
            "S.F.B.J.", "S.N.F.", "Samuel", "société", "soeur", "suite de", "TANG", "Tarn", "cercle de", "culture", "Drusus", "Londres", "M.F.", "CAESONIA", "Octave",
            "pastiche","République", "suiveur", "MULLER", "Miss]", "Mariniane","marque","VALENTINIANUS", "anciennement",
            ]
            for to_exclude in exclude_list:
                if to_exclude.lower() in res.lower():
                    res = np.nan
                    break
        
    return res


# %% _convert_to_float
def _convert_to_float(str_):
    res = 0
    to_proc = str_
    try:
        if isinstance(to_proc, str):
            to_proc = to_proc.replace(",", ".")
            to_proc = to_proc.replace("..", ".")

        res = float(to_proc)
    except:
        res = 0

    return res




# %% _add_missing_artists
def _add_missing_artists(df, verbose=0):
    # il manquait des artistes, il a donc fallu les extraires en ligne de commande pour les ajouter à la DF.
    # Le fichier name étant une simple sauvegarde de la DF
    # Ligne de commande correspondante : grep "'e : .*'" name.txt
    to_add = [("Quagliozzi","Aurélien"),
            ("Romon","Anthony"),
            ("da Rocha","François"),
            ("Dumont","Stéphane"),
            ("Collot","Patrick"),
            ("Legrand","Romain"),
            ("Defeyer","Jean-Baptiste"),
            ("Faroux","Thomas"),
            ("Giauffret","Michel"),
            ("Mattio","Jean-Philippe"),
            ("Dupont","Isabelle"),
            ("Codron","Anthony"),
            ("Courmont","Jean-Marc"),
            ("Bohée","Francis"),
            ("Cubilier","Eric"),
            ("Roger","Mylène"),
            ("Dattola","Rina"),
            ("Leroux","Rudy"),
            ("Bovis","Nadine"),
            ("Raymond","Alexis"),
            ("Jauregui","Nicolas"),
            ("Lepage","Sandra"),
            ("Delecroix","Elsa"),
            ("Denis","Daniel"),
            ("Borla","Audrey"),
            ("Stepaniak","Philippe"),
            ("Farineau","Typhanie"),
            ("Singer","Roland"),
            ("Lavagna","Richard"),
            ("Lavagna","Sabine"),
            ("Noseda","Veronica"),
            ("Collomb","Marie-Caroline"),
            ("Bertino","Eric"),
            ("Marissael","Pascal"),
            ("Desmaretz","Arnaud"),
            ("Capelain","Jean"),
            ("Bougaret","Eric"),
            ("Resegotti","Robert"),
            ("Lis","Damien"),
            ("Duhomez","Marcel")]

    for (nom, prenom) in to_add:
        new_row = {'nom_naissance':nom.upper()+' '+prenom, 'dit':np.nan, 'metier':np.nan, 2:np.nan,3:np.nan}
        #append row to the dataframe
        df = df.append(new_row, ignore_index=True)
    return df

# ----------------------------------------------------------------------------------
#                        TEST
# ----------------------------------------------------------------------------------
# %% _test_millesime
def _test_millesime(verbose=1):
    test_millesime = [
        "1890 entre;1908 et",
        "1879 juin",
        "1862 après",
        "1880 vers",
        "1851",
        "1832/12",
        "1920 vers",
        '20 février 1892',
    ]
    for t in test_millesime:
        print(f'{t} => {standardize_creation_millesime(t, verbose=verbose)}')

# %% _test_standardize_dimensions
def _test_standardize_dimensions(verbose=1):
    print(standardize_dimension(" l. 30 cm ; H. 65 cm ; P. 22 cm ; VOLUM. 0,0429", verbose=verbose))
    print(standardize_dimension(" l. 30 CM ; H. 62 CM ; P. 23 CM ; VOLUM. 0,0428", verbose=verbose))
    print(standardize_dimension("Hauteur en cm 9.4 ; Largeur en cm 12.8", verbose=verbose))
    print(standardize_dimension("Hauteur en cm 17,2 ; Diamètre en cm 9.3", verbose=verbose))

# %% _test_clan_artiste
def _test_clan_artiste(verbose=1):
    auteurs_to_test = [
        "LE GULUCHE Joseph Marie",
        'THOMSEN Constant',
        'FJERDINGSTAD Christian (orfèvre)',
        'DUTHOIT Louis',
        'DUTHOIT Aimé;DUTHOIT Louis',
    ]
    for t in auteurs_to_test:
        print(clean_artiste(t, verbose=verbose))

# %% _test_extract_metier_for_artiste_and_oeuvre
def _test_extract_metier_for_artiste_and_oeuvre(verbose=1):
    to_test = {
        "SAINT-GERMAIN Marguerite (De) (céramiste)":"SAINT-GERMAIN Marguerite",
        "LAMOISSE Eugène (dessinateur)":"LAMOISSE Eugène",
        "FJERDINGSTAD Christian (orfèvre)":"FJERDINGSTAD Christian",
        "DUTHOIT Aimé;DUTHOIT Louis":"DUTHOIT Aimé",
        "DUTHOIT Aimé;DUTHOIT Louis":"DUTHOIT Louis",
    }

    for str_line, auteur_name in to_test.items():
        print(extract_metier_for_artiste_and_oeuvre(str_line, auteur_name, verbose=verbose))

# %% _test_clean_metier
def _test_clean_metier(verbose=1):
    to_test = {
        "SAINT-GERMAIN Marguerite (De) (céramiste)":"SAINT-GERMAIN Marguerite",
        "LAMOISSE Eugène (dessinateur)":"LAMOISSE Eugène",
        "FJERDINGSTAD Christian (orfèvre)":"FJERDINGSTAD Christian",
        "DUTHOIT Aimé;DUTHOIT Louis":"DUTHOIT Aimé",
        "DUTHOIT Aimé;DUTHOIT Louis":"DUTHOIT Louis",
    }

    for str_line, auteur_name in to_test.items():
        print(clean_metier(str_line, verbose=verbose))


# %% test
def test(verbose=1):
    _test_millesime(verbose=verbose)
    _test_standardize_dimensions(verbose=verbose)
    _test_clan_artiste(verbose=verbose)
    _test_extract_metier_for_artiste_and_oeuvre(verbose=verbose)
    _test_clean_metier(verbose=verbose)

# ----------------------------------------------------------------------------------
#                        MAIN
# ----------------------------------------------------------------------------------
# %% main
if __name__ == '__main__':
    verbose = 1
    run_extraction = 0

    # Récupère le répertoire du programme
    file_path = getcwd() + "\\"
    if "PROJETS" not in file_path:
        file_path = join(file_path, "PROJETS")
    if "projet_joconde" not in file_path:
        file_path = join(file_path, "projet_joconde")
    
    data_set_path = join(file_path , "dataset\\")
    data_set_file_name = "base-joconde-extrait.csv"

    print(f"Current execution path : {file_path}")
    print(f"Dataset path : {data_set_path}")

    # Chargement et nettoyage général
    df_origin = load_data(data_set_path=data_set_path,data_set_file_name=data_set_file_name, verbose=verbose)
    df_encode = proceed_encoding(df_origin, verbose=verbose)
    df_clean = proceed_duplicated(df_encode, verbose=verbose)
    df_clean_na = proceed_na_values(df_clean, verbose=verbose)

    if verbose > 1:
        figure, ax = color_graph_background(1,1)
        sns.heatmap(df_clean_na.isnull(), yticklabels=False,cbar=False, cmap='viridis')
        plt.xticks(rotation=45, ha="right", fontsize=10)
        plt.title("NA dans la DF après traitement.")
        figure.set_size_inches(18, 5, forward=True)
        plt.show()

    # extraction des données
    if run_extraction > 0:
        df_villes = extract_villes(df=df_clean_na, dest_path=data_set_path, dest_file_name='villes_departement_region_pays.csv',verbose=verbose)
        df_musees = extract_musees(df=df_clean_na, dest_path=data_set_path, dest_file_name='musees.csv', verbose=verbose)
        df_artistes = extract_artistes(df=df_clean_na, dest_path=data_set_path, dest_file_name='artistes.csv', verbose=verbose)
        df_materiaux = extract_materiaux_technique(df=df_clean_na, dest_path=data_set_path, dest_file_name='materiaux_techniques.csv', verbose=verbose)
    
    
