# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Module to clean the input dataset

Project: Cojoden avance
=======

Usage:
======
    python cojoden_nettoyage.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"

# %% import
from os import getcwd
from os.path import join
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import csv
import re
from IPython.core.display import HTML
import sys
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from data_preprocessing.cojoden_functions import color_graph_background


# ---------------------------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------------------------
# %% load_data
def load_data(data_set_path,data_set_file_name,nrows = 0, skiprows = 0, verbose=0):
    """Load datas with less than 75% of NaN values from the dataset file

    Args:
        data_set_path (str): the dataset path
        data_set_file_name (str): the dataset file name
        nrows (int, optional): the number of skipped row. Defaults to 0.
        skiprows (int, optional): the nyumber of skipped columns. Defaults to 0.
        verbose (int, optional): Log level. Defaults to 0.

    Returns:
        DataFrame: Loaded datas
    """
    
    usecols = ['ref', 'pop_coordonnees', 
        'autr', 'bibl', 'comm', 'deno', 'desc',
        'dims', 'domn', 'dpt', 'ecol', 
        'hist',  'lieux', 'loca', 'loca2', 'mill', 'nomoff', 
        'paut', 'peri', 'pins', 'prep', 
        'region', 'repr',  'tech', 'titr', 'ville_', 'museo']

    # Suppression des colonnes avec plus de 75% de NAN 
    # 'adpt','nsda','geohi', 'manquant', 'manquant_com', 'milu','onom','refmem', 'refmer','refpal', 'retif',
    # 'peoc','peru','plieux','refmis','srep','attr','puti', 'ddpt','pdec','appl','drep','depo','epoq','decv',
    # 'expo', 'etat', 'gene', 'larc', 

    # Suppression des colonnes non nécessaire :
    # 'www','base','contient_image', 'dacq','dmaj','copy','pop_contient_geolocalisation','aptn', 
    # 'dmis', 'msgcom','museo', 'producteur', 'image','inv', 'phot', 'stat', 'label', 'historique','util','insc',

    df_origin = None
    if nrows > 0:
        print("(", skiprows, "to", nrows,"rows)")
        df_origin = pd.read_csv(join(data_set_path,data_set_file_name), skiprows=skiprows, quoting=csv.QUOTE_NONNUMERIC, sep=';', low_memory=False, usecols=usecols)
    else:
        df_origin = pd.read_csv(join(data_set_path,data_set_file_name), quoting=csv.QUOTE_NONNUMERIC, sep=';', low_memory=False, usecols=usecols)

    print(f"{df_origin.shape} données chargées ------> {list(df_origin.columns)}")

    to_rename = {
        'autr':'auteur',
        'comm':'commentaires', 
        'deno':'type_oeuvre', 
        'desc':'description',
        'dims':'dimensions', 
        'domn':'domaine', 
        'dpt':'geo_departement', 
        'ecol':'geo_ecole_pays', 
        'lieux':'creation_lieux', 
        'loca':'lieux_conservation', 
        'loca2':'geo_pays_region_ville', 
        'mill':'creation_millesime', 
        'nomoff':'nom_officiel_musee', 
        'paut':'auteur_precisions', 
        'peri':'creation_periode', 
        'pins':'inscription_precisions',
        'prep':'sujet_precisions', 
        'repr':'sujet',  
        'tech':'materiaux_technique',
        'titr':'titre',
        'ville_':'geo_ville',
        'region' : 'geo_region',
        # 'bibl':'bibliographie',
        # 'insc':'inscription',
        # 'inv':'no_inventaire', 
        # 'label':'label_musee_fr',  
        # 'phot':'credit_photo', 
        # 'producteur':'data_producteur', 
        # 'stat':'statut_juridique', 
        # 'util':'utilisation', 
    }

    df_origin = df_origin.rename(columns=to_rename)
    if verbose > 1:
        print(list(df_origin.columns))

    if verbose > 1:
        figure, _ = color_graph_background(1,1)
        sns.heatmap(df_origin.isnull(), yticklabels=False,cbar=False, cmap='viridis')
        plt.xticks(rotation=45, ha="right", fontsize=10)
        plt.title("NA dans la DF")
        figure.set_size_inches(18, 5, forward=True)
        plt.show()
        display(HTML(df_origin.head().to_html()))
    return df_origin

# %% proceed_encoding
def proceed_encoding(df_origin, verbose=0):
    # L'encodage a été traité directement en ligne de commands dans le fichier de données sources.
    # Cf. script proceed_encoding.sh
    for col in df_origin.columns:
        no_na = df_origin[col].notna()
        df_origin.loc[no_na, col] = df_origin.loc[no_na, col].str.replace('Ã  ', 'à ')
    return df_origin

# %% proceed_duplicated
def proceed_duplicated(df_origin, verbose=0):
    if verbose > 0:
        print(f"[proceed_duplicated]\t INFO : Before {df_origin.shape}")
    # Suppression des 2246 rows strictement identiques
    df_clean = df_origin.drop_duplicates()

    # Suppression des doublons sur la référence
    df_clean = df_clean.drop_duplicates(subset=['ref'])
    if verbose > 0:
        print(f"[proceed_duplicated]\t INFO : after {df_clean.shape}")
    return df_clean

# %% proceed_na_values
def proceed_na_values(df_origin, verbose=0):
    """Processing the NaN values :
    - Replace (Sans titre) with NaN value
    - Row title less : Generate a title with the other columns 
    - Row type oeuvre less : Extract the type from other columns
    - Text column : create the column with merge of 'sujet_precisions', "description", 'sujet'

    Args:
        df_origin (DataFrame): The source dataframe
        verbose (int, optional): Log Level. Defaults to 0.

    Returns:
        DataFrame: A new DataFrame updated
    """
    df_clean = df_origin.copy()

    # Affectation de NA aux oeuvres qui n'ont pas de titre
    if verbose > 0:
        print(f"[proceed_na_values]\t INFO : {df_clean[df_clean['titre']=='(Sans titre)'].shape} oeuvres (Sans titre)")
        print(f"[proceed_na_values]\t INFO : {df_clean['titre'].isna().sum()} NA Before")
        
    df_clean.loc[df_clean['titre']=='(Sans titre)', 'titre'] = np.nan
    if verbose > 0:
        print(f"[proceed_na_values]\t INFO : {df_clean[df_clean['titre']=='(Sans titre)'].shape} oeuvres (Sans titre)")
        print(f"[proceed_na_values]\t INFO : {df_clean['titre'].isna().sum()} NA After")
    
    # A priori il serait possible de générer un titre pertient à partir des descriptions.
    # Dans un premier temps, suppression des lignes qui n'ont ni description, ni titre, ni auteur
    label_na = (df_clean["titre"].isna())&(df_clean["auteur"].isna())&(df_clean["description"].isna())&(df_clean["sujet_precisions"].isna())&(df_clean["sujet"].isna())
    df_clean = df_clean[~label_na]
    if verbose > 0:
        print(f"[proceed_na_values]\t INFO : {df_origin.shape[0]-df_clean.shape[0]} lignes sans labels supprimées")

    # On génère un titre à partir des autres colonnes textuelles.
    is_na_titre_idx = df_clean['titre'].isna()
    df_clean.loc[is_na_titre_idx, "titre"] = df_clean.loc[is_na_titre_idx,["description", 'type_oeuvre', 'sujet', 'sujet_precisions']].apply(lambda x: _nouveau_titre(description=x['description'], type_oeuvre=x['type_oeuvre'], sujet=x['sujet'], precisions=x['sujet_precisions']), axis=1)
    if verbose > 0:
        print(f"[proceed_na_values]\t INFO : {df_clean['titre'].isna().sum()} NA Titres après génération de titre")

    # On traite les types d'oeuvre
    if verbose > 0:
        print(f"[proceed_na_values]\t INFO : {df_clean['type_oeuvre'].isna().sum()} NA Type d'oeuvres")
    is_na_type_oeuvre = df_clean['type_oeuvre'].isna()
    df_clean.loc[is_na_type_oeuvre, "type_oeuvre"] = df_clean.loc[is_na_type_oeuvre,["description", 'materiaux_technique']].apply(lambda x: _nouveau_type_oeuvre(description=x['description'], materiaux_technique=x['materiaux_technique']), axis=1)
    # on traite les derniers résidus
    df_clean['type_oeuvre'] = df_clean['type_oeuvre'].fillna(df_clean['description'])
    df_clean['type_oeuvre'] = df_clean['type_oeuvre'].fillna(df_clean['materiaux_technique'])
    if verbose > 0:
        print(f"[proceed_na_values]\t INFO : {df_clean['type_oeuvre'].isna().sum()} NA Type d'oeuvres après traitement")

    # Création d'une colonne texte avec toutes les colonnes de descriptions
    df_clean["texte"] = df_clean[['sujet_precisions', "description", 'sujet']].apply(lambda x: _nouveau_texte(x=x), axis=1)
    if verbose > 0:
        print(f"[proceed_na_values]\t INFO : {df_clean['texte'].isna().sum()} NA Texte")

    return df_clean

# ----------------------------------------------------------------------------------
# PRIVATE FUNCTIONS - Text generation to fill na values
# ----------------------------------------------------------------------------------

# %% standardize_metier
def standardize_metier(input_str, verbose=0):
    """Replace the non standard métier with the standard name.

    Args:
        input_str (str): the metier to standardize
        verbose (int, optional): Log level. Defaults to 0.

    Returns:
        str: standard métier name
    """
    res = input_str
    if res is not None and isinstance(res, str):
        to_replace = {
                "joailler"      :"joaillier",
                "auteur de la composition": "auteur",
                "céramistes"    :"céramiste",
                "couturière"    :"couturier",
                "création"      :"créateur",
                "créatrice"     :"créatrice",
                "décoratrice"   :"décorateur",
                "composition"   :"compositeur",
                "direction"     :"directeur",
                "auteur d'origine":"auteur",
                "emailleur"     :"émailleur",
                "emaillographe" :"émaillographe",
                "ENGINEERS"     :"ingénieur",
                "exécution"     :"exécutant",
                "fondeurs"      :"fondeur",
                "imitateur de"  :"imitateur",
                "imprimerie phototypique":"imprimeur phototypique",
                "imprimeur à Marseille":"imprimeur",
                "imprmeur"      :"imprimeur",
                "maitre potier" :"maître-potier",
                "marchand mercier":"marchand",
                "ouvrier lunetier":"lunetier",
                "plasticienne"  :"plasticien",
                "poterie"       :"potier",
                "sculptrice"    :"sculpteur",
                "verrerie"      :"verrier",
            }
        for str1, str2 in to_replace.items():
            res = res.replace(str1, str2)
        res = res.strip()

    return res

# %% standardize_creation_millesime
def standardize_creation_millesime(str_millesime, verbose=0):
    annee_debut = np.nan
    annee_fin = np.nan
    regexsimple = r'(?P<value1>[0-9]{4}\b)'
    regex = regexsimple+r'.*(?P<value2>[0-9]{4}\b)'
    res = str_millesime
    if res is not None:
        gg = re.search(regex, res, re.I)
        if gg is None:
            gg = re.search(regexsimple, res, re.I)
        if gg is not None:
            annee_debut = gg.group("value1")
            try:
                annee_fin = gg.group("value2")
            except:
                pass
            if annee_fin is not None and not isinstance(annee_fin, float) and annee_debut is not None:
                if annee_fin < annee_debut:
                    tp = annee_debut
                    annee_debut = annee_fin
                    annee_fin = tp
            
    return [annee_debut, annee_fin]

# %% standardize_dimension
def standardize_dimension(str_dim, verbose=0):
    # l. 30 cm ; H. 65 cm ; P. 22 cm ; VOLUM. 0,0429
    # l. 30 CM ; H. 62 CM ; P. 23 CM ; VOLUM. 0,0428
    # Hauteur en cm 9.4 ; Largeur en cm 12.8
    # 'Hauteur en cm 17,2 ; Diamètre en cm 9.3'
    # Standardisation de la chaine
    largeur = np.nan
    hauteur = np.nan
    profondeur = np.nan
    res = str_dim
    if res is not None:
        res = res.replace("l.", "largeur")
        res = res.replace("H.", "hauteur")
        res = res.replace("P.", "profondeur")
        res = res.replace("L.", "profondeur")
        tab_temp = res.split(";")
        # Extraction des valeurs
        regex = r'(?P<value>[0-9]+,?\.?[0-9]*)'
        for s in tab_temp:
            if "largeur" in s.lower():
                gg = re.search(regex, s, re.I)
                largeur = gg.group("value") if gg is not None else np.nan
            elif "hauteur" in s.lower():
                gg = re.search(regex, s, re.I)
                hauteur = gg.group("value") if gg is not None else np.nan
            elif "profondeur" in s.lower():
                gg = re.search(regex, s, re.I)
                profondeur = gg.group("value") if gg is not None else np.nan
    
    return [largeur, hauteur, profondeur]

# %% clean_lieux_creation
def clean_lieux_creation(str_lieu, verbose=0):
    lieu = ""
    if str_lieu is not None and isinstance(str_lieu, str):
        if "lieu de création" in str_lieu:
            sp = str_lieu.split(",")[-1]
            sp = sp.split("(lieu de création)")[0]
            sp = sp.strip()
            sp = sp.split(";")[-1].strip()
            lieu = sp
        
    return lieu


# %% clean_titre
def clean_titre(input_str, verbose=0):
    res = input_str

    if res is not None:
        res = res.split(";")[0].strip()
        # Suppression des commentaires entre parenthèse dans les titres, par exemple : [titre attribué], (titre inscrit),  (esquisse)
        res = res.split("[")[0].strip()
        res = res.split("(")[0].strip()
        res = clean_text(res, verbose=verbose-1)
        
    return res

# %% clean_museo
def clean_museo(input_str, verbose=0):
    res = input_str

    if res is not None and isinstance(res, str):
        res = res.upper()
        if not res.startswith("M"):
            res = "M"+res
        res = res.strip()
        # Correction du numéro du musée de Sèvres
        if "M05019" == res:
            res = "M5019"
    return res



# %% clean_text
def clean_text(input_str, verbose=0):
    res = input_str

    if res is not None:
        # Suppression des termes spécifiques rencontrés lors de l'exploration des données
        to_replace = {
            # On remplace les guillemets par des simples quotes pour éviter des soucis à l'enregistrement
            "\""            : "'",
            "["             : "(",
            ")"             : ")",
            "Ã  "           :"à ",
            "Ã "            :"à",
            "Ã "            :"à",
            "Ã‡"            :"Ç",
            "Ã¼"            :'ü',
            "Å“"            :"oe",
            "(attribué à)"  :"",
            "(attibué à)"   :"",
            "(attribué)"    :"",
            'attribué Ã )'  :")",
            'attribué à)'   :")",
            '(attribué, '   :"(",
            ", attribué)"   :")",
            "(attribué,"    :"",
            "(?;Attribué)"  :"",
            "(Attribué)"    :"",
            "(Attribué;)"   :"",
            "(Attribué, )"  :"",
            "? (d\'après)"  : "",
            "\? (copie d\'après);": "",
            "(d'après, dit)":"(dit)",
            ", d'après, )"  :")",
            "(d'après;)"    :"",
            "(d'après la gravure de)":"",
            "(d'après)"     :"",
            "d'après)"      :")",
            "(d'après, "    :"(",
            "Ã"            :"Ï",
            "(dite)"        :"(dit)",
            "(dit, )"       :"(dit)",
            "(dite, )"      :"(dit)",
            "pendant (?)"   :"",
            " (?)"          :"",
            '?)'            :")",
            "née)"          :"né)",
            'atelier, genre de)':"",
            # Homogénéisation des métiers
            "joailler"      :"joaillier",
            "imprmeur"      :"imprimeur",
            "sculpeur"      :"sculpteur",
            "Â"             :"",
            "(genre de)"    :"",
            "\? (copie d\'après);"  : "",
            "? (d\'après)"          : "",
            'attribué Ã )'          :")",
            'atelier, genre de)'    :")",
            '?)'                    :")",
            "d'après)"              :")",
            "   "           :" ",
            "  "            :" ",
        }
        for str1, str2 in to_replace.items():
            res = res.replace(str1, str2)
        res = res.strip()

    return res.strip()

# %% expand_coordonnate
def expand_coordonnate(df, verbose=0):
    short_name = "expand_coordonnate"
    # Extraction des coordonnées
    df_coordonnees = df['pop_coordonnees'].dropna().str.split(r",", expand=True)
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df.shape} origin and {df_coordonnees.shape} coordonnates")
    df_coordonnees = df_coordonnees.rename(columns={0:"latitude", 1:'longitude'})

    # Fusion des df pour affecter les coordonnées à la DF musées
    df_res = pd.merge(df, df_coordonnees,left_index=True, right_index=True, copy=True, indicator=False)
    for col in ["latitude", "longitude"]:
        df_res[col] = df_res[col].fillna(0)
        df_res[col] = df_res[col].astype(float)
    
    df_res = df_res.drop(columns=["pop_coordonnees"])
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df.shape} origin and {df_res.shape} expanded")
    return df_res

# %% expand_millesime
def expand_millesime(df, verbose=0):
    # Traitement des années    
    df.loc[df['creation_millesime'].notna(),'millesime_clean'] =  df.loc[df['creation_millesime'].notna(),'creation_millesime'].apply(lambda x: standardize_creation_millesime(str_millesime=x, verbose=verbose))
    df.loc[df["millesime_clean"].notna(),"annee_debut"] = df.loc[df["millesime_clean"].notna(),"millesime_clean"].apply(lambda x: x[0])
    df.loc[df["millesime_clean"].notna(),"annee_fin"] = df.loc[df["millesime_clean"].notna(),"millesime_clean"].apply(lambda x: x[1])
    df = df.drop(columns=["creation_millesime", "millesime_clean"])
    return df

# %% expand_dimensions
def expand_dimensions(df, verbose=0):
    df["dimensions_clean"] = np.nan
    df.loc[df["dimensions"].notna(),"dimensions_clean"] = df.loc[df["dimensions"].notna(),"dimensions"].apply(lambda x: standardize_dimension(str_dim=x, verbose=verbose-1))
    i = 0
    for col in ["largeur_cm", "hauteur_cm", "profondeur_cm"]:
        df.loc[df["dimensions_clean"].notna(),col] = df.loc[df["dimensions_clean"].notna(),"dimensions_clean"].apply(lambda x: convert_to_float(x[i]))
        df[col] = df[col].fillna(0)
        try:
            df[col] = df[col].astype(float)
        except Exception as error:
            print(error)
        i += 1
    df = df.drop(columns=["dimensions_clean", "dimensions"])
    return df

# %% convert_to_float
def convert_to_float(str_):
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


# %% _nouveau_titre
def _nouveau_titre(description, type_oeuvre, sujet, precisions):
    """
    Génère un titre à partir du début de la description (jusqu'au premier séparateur identifié) ou à partir du type d'oeuvre ou à partir des précisions

    Args:
        description (str): _description_
        type_oeuvre (str): _description_
        sujet (str): _description_
        precisions (str): _description_

    Returns:
        str: Titre généré
    """
    titre = ""
    precision_done = False

    if precisions is not None and isinstance(precisions, str):
        titre = precisions.strip()
        precision_done = len(titre)>0
    

    if not precision_done and description is not None and isinstance(description, str):
        titre = description.split(".")[0]
        titre = titre.split(";")[0]
        titre = titre.split(",")[0]
        
    if not precision_done and type_oeuvre is not None and isinstance(type_oeuvre, str):
        start_type = type_oeuvre.split(" ")[0]
        if start_type.lower() not in titre.lower():
            titre = type_oeuvre + " " + titre

    if len(titre)==0 and sujet is not None and isinstance(sujet, str):
        start_type = sujet.split("(")[-1]
        start_type = start_type.split(")")[0]
        if len(start_type)>0:
            titre = start_type.strip()
    
    titre = titre.strip()

    return titre if len(titre)>0 else np.nan

# %% _nouveau_type_oeuvre
def _nouveau_type_oeuvre(description, materiaux_technique):
    titre = ""
    if description is not None and isinstance(description, str):
        titre = description.split(".")[0]
        titre = titre.split(";")[0]
        titre = titre.split(",")[0]
        
    if materiaux_technique is not None and isinstance(materiaux_technique, str):
        start_type = materiaux_technique.split(" ")[0]
        if start_type.lower() not in titre.lower():
            titre = materiaux_technique + " " + titre
    
    titre = titre.strip()

    return titre if len(titre)>0 else np.nan

# %% _nouveau_texte
def _nouveau_texte(x):
    titre = ""
    
    for col in x.index:
        if isinstance(x[col], str):
            sep = " " if len(titre)>0 and titre.endswith(".") else ". " if len(titre)>0 else ""
            titre = titre + sep +  x[col].strip()

    titre = titre.strip()

    return titre

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



# %% test
def test(verbose=1):
    _test_millesime(verbose=verbose)
    _test_standardize_dimensions(verbose=verbose)

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

    """
    OUTPUT expected :
    [proceed_duplicated]     INFO : Before (650853, 27)
    [proceed_duplicated]     INFO : after (639325, 27)
    [proceed_na_values]      INFO : (4653, 27) oeuvres (Sans titre)
    [proceed_na_values]      INFO : 136447 NA Before
    [proceed_na_values]      INFO : (0, 27) oeuvres (Sans titre)
    [proceed_na_values]      INFO : 141100 NA After
    [proceed_na_values]      INFO : 34168 lignes sans labels supprimées
    [proceed_na_values]      INFO : 173 NA Titres après génération de titre
    [proceed_na_values]      INFO : 220220 NA Type d'oeuvres
    [proceed_na_values]      INFO : 6880 NA Type d'oeuvres après traitement
    [proceed_na_values]      INFO : 0 NA Texte
    """

    if verbose > 1:
        figure, ax = color_graph_background(1,1)
        sns.heatmap(df_clean_na.isnull(), yticklabels=False,cbar=False, cmap='viridis')
        plt.xticks(rotation=45, ha="right", fontsize=10)
        plt.title("NA dans la DF après traitement.")
        figure.set_size_inches(18, 5, forward=True)
        plt.show()

   
