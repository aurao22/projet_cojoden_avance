# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Module containing project utilities functions

Project: Cojoden avance
=======

Usage:
======
    python cojoden_functions.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"

# %% import
import numpy as np
import matplotlib.pyplot as plt
import string
import pandas as pd

SPECIAL_CHARS_ARA = 'ÀàÁáÂâÃãÄäÅå&ÆæÇçÐÉÈèéÊêËëÌìÍíÎîÏïÑñÒòÓóÔôÕõÖöœŒØøßÙùÚúÛûÜüÝýŸÿ'

# ----------------------------------------------------------------------------------
#                        FUNCTION TRAITEMENT DU TEXTE
# ----------------------------------------------------------------------------------
#%% convert_string_to_search_string
def convert_string_to_search_string(input):
    output = input.upper()
    to_replace_to_reverse = get_replacement_car_for_specials_car()
    for replace_val, to_replace_cars in to_replace_to_reverse.items():
        for to_replace in to_replace_cars:
            output = output.replace(to_replace, replace_val, regex=False)
    return output

#%% convert_df_string_to_search_string
def convert_df_string_to_search_string(input_df, col_name, stop_word_to_remove=[]):
    output = input_df.copy()
    output[col_name] = output[col_name].str.upper()
    to_replace_to_reverse = get_replacement_car_for_specials_car()
    # On ajoute la liste des stop words
    to_replace_to_reverse[""] = to_replace_to_reverse.get("", [])
    to_replace_to_reverse[""].extend(stop_word_to_remove)

    for replace_val, to_replace_cars in to_replace_to_reverse.items():
        for to_replace in to_replace_cars:
            output[col_name] = output[col_name].str.replace(to_replace.upper(), replace_val.upper(), regex=False)
    output[col_name] = output[col_name].str.replace("   ", " ", regex=False)
    output[col_name] = output[col_name].str.replace("  ", " ", regex=False)
    output[col_name] = output[col_name].str.strip()
    return output

#%% get_replacement_car_for_specials_car
def get_replacement_car_for_specials_car():
    to_replace_to_reverse = {
        'A' : ['À','à','Á','á','Â','â','Ã','ã','Ä','ä','Å','å'],
        'ET' : ['&'],
        'AE': ['Æ','æ'],
        'C' : ['Ç','ç'],
        'D' : ['Ð'],
        'E' : ['É','È','è','é','Ê','ê','Ë','ë'],
        'I' : ['Ì','ì','Í','í','Î','î','Ï','ï'],
        'N' : ['Ñ','ñ'],
        'O' : ['Ò','ò','Ó','ó','Ô','ô','Õ','õ','Ö','ö'],
        'OE': ['œ','Œ'],
        'O' : ['Ø','ø'],
        'SS': ['ß'],
        'U' : ['Ù','ù','Ú','ú','Û','û','Ü','ü'],
        'Y' : ['Ý','ý','Ÿ','ÿ'],
        ' ' : _car_to_replace_by_space(),
        }
    return to_replace_to_reverse

#%% get_annee
def get_annee(nom_auteur_search, df_precision, type_=1):
    annee = -1
    list_type = ['1', '2', '3', '4']

    if not type_:
        list_type = ['4', '3', '2', '1']

    if nom_auteur_search is not None and len(nom_auteur_search) > 3:
        for i in list_type:
            tp = df_precision[(df_precision['auteur_search'].str.contains(nom_auteur_search)) & (df_precision[f'annee_{i}'].notna())]
            if tp is not None and tp.shape is not None and tp.shape[0]>0:
                mini = min(tp[f'annee_{i}'].values)
                maxi = max(tp[f'annee_{i}'].values)
                if annee == -1 or (type_ and annee > mini) or (not type_ and annee < maxi):
                    if type_:
                        annee = mini
                        break
                    else:
                        annee = maxi
                        break
    if annee == -1:
        annee = np.nan

    return annee

#%% _car_to_replace_by_space
def _car_to_replace_by_space():
    punc = set(string.punctuation)
    for c in ['ð', 'Þ', 'þ', '-', ',', "'", ".",  "  "]:
        punc.add(c)
    punc = list(punc)
    return punc

# ----------------------------------------------------------------------------------
#                        FUNCTION DF
# ----------------------------------------------------------------------------------
#%% get_na_columns_classement
def get_na_columns_classement(df, verbose=0):
    """Supprime les colonnes qui ont un pourcentage de NA supérieur au max_na

    Args:
        df (DataFrame): Données à nettoyer
        max_na (int) : pourcentage de NA maximum accepté (qui sera conserver)
        verbose (bool, optional): Mode debug. Defaults to False.
        inplace (bool, optional): Pour mettre à jour la dataframe reçue directement. Defaults to True.

    Returns:
        [DataFrame]: DataFrame avec les données mises à jour
    """      
    dict_col = {}

    # Constitution de la list des colonnes à supprimer
    for col in df.columns:
        pourcent = int((df[col].isna().sum()*100)/df.shape[0])
        list = dict_col.get(pourcent, [])
        list.append(col)
        dict_col[pourcent] = list
            
    if verbose:
        for k in range(101):
            if len(dict_col.get(k, [])) > 0:
                print(k, "=>", len(dict_col.get(k, [])), dict_col.get(k, []))   
    return dict_col 

#%% remove_na_columns
def remove_na_columns(df, max_na=73, excluded_cols=[], verbose=True, inplace=True):
    """Supprime les colonnes qui ont un pourcentage de NA supérieur au max_na

    Args:
        df (DataFrame): Données à nettoyer
        max_na (int) : pourcentage de NA maximum accepté (qui sera conserver)
        verbose (bool, optional): Mode debug. Defaults to False.
        inplace (bool, optional): Pour mettre à jour la dataframe reçue directement. Defaults to True.

    Returns:
        [DataFrame]: DataFrame avec les données mises à jour
    """
    if not inplace:
        df = df.copy()
        
    to_remove = set()
    dict_col = {}

    # Constitution de la list des colonnes à supprimer
    for col in df.columns:
        pourcent = int((df[col].isna().sum()*100)/df.shape[0])
        list = dict_col.get(pourcent, [])
        list.append(col)
        dict_col[pourcent] = list
        if pourcent > max_na and col not in excluded_cols:
            to_remove.add(col)
    
    if verbose:
        for k in range(101):
            if len(dict_col.get(k, [])) > 0:
                print(k, "=>", len(dict_col.get(k, [])), dict_col.get(k, []))
    
    shape_start = df.shape
    # Suppression des colonnes
    df.drop(to_remove, axis=1, inplace=True)
    print(f"Removed : {to_remove}")
    shape_end = df.shape
    
    print("remove_na_columns, shape start: ",shape_start,"=>",shape_end,"s............................................... END")        
    return df  


# %% transpose_df_after_split_extend
def transpose_df_after_split_extend(df, cols_to_keep, verbose=0):
    short_name = "transpose_df_after_split_extend"
    cols = list(df.columns)
    if verbose > 1:
        print(f"[{short_name}]\t DEBUG : {cols}")
    
    cols_to_keep.append(0)
    df4 = df[cols_to_keep]
    for col in cols_to_keep:
        cols.remove(col)
    cols_to_keep.remove(0)
    
    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df4.shape} on origin df and {cols} to proceed")
    
    for col in cols:
        cols_to_keep.append(col)
        dft = df[cols_to_keep]
        dft = dft[dft[col].notna()]
        dft = dft.rename(columns={col: 0})
        df4 = pd.concat([df4, dft], axis=0)
        if verbose > 1:
            print(f"[{short_name}]\t DEBUG : {df4.shape} after add {col}")    
        cols_to_keep.remove(col)

    if verbose > 0:
        print(f"[{short_name}]\t INFO : {df4.shape} at the end and {df4.columns}")
    df4 = df4.sort_values(cols_to_keep)
    return df4

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

# ----------------------------------------------------------------------------------
#                        GRAPHIQUES
# ----------------------------------------------------------------------------------
PLOT_FIGURE_BAGROUNG_COLOR = 'white'
PLOT_BAGROUNG_COLOR = PLOT_FIGURE_BAGROUNG_COLOR

#%% color_graph_background
def color_graph_background(ligne=1, colonne=1):
    figure, axes = plt.subplots(ligne,colonne)
    figure.patch.set_facecolor(PLOT_FIGURE_BAGROUNG_COLOR)
    if isinstance(axes, np.ndarray):
        for axe in axes:
            # Traitement des figures avec plusieurs lignes
            if isinstance(axe, np.ndarray):
                for ae in axe:
                    ae.set_facecolor(PLOT_BAGROUNG_COLOR)
            else:
                axe.set_facecolor(PLOT_BAGROUNG_COLOR)
    else:
        axes.set_facecolor(PLOT_BAGROUNG_COLOR)
    return figure, axes