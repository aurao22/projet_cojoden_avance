# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Module to populate the cojoden database from CSV Files.
WARNING : the populate function will erase previous data

Project: Cojoden avance
=======

Usage:
======
    python cojoden_dao_populate_from_csv.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"

# %% import
import sys
from os import getcwd
from os.path import join

execution_path = getcwd()

if 'PROJETS' not in execution_path:
    execution_path = join(execution_path, "PROJETS")
if 'projet_cojoden_avance' not in execution_path:
    execution_path = join(execution_path, "projet_cojoden_avance")
print(f"[cojoden_dao_insert] execution path= {execution_path}")
sys.path.append(execution_path)

from tqdm import tqdm
from dao.cojoden_dao_search import search_artiste, search_ville, search_metier, search_domaine, search_materiaux_technique, search_musee_by_museo, search_oeuvre_by_ref
from data_preprocessing.cojoden_functions import convert_string_to_search_string
from dao.cojoden_dao import executer_sql

# ----------------------------------------------------------------------------------
# %% INSERT FUNCTION
# ----------------------------------------------------------------------------------
def insert_metier(metier, categorie=None, verbose=0):
    shortname = 'insert_metier'
    # INSERT INTO `METIER` (`metier_search`, `metier`, `categorie`) VALUES (NULL, NULL, NULL);
    exist = search_metier(value=metier, verbose=verbose)
    if exist is not None and len(exist)>0:
        if verbose > 0:
            print(f"[{shortname}] \tINFO : {metier} déjà existante en BDD => {exist}")
        return exist
    else:
        metier_search = convert_string_to_search_string(metier)

        sql_start = "INSERT INTO `METIER` (`metier_search`, `metier`"
        metier = metier.replace("'", "\\'")
        sql_val = f") VALUES ('{metier_search}', '{metier}'"
        
        if categorie is not None  and len(categorie)>0:
            sql_start   += ", `categorie`"
            categorie = categorie.replace("'", "\\'")
            sql_val     += f", '{categorie}'"

        sql = sql_start + sql_val + ");"

        if verbose > 1:
            print(f"[{shortname}] \t DEBUG : {sql}")
        
        res = executer_sql(sql=sql,verbose=verbose)
        return res

def insert_ville(ville, departement=None, region1=None, verbose=0):
    shortname = 'insert_ville'
    # INSERT INTO `VILLE` (`id`, `ville_search`, `ville`, `departement`, `region1`) VALUES (NULL, NULL, NULL, NULL, NULL);
    if ville is not None:
        exist = search_ville(value=ville, verbose=verbose)
        if exist is not None and len(exist)>0:
            if verbose > 0:
                print(f"[{shortname}] \tINFO : {ville} déjà existante en BDD => {exist}")
            return exist
        else:
            ville_search = convert_string_to_search_string(ville)
            
            sql_start = "INSERT INTO `VILLE` (`id`, `ville_search`, `ville` "
            ville = ville.replace("'", "\\'")
            sql_val = f") VALUES (NULL,'{ville_search}', '{ville}'"
            
            if departement is not None  and len(departement)>0:
                sql_start   += ", `departement`"
                departement = departement.replace("'", "\\'")
                sql_val     += f", '{departement}'"

            if region1 is not None  and len(region1)>0:
                sql_start   += ", `region1`"
                region1 = region1.replace("'", "\\'")
                sql_val     += f", '{region1}'"

            sql = sql_start + sql_val + ");"

            if verbose > 1:
                print(f"[{shortname}] \t DEBUG : {sql}")
            
            res = executer_sql(sql=sql,verbose=verbose)
            return res
    return None

def insert_musee(museo, nom, latitude=None, longitude=None, plaquette_url=None, ville=None, departement=None, region1=None,verbose=0):
    shortname = 'insert_metier'
    # INSERT INTO `MUSEE` (`museo`, `nom_search`, `nom`, `latitude`, `longitude`, `plaquette_url`, `ville`) VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL);

    exist = search_musee_by_museo(value=museo, verbose=verbose)
    if exist is not None and len(exist)>0:
        if verbose > 0:
            print(f"[{shortname}] \tINFO : {museo} déjà existant en BDD => {exist}")
        return exist
    else:
        if ville is not None and isinstance(ville, str):
            v_id = insert_ville(ville=ville, departement=departement, region1=region1, verbose=verbose)
            if (v_id is None or v_id == 0 or (isinstance(v_id, list) and len(v_id)==0)):
                if verbose > 0: print(f"[{shortname}] \tWARN : {ville} not found and not inserted.")
                ville = None
            else:
                if isinstance(v_id, list):
                    ville = v_id[0][0]
                    
        nom_search = convert_string_to_search_string(nom)

        sql_start = "INSERT INTO `MUSEE` (`museo`, `nom_search`, `nom` "
        nom = nom.replace("'", "\\'")
        sql_val = f") VALUES ('{museo}', '{nom_search}', '{nom}'"
        
        if latitude is not None:
            sql_start   += ", `latitude`"
            sql_val     += f", {latitude}"
        
        if longitude is not None:
            sql_start   += ", `longitude`"
            sql_val     += f", {longitude}"

        if plaquette_url is not None  and len(plaquette_url)>0:
            sql_start   += ", `plaquette_url`"
            plaquette_url = plaquette_url.replace("'", "\\'")
            sql_val     += f", '{plaquette_url}'"

        if ville is not None :
            sql_start   += ", `ville`"
            sql_val     += f", {ville}"

        sql = sql_start + sql_val + ");"

        if verbose > 1:
            print(f"[{shortname}] \t DEBUG : {sql}")
        
        res = executer_sql(sql=sql,verbose=verbose)
        return res

def insert_artiste(nom_naissance, nom_dit=None, commentaire=None, id=None, verbose=0):
    shortname = 'insert_artiste'
    # INSERT INTO `ARTISTE` (`id`, `nom_search`, `nom_naissance`, `nom_dit`, `commentaire`) VALUES (NULL, NULL, NULL, NULL, NULL);

    exist = search_artiste(value=nom_naissance, verbose=verbose)
    if exist is not None and len(exist)>0:
        if verbose > 0:
            print(f"[{shortname}] \tINFO : {nom_naissance} déjà existant en BDD => {exist}")
        return exist
    else:
        nom_search = convert_string_to_search_string(nom_naissance)

        sql_start = "INSERT INTO `ARTISTE` (`nom_naissance`, `nom_search`"
        nom_naissance = nom_naissance.replace("'", "\\'")
        sql_val = f") VALUES ('{nom_naissance}', '{nom_search}'"
        
        if id is not None  and len(id)>0:
            sql_start   += ", `id`"
            sql_val     += f", {int(id)}"

        if nom_dit is not None  and len(nom_dit)>0:
            sql_start   += ", `nom_dit`"
            nom_dit = nom_dit.replace("'", "\\'")
            sql_val     += f", '{nom_dit}'"

        if commentaire is not None  and len(commentaire)>0:
            sql_start   += ", `commentaire`"
            commentaire = commentaire.replace("'", "\\'")
            sql_val     += f", '{commentaire}'"

        sql = sql_start + sql_val + ");"

        if verbose > 1:
            print(f"[{shortname}] \t DEBUG : {sql}")
        
        res = executer_sql(sql=sql,verbose=verbose)
        return res

def insert_oeuvre(ref,titre,type_oeuvre,lieux_conservation,texte=None, annee_debut=None,annee_fin=None,inscriptions=None,largeur_cm=0,hauteur_cm=0,profondeur_cm=0,creation_lieux=None, commentaires=None, verbose=0):
    shortname = 'insert_oeuvre'
    # INSERT INTO `OEUVRE` (`ref`, `titre`, `type`, `texte`, `lieux_conservation`, `annee_debut`, `annee_fin`, `inscriptions`, `commentaires`, `largeur_cm`, `hauteur_cm`, `profondeur_cm`, `img_url`, `creation_lieux`) VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

    exist = search_oeuvre_by_ref(value=ref, verbose=verbose)
    if exist is not None and len(exist)>0:
        if verbose > 0:
            print(f"[{shortname}] \tINFO : {ref} déjà existant en BDD => {exist}")
        return exist
    else:
        sql_start = "INSERT INTO `OEUVRE` (`ref`, `titre`"
        titre = titre.replace("'", "\\'")
        ref = ref.replace("'", "\\'")
        sql_val = f") VALUES ('{ref}', '{titre}'"
        
        if type_oeuvre is not None  and len(type_oeuvre)>0:
            sql_start   += ", `type`"
            type_oeuvre = type_oeuvre.replace("'", "\\'")
            sql_val     += f", '{type_oeuvre}'"

        if texte is not None  and len(texte)>0:
            sql_start   += ", `texte`"
            texte = texte.replace("'", "\\'")
            sql_val     += f", '{texte}'"

        if lieux_conservation is not None  and len(lieux_conservation)>0:
            sql_start   += ", `lieux_conservation`"
            lieux_conservation = lieux_conservation.replace("'", "\\'")
            sql_val     += f", '{lieux_conservation}'"

        if annee_debut is not None  and len(annee_debut)>0:
            sql_start   += ", `annee_debut`"
            sql_val     += f", '{int(annee_debut)}'"

        if annee_fin is not None  and len(annee_fin)>0:
            sql_start   += ", `annee_fin`"
            sql_val     += f", '{int(annee_fin)}'"

        if inscriptions is not None  and len(inscriptions)>0:
            sql_start   += ", `inscriptions`"
            inscriptions = inscriptions.replace("'", "\\'")
            sql_val     += f", '{inscriptions}'"

        if largeur_cm is not None and largeur_cm>0:
            sql_start   += ", `largeur_cm`"
            sql_val     += f", {int(largeur_cm)}"
        
        if hauteur_cm is not None and hauteur_cm > 0:
            sql_start   += ", `hauteur_cm`"
            sql_val     += f", {int(hauteur_cm)}"

        if profondeur_cm is not None  and profondeur_cm >0:
            sql_start   += ", `profondeur_cm`"
            sql_val     += f", {int(profondeur_cm)}"

        if creation_lieux is not None  and len(creation_lieux)>0:
            sql_start   += ", `creation_lieux`"
            sql_val     += f", {int(creation_lieux)}"

        if commentaires is not None  and len(commentaires)>0:
            sql_start   += ", `commentaires`"
            commentaires = commentaires.replace("'", "\\'")
            sql_val     += f", '{commentaires}'"
        
        sql = sql_start + sql_val + ");"

        if verbose > 1:
            print(f"[{shortname}] \t DEBUG : {sql}")
        
        res = executer_sql(sql=sql,verbose=verbose)
        return res

def insert_domaine(domaine, id=None, verbose=0):
    shortname = 'insert_domaine'
    # INSERT INTO `DOMAINE` (`id`, `dom_search`, `domaine`) VALUES (NULL, NULL, NULL);
    exist = search_domaine(value=domaine, verbose=verbose)
    if exist is not None and len(exist)>0:
        if verbose > 0:
            print(f"[{shortname}] \tINFO : {domaine} déjà existant en BDD => {exist}")
        return exist
    else:
        dom_search = convert_string_to_search_string(domaine)

        sql_start = "INSERT INTO `DOMAINE` (`dom_search`, `domaine`"
        domaine = domaine.replace("'", "\\'")
        sql_val = f") VALUES ('{dom_search}', '{domaine}'"
        
        if id is not None  and len(id)>0:
            sql_start   += ", `id`"
            sql_val     += f", {int(id)}"

        sql = sql_start + sql_val + ");"

        if verbose > 1:
            print(f"[{shortname}] \t DEBUG : {sql}")
        
        res = executer_sql(sql=sql,verbose=verbose)
        return res

def insert_materiaux(materiaux_technique, id=None, verbose=0):
    shortname = 'insert_materiaux'
    # INSERT INTO `MATERIEAUX_TECHNIQUE` (`id`, `mat_search`, `materiaux_technique`) VALUES (NULL, NULL, NULL);"
    exist = search_materiaux_technique(value=materiaux_technique, verbose=verbose)
    if exist is not None and len(exist)>0:
        if verbose > 0:
            print(f"[{shortname}] \tINFO : {materiaux_technique} déjà existant en BDD => {exist}")
        return exist
    else:
        dom_search = convert_string_to_search_string(materiaux_technique)

        sql_start = "INSERT INTO `materiaux_technique` (`mat_search`, `materiaux_technique`"
        materiaux_technique = materiaux_technique.replace("'", "\\'")
        sql_val = f") VALUES ('{dom_search}', '{materiaux_technique}'"
        
        if id is not None  and len(id)>0:
            sql_start   += ", `id`"
            sql_val     += f", {int(id)}"

        sql = sql_start + sql_val + ");"

        if verbose > 1:
            print(f"[{shortname}] \t DEBUG : {sql}")
        
        res = executer_sql(sql=sql,verbose=verbose)
        return res

def insert_concerner(ref_oeuvre, domaine, verbose=0):
    shortname = 'insert_concerner'
    # INSERT INTO `CONCERNER` (`domaine`, `oeuvre`) VALUES (NULL, NULL);

    if domaine is not None and isinstance(domaine, str):
        v_id = insert_domaine(domaine=domaine, verbose=verbose)
        if (v_id is None or v_id == 0 or (isinstance(v_id, list) and len(v_id)==0)):
            if verbose > 0: print(f"[{shortname}] \tWARN : {domaine} not found and not inserted.")
            domaine = None
        else:
            if isinstance(v_id, list):
                domaine = v_id[0][0]
            else:
                domaine = v_id
    
    sql = f"INSERT INTO `CONCERNER` (`domaine`, `oeuvre`) VALUES ({domaine}, '{ref_oeuvre}');"
        
    if verbose > 1:
        print(f"[{shortname}] \t DEBUG : {sql}")
    
    res = executer_sql(sql=sql,verbose=verbose)
    return res

def insert_creer(ref_oeuvre, artiste, role=None, nom_dit=None, verbose=0):
    shortname = 'insert_creer'
    # INSERT INTO `CREER` (`oeuvre`, `artiste`, `role`) VALUES (NULL, NULL, NULL);

    if artiste is not None and isinstance(artiste, str):
        v_id = insert_artiste(nom_naissance=artiste, nom_dit=nom_dit, verbose=verbose)
        if (v_id is None or v_id == 0 or (isinstance(v_id, list) and len(v_id)==0)):
            if verbose > 0: print(f"[{shortname}] \tWARN : {artiste} not found and not inserted.")
            artiste = None
        else:
            if isinstance(v_id, list):
                artiste = v_id[0][0]
            else:
                artiste = v_id
    
    sql_start = "INSERT INTO `CREER` (`oeuvre`, `artiste`"
    ref_oeuvre = ref_oeuvre.replace("'", "\\'")
    sql_val = f") VALUES ('{ref_oeuvre}', {artiste}"
    
    if role is not None  and len(role)>0:
        sql_start   += ", `role`"
        role = role.replace("'", "\\'")
        sql_val     += f", '{role}'"

    sql = sql_start + sql_val + ");"

    if verbose > 1:
        print(f"[{shortname}] \t DEBUG : {sql}")
    
    res = executer_sql(sql=sql,verbose=verbose)
    return res


def insert_composer(ref_oeuvre, materiaux, complement=None, verbose=0):
    shortname = 'insert_composer'
    # INSERT INTO `COMPOSER` (`oeuvre`, `materiaux`, `complement`) VALUES (NULL, NULL, NULL);

    if materiaux is not None and isinstance(materiaux, str):
        # Il faut récupérer l'identifiant
        v_id = insert_materiaux(materiaux_technique=materiaux, verbose=verbose)
        if (v_id is None or v_id == 0 or (isinstance(v_id, list) and len(v_id)==0)):
            if verbose > 0: print(f"[{shortname}] \tWARN : {materiaux} not found and not inserted.")
            materiaux = None
        else:
            if isinstance(v_id, list):
                materiaux = v_id[0][0]
            else:
                materiaux = v_id
        
    sql_start = "INSERT INTO `CREER` (`oeuvre`, `artiste`"
    ref_oeuvre = ref_oeuvre.replace("'", "\\'")
    sql_val = f") VALUES ('{ref_oeuvre}', {materiaux}"
    
    if complement is not None  and len(complement)>0:
        sql_start   += ", `complement`"
        complement = complement.replace("'", "\\'")
        sql_val     += f", '{complement}'"

    sql = sql_start + sql_val + ");"

    if verbose > 1:
        print(f"[{shortname}] \t DEBUG : {sql}")
    
    res = executer_sql(sql=sql,verbose=verbose)
    return res

# ----------------------------------------------------------------------------------
#                        PRIVATE
# ----------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------
#  %%                      TEST
# ----------------------------------------------------------------------------------
def _test_insert_ville(verbose=1):
    to_test = {
        # (ville,departement,region1) : ""
        ("Lannion","Côtes d'Armor",'Bretagne')  : 411,
        ("Pédernec","Côtes d'Armor",'Bretagne') : 412,
        ("Brest","Côtes d'Armor",'Bretagne')    : 64, # déjà existant
        (None,"Finistère",'Bretagne')           : None,
    }

    for (ville,departement,region1), expected in tqdm(to_test.items(), desc="insert_ville"):
        res = insert_ville(ville, departement=departement, region1=region1, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        
        assert res == expected or res > 410


def _test_insert_metier(verbose=1):
    to_test = {
        # (metier,categorie)
        ("test_Magicien",None)            : 'TEST MAGICIEN',
        ("test_Testeur", "Informatique")  : 'TEST TESTEUR',
    }
    for (metier,categorie), expected in tqdm(to_test.items(), desc="insert_metier"):
        res = insert_metier(metier=metier, categorie=categorie, verbose=verbose)
        res = search_metier(value=metier, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        assert res == expected

def _test_insert_musee(verbose=1):
    to_test = {
        # (museo, nom, latitude, longitude, ville, departement, region1)
        ("test_museo_1", "test_museo_1", None, None, "LANNION", "Côtes d'Armor",'Bretagne')             : "test_museo_1",
        # 64 = Brest
        ("test_museo_2", "test_museo_2", None, None,  64,    None          ,None)                       : "test_museo_2",
        ("test_museo_3", "test_museo_3", None, None,  "BREST",    None          ,None)                  : "test_museo_3",
    }
    for (museo, nom, latitude, longitude, ville, departement, region1), expected in tqdm(to_test.items(), desc="insert_musee"):
        res = insert_musee(museo=museo, nom=nom, latitude=latitude, longitude=longitude, ville=ville, departement=departement, region1=region1,verbose=verbose)
        res = search_musee_by_museo(value=museo, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        assert res == expected

def _test_insert_artiste(verbose=1):
    to_test = {
        # (nom_naissance, nom_dit)
        ("test_artiste_1", "test_artiste_1_dit")  : 46237,
        ("test_artiste_2", "test_artiste_2_dit")  : 46238,
        ("test_artiste_1", None)                  : 46237,   
    }
    for (nom_naissance, nom_dit), expected in tqdm(to_test.items(), desc="insert_artiste"):
        res = insert_artiste(nom_naissance=nom_naissance, nom_dit=nom_dit, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        
        assert res == expected or res > 46236

def _test_insert_oeuvre(verbose=1):
    # SELECT * FROM oeuvre WHERE lieux_conservation = 'M0197' LIMIT 100;
    # M0197 => Brest
    # M0196 => Saint Brieuc
    to_test = {
        # (ref,titre,type_oeuvre,lieux_conservation)
        ("test_oeuvre_1", "test_oeuvre_titre", "type test", 'M0197')  : "test_oeuvre_1",
        ("test_oeuvre_2", "test_oeuvre_titre", "type test", "M0196")  : "test_oeuvre_2",
        ("test_oeuvre_1", "test_oeuvre_titre", None, None)  : "test_oeuvre_1",   
    }
    for (ref,titre,type_oeuvre,lieux_conservation), expected in tqdm(to_test.items(), desc="insert_oeuvre"):
        res = insert_oeuvre(ref=ref,titre=titre,type_oeuvre=type_oeuvre,lieux_conservation=lieux_conservation,verbose=verbose)
        res = search_oeuvre_by_ref(value=ref, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        assert res == expected

def _test_insert_domaine(verbose=1):
    to_test = {
        # (ref,titre,type_oeuvre,lieux_conservation)
        "test_domaine_1"  : 143,
        "test_domaine_2"  : 144,
        "afrique"         : 1,
        "cinéma"          : 40,

    }
    for domaine, expected in tqdm(to_test.items(), desc="insert_domaine"):
        res = insert_domaine(domaine, verbose=0)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        
        assert res == expected or res > 142

def _test_insert_materiaux(verbose=1):
    to_test = {
        # (ref,titre,type_oeuvre,lieux_conservation)
        "test_mat_1"    : 8463,
        "test_mat_2"    : 8464,
        "acétate"       : 32,
        "accacia"       : 30,
        "agate"         : 90,
    }
    for mat, expected in tqdm(to_test.items(), desc="insert_materiaux"):
        res = insert_materiaux(materiaux_technique=mat, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        
        assert res == expected or res > 8462

def _test_insert_concerner(verbose=1):
    to_test = {
        # (ref_oeuvre,domaine)
        ("test_oeuvre_1", "cinéma")    : 0,
        ("test_oeuvre_1", 10)          : 0,
    }
    for (ref_oeuvre,domaine), expected in tqdm(to_test.items(), desc="insert_concerner"):
        res = insert_concerner(ref_oeuvre, domaine, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        
        assert res == expected
    
def _test_insert_creer(verbose=1):
    to_test = {
        # (ref_oeuvre,artiste, role, nom_dit)
        ("test_oeuvre_1", 36847, 'acteur', None)                   : 0,
        ("test_oeuvre_1", 40275, 'acteur2', None)                  : 0,
        ("test_oeuvre_2", "RAOUL joconde", 'acteur', "joconde")    : 0,
        ("test_oeuvre_2", "RAOUL", 'acteur 3', None)    : 0,
    }
    for (ref_oeuvre,artiste, role, nom_dit), expected in tqdm(to_test.items(), desc="insert_creer"):
        res = insert_creer(ref_oeuvre=ref_oeuvre, artiste=artiste, role=role, nom_dit=nom_dit, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        assert res == expected

def _test_insert_composer(verbose=0):
    to_test = {
        # (ref_oeuvre,materiaux)
        ("test_oeuvre_1", 10)                   : 0,
        ("test_oeuvre_1", 20)                   : 0,
        ("test_oeuvre_2", "agate")              : 0,
        ("test_oeuvre_2", "test_mat_3")         : 0,
    }
    for (ref_oeuvre,materiaux), expected in tqdm(to_test.items(), desc="insert_composer"):
        res = insert_composer(ref_oeuvre=ref_oeuvre, materiaux=materiaux, verbose=verbose)
        if isinstance(res, list) and len(res)>0:
            res = res[0][0]
        assert res == expected

def _test_clean_after_run(verbose=1):

    to_execute = [
        # Suppression des clés étrangères en 1er
        "DELETE FROM concerner WHERE oeuvre LIKE 'test_oeuvre_%';",
        "DELETE FROM creer WHERE oeuvre LIKE 'test_oeuvre_%';",
        "DELETE FROM composer WHERE oeuvre LIKE 'test_oeuvre_%';",
        
        "DELETE FROM VILLE WHERE id > '410';",      # Suppression des lignes qui ont été insérées hors populate initial
        "ALTER TABLE VILLE auto_increment = 411;"

        "DELETE FROM METIER WHERE metier_search LIKE 'TEST %';",

        "DELETE FROM MUSEE WHERE museo LIKE 'test_museo_%';",

        "DELETE FROM artiste WHERE id > '46236';",  # Suppression des lignes qui ont été insérées hors populate initial
        "ALTER TABLE artiste auto_increment = 46237;"

        "DELETE FROM materiaux_technique WHERE id > '8462';",  # Suppression des lignes qui ont été insérées hors populate initial
        "ALTER TABLE materiaux_technique auto_increment = 8463;"

        "DELETE FROM oeuvre WHERE ref LIKE 'test_oeuvre_%';",

        "DELETE FROM domaine WHERE id > '142';",    # Suppression des lignes qui ont été insérées hors populate initial
        "ALTER TABLE domaine auto_increment = 143;"

        "DELETE FROM domaine WHERE id > '8462';",    # Suppression des lignes qui ont été insérées hors populate initial
        "ALTER TABLE domaine auto_increment = 8463;"
    ]

    for sql in tqdm(to_execute, desc="clean_after_run"):
        try:
            res = executer_sql(sql)

        except Exception as error:
            print(error)


# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    verbose = 1
    _test_clean_after_run(verbose=verbose)
    _test_insert_ville(verbose=verbose)
    _test_insert_metier(verbose=verbose)
    _test_insert_musee(verbose=verbose)
    _test_insert_artiste(verbose=verbose)
    _test_insert_oeuvre(verbose=verbose)
    _test_insert_domaine(verbose=verbose)
    _test_insert_materiaux(verbose=verbose)
    _test_insert_concerner(verbose=verbose)
    _test_insert_creer(verbose=verbose)
    _test_insert_composer(verbose=verbose)
    _test_clean_after_run(verbose=verbose)

