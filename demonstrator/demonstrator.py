# %% import
import sys
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
from dao.cojoden_dao import *


# ---------------------------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------------------------



# rechercher les musées sur plusieurs critères:
menu = "Pour rechercher un musée, saisissez la chaine recherchée : "

# SELECT * FROM artiste WHERE nom_search LIKE '%AURELIE' LIMIT 100;
EXIT_STR = "exit"

def display_menu(verbose=0):
    search_str = input(f"Saisissez le mot à rechercher ({EXIT_STR} pour quitter) :")
    if EXIT_STR not in search_str:
        search_place = input(f"Préciser la ville recherchée ({EXIT_STR} pour quitter) :")
        if EXIT_STR in search_place:
            search_str = search_place
    else:
        search_place = EXIT_STR
    return search_str, search_place

def run_demonstrator(verbose=0):
    short_name = "run_demo"
    search_str = ""
    search_place = ""

    while EXIT_STR not in search_str:
        search_str, search_place = display_menu(verbose=verbose)
        if search_str is not None:
            search_str = search_str.strip()
            # Rechercher :
            # 1. dans les oeuvres
            # 2. dans les artistes
            # 3. dans les domaines
            # 4. dans les matériaux
            

# ----------------------------------------------------------------------------------
#                        MAIN
# ----------------------------------------------------------------------------------
# %% main
if __name__ == '__main__':
    short_name = "demonstrator"
    print(f"[{short_name}]------------------------------------------------------ START")
    verbose = 1
    run_demonstrator(verbose=verbose)

    print(f"[{short_name}]------------------------------------------------------ END")