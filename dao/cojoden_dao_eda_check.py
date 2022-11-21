# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Module to explore the database cojoden

Project: Cojoden avance
=======

Usage:
======
    python cojoden_dao_eda_check.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
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
print(f"[cojoden_dao_eda_check] execution path= {execution_path}")
sys.path.append(execution_path)
from dao.cojoden_dao import executer_sql
from tqdm import tqdm

# ----------------------------------------------------------------------------------
# %% FUNCTIONS
# ----------------------------------------------------------------------------------

def _check_nb_role(verbose=0):
    expected = 2673
    sql = """SELECT count(creer.role) as nb, creer.role
    FROM creer
    GROUP BY creer.role
    ORDER BY nb DESC;"""
    res = executer_sql(sql, verbose=verbose)

    assert len(res) >= expected, f"Le nombre de rôle a changé : {len(res)} vs {expected} expected"
    print(f"[nb_role]  \t\t{len(res)} \t------------------->  CHECK")

def _check_nb_distinct_role(verbose=0):
    expected = 2672
    sql = """SELECT count(distinct(creer.role))
            FROM creer;"""
    res = executer_sql(sql, verbose=verbose)

    assert res[0][0] >= expected, f"Le nombre de rôle disctinct a changé : {res[0][0]} vs {expected} expected"
    print(f"[nb_distinct_role] \t{res[0][0]} \t------------------->  CHECK")


def _check_nb_role_not_null(verbose=0):
    expected = 191594
    sql = """SELECT count(*) as nb
            FROM creer
            WHERE creer.role is not NULL
            ORDER BY nb DESC;"""
    res = executer_sql(sql, verbose=verbose)

    assert res[0][0] >= expected, f"Le nombre de rôle renseigné a changé : {res[0][0]} vs {expected} expected"
    print(f"[nb_role_not_null]  \t{res[0][0]} \t------------------->  CHECK")    

# ----------------------------------------------------------------------------------
# %%                       MAIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    verbose = 1
    _check_nb_role(verbose=verbose)
    _check_nb_distinct_role(verbose=verbose)
    _check_nb_role_not_null(verbose=verbose)
    