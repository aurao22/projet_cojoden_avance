# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" The cojoden UI

Project: Cojoden avance
=======

Usage:
======
    python cojoden_app.py
"""
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"

# ----------------------------------------------------------------------------------
import sys
sys.path.append("C:\\Users\\User\\WORK\\workspace-ia\\PROJETS\\")
from projet_cojoden_avance.dao.cojoden_dao_search import *
from flask import Flask, render_template, request
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def form():
    return render_template('home.html')

@app.route('/hello', methods=['GET', 'POST'])
def hello():

    record = request.form
    print(record)

    

    revenu_median=record.get('revenu_median', 0)
    age_median=record.get('age_median', 0)
    nb_room_mean=record.get('nb_room_mean', 0)
    nb_bedroom_mean=record.get('nb_bedroom_mean', 0)
    population=record.get('population', 0)
    occupation_mean=record.get('occupation_mean', 0)
    latitude=record.get('latitude', 0)
    longitude=record.get('longitude', 0)

    print(revenu_median, age_median, nb_room_mean, nb_bedroom_mean, population, occupation_mean, latitude, longitude)

    predict_price = do_prediction(revenu_median, age_median, nb_room_mean, nb_bedroom_mean, population, occupation_mean, latitude, longitude)
    print(predict_price)

    return render_template('greeting.html', say=round(predict_price,2), to=predict_price,revenu_median=revenu_median, 
                        age_median=age_median, nb_room_mean=nb_room_mean, 
                        nb_bedroom_mean=nb_bedroom_mean, population=population, 
                        occupation_mean=occupation_mean, 
                        latitude=latitude, longitude=longitude)


if __name__ == "__main__":
    app.run()