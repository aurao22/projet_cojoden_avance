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
import streamlit as st
import pandas as pd
from PIL import Image
sys.path.append(r"C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance")
import folium
from bokeh.models.widgets import Button
from bokeh.models import CustomJS
from streamlit_bokeh_events import streamlit_bokeh_events
from dao.cojoden_dao_search import search_musees
from data_preprocessing.cojoden_functions import convert_string_to_search_string
from demonstrator.demonstrator import DEFAULT_VALUES, LABELS, search_musees


st.set_page_config(
     layout="wide",
     page_title="COJODEN",
     page_icon = "🎨"
 )


st.title('Recherche de musées.')
col1, col2 = st.columns([1, 1])

img_path = r'C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance\img\cojoden_app_img.png'
image = Image.open(img_path)
col1.image(image, caption='Cojoden')

col2.subheader("Merci de renseigner les éléments de votre recherche")

loc_button = Button(label="Me geolocaliser")
loc_button.js_on_event("button_click", CustomJS(code="""
    navigator.geolocation.getCurrentPosition(
        (loc) => {
            document.dispatchEvent(new CustomEvent("GET_LOCATION", {detail: {lat: loc.coords.latitude, lon: loc.coords.longitude}}))
        }
    )
    """))
result = streamlit_bokeh_events(
    loc_button,
    events="GET_LOCATION",
    key="get_location",
    refresh_on_update=False,
    override_height=75,
    debounce_time=0)


if(result != None):
    vars()["coord_lat"] = result['GET_LOCATION']['lat']
    vars()["coord_long"] = result['GET_LOCATION']['lon']
    DEFAULT_VALUES['coord_lat'] = result['GET_LOCATION']['lat']
    DEFAULT_VALUES['coord_long'] = result['GET_LOCATION']['lon']
    # st.write("lat : " + str(result['GET_LOCATION']['lat']))
    # st.write("lon : " + str(result['GET_LOCATION']['lon']))

for key, (label, help) in LABELS.items():       
    # création dynamique des variables qui contiendront la valeur du champs
    vars()[key] = col2.text_input(label=label,value=DEFAULT_VALUES.get(key, ""), key=key, help=help, on_change=None)


if col2.button("Rechercher", key="button_submit", help='Cliquez sur le bouton pour lancer la recherche.'):
  
    input_datas = {}
    for k in LABELS.keys():
        input_datas[k] = [vars()[k]]
    
    col1.title(f'Voici le résultat de la recherche :' )
    
    df = search_musees(input_datas = input_datas, verbose=1)
    
    st.dataframe(df)
    st.map(df[["latitude", "longitude"]])
