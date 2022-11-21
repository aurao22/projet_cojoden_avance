# %% doc
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# """ The cojoden UI

# Project: Cojoden avance
# =======

# Usage:
# ======
#     streamlit run c:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance\demonstrator\cojoden_app_streamlit.py
# """
__authors__     = ("Aurélie RAOUL")
__contact__     = ("aurelie.raoul@yahoo.fr")
__copyright__   = "MIT"
__date__        = "2022-10-01"
__version__     = "1.0.0"

# ----------------------------------------------------------------------------------
import streamlit as st
from PIL import Image
import sys
from os import getcwd
from os.path import join
execution_path = getcwd()

if 'PROJETS' not in execution_path:
    execution_path = join(execution_path, "PROJETS")
if 'projet_cojoden_avance' not in execution_path:
    execution_path = join(execution_path, "projet_cojoden_avance")

print(f"[cojoden_app_streamlit] execution path= {execution_path}")
sys.path.append(execution_path)
from bokeh.models.widgets import Button
from bokeh.models import CustomJS
from streamlit_bokeh_events import streamlit_bokeh_events
import demonstrator as demo


st.set_page_config(
     layout="wide", # "wide" or "centered"
     page_title="COJODEN",
     page_icon = "🎨"
 )

# st.title('Cojoden - Recherche de musées')

col1, col2 = st.columns((1, 2))

img_path = r'C:\Users\User\WORK\workspace-ia\PROJETS\projet_cojoden_avance\img\cojoden_app_img.png'
image = Image.open(img_path)
col1.image(image)

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
    # refresh_on_update=False,
    override_height=50,
    debounce_time=0)


if(result != None):
    vars()["coord_lat"] = result['GET_LOCATION']['lat']
    vars()["coord_long"] = result['GET_LOCATION']['lon']
    demo.DEFAULT_VALUES['coord_lat'] = result['GET_LOCATION']['lat']
    demo.DEFAULT_VALUES['coord_long'] = result['GET_LOCATION']['lon']

col1.subheader("Merci de renseigner les éléments de votre recherche")

for key, (label, help) in demo.MUSEES_LABELS.items():       
    # création dynamique des variables qui contiendront la valeur du champs
    vars()[key] = col1.text_input(label=label,value=demo.DEFAULT_VALUES.get(key, ""), key=key, help=help, on_change=None)
    # Il faut traiter uniquement les valeurs qui sont renseignées
    if vars()[key] is not None and len(vars()[key].strip()) == 0:
        vars()[key] = None
    

if col1.button("Rechercher", key="button_submit", help='Cliquez sur le bouton pour lancer la recherche.'):
  
    col2.subheader("Recherche...")
    input_datas = {}
    for k in demo.MUSEES_LABELS.keys():
        input_datas[k] = vars()[k]
    
    try:
        df = demo.search_musees(input_datas = input_datas, verbose=2)
        if df.shape[0]>0:
            col2.dataframe(df[["museo", "nom", "ville", "nb_oeuvres", "nb_artistes", "distance"]])
            col2.map(df[["latitude", "longitude"]])
        else:
            col2.text("Aucun résultat trouvé avec ces critères, merci de modifier votre recherche.")
    except Exception as error:
        col2.code(error)
