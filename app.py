"""
Ohio 2026 Election Model — dashboard.

Run with: /usr/bin/python3 -m streamlit run app.py

Four pages, all reading the same model (gui/projection.py):
  Current Projection · Polling Averages · Districts & Races · Election Night
"""

import streamlit as st

st.set_page_config(page_title="Ohio 2026 Forecast", page_icon=":ballot_box:", layout="wide")

pages = [
    st.Page("views/current_projection.py", title="Current Projection", icon=":material/insights:", default=True),
    st.Page("views/polling_averages.py", title="Polling Averages", icon=":material/show_chart:"),
    st.Page("views/districts_and_races.py", title="Districts & Races", icon=":material/map:"),
    st.Page("views/election_night.py", title="Election Night", icon=":material/how_to_vote:"),
]

with st.sidebar:
    st.markdown("**Ohio 2026 Forecast**")
    st.caption("Governor, US Senate, statewide offices, Ohio House, Ohio Senate, US House")

st.navigation(pages).run()
