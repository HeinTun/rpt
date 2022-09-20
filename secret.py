# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 17:45:10 2022

@author: heinl
"""
import streamlit as st

@st.cache
def im_keys():
    # AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") <-- better approach; need to figure out how
    return (st.secrets["AWS_ACCESS_KEY"], st.secrets["AWS_SECRET_KEY"], "us-east-1")

