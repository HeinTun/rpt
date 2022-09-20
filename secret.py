# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 17:45:10 2022

@author: heinl
"""
import streamlit as st

@st.cache
def im_keys():
    AWS_ACCESS_KEY = "AKIA4GK7IHHC32JKRZSQ"
    # AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") <-- better approach; need to figure out how
    AWS_SECRET_KEY = "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"
    AWS_REGION = "us-east-1"
    return (AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION)

