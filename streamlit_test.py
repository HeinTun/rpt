import streamlit as st

#from streamlit_folium import st_folium
import streamlit.components.v1 as components

import pandas as pd
import numpy as np

import datetime
import time

import boto3
import s3fs

from helper import *

import plotly.express as px


st.set_page_config(layout="wide")

st.title("Street Supply Tool")

with st.sidebar:
    
    st.sidebar.header("ABOUT")
    st.sidebar.info(
        """
        How many kilometers of roads within a city are highways? 

        Extracting data from OpenStreetMap (OSM), Street Supply Tool allows users to select arbitary polygons (cities, towns, etc), and returns the aggregated values of various road hierarchies. 
        """)

    st.sidebar.header("WHY?")
    st.sidebar.info(
        """
        Understanding road network length by type within a given area gives us a metric to understand street supply. We can begin to quantify and compare how much of a region is made up of different types of roads.
        Some roads have good associations for sustainable mobility (transit lanes :metro: :bus: :bike:); other road types have poor associations for sustainable mobility (highways :car:). 
    
        By being able to classify the amount of different types of roads in a variety of areas, we can compare road supply type in different cities.
        """)
    
    st.sidebar.markdown("-----")
    st.sidebar.text("Contact: ")
    st.sidebar.text("Thet Hein Tun (thet.tun@wri.org);")
    st.sidebar.text("Adam Davidson (adam.davidson@wri.org)")
    



aws_k = (st.secrets["AWS_ACCESS_KEY"], st.secrets["AWS_ACCESS_KEY_ID"], "us-east-1")
# Boto and Athena/S3/S3F3
athena_client = boto3.client("athena", aws_access_key_id=aws_k[0], aws_secret_access_key=aws_k[1], region_name=aws_k[2])
s3_client = boto3.client("s3", aws_access_key_id=aws_k[0], aws_secret_access_key=aws_k[1], region_name=aws_k[2])
fs = s3fs.S3FileSystem(key = aws_k[0], secret= aws_k[1], client_kwargs={'region_name':aws_k[2]})

if 'line' not in st.session_state:
    st.session_state['line'] = ""
    st.session_state['qtime'] = ""
    st.session_state['form1'] = 0
    st.session_state['disabled'] = False
    st.session_state['t'] = pd.DataFrame()
    st.session_state['t1'] = ""
    

def form_callback():
    return st.session_state.line, st.session_state.qtime, st.session_state.t

def disable():
    st.session_state["disabled"] = True

def to_stop():
    return st.stop()


## STEP 1: ASK USER INPUT FOR CITY OR A REGION

col1, col2, col3 = st.columns(3)
col4, mid, col5, mid2, col6 = st.columns([1.2,0.1,2.2,0.1, 3])

with col1:

# Approach 1: BY NAME (One City)

    with st.form("my_form"):
        st.write("Which cities :cityscape: are you interested in?")
        
        with st.expander("Tip!"):
            st.markdown("""<p style='font-family:monospace; font-size: 12.5px;'>
                We are pulling data from OSM, which might not understand your syntax! Try one city at a time (initially) to see if you are pulling the right boundaries.
                See <a href="https://www.openstreetmap.org/search?query=Havana%2C%20Cuba">this</a> as an example.</p>""", True)
        
        line = st.text_area("Type your 'City, Country' of interest (one per line):", "Washington DC, USA\nHavana, Cuba",  # Default
                           key="city_name", disabled=st.session_state.disabled)
        

        submitted = st.form_submit_button("Submit",on_click=form_callback)

        if submitted or st.session_state.form1:
            city_name = [x for x in line.split("\n") if x!= ""]
            st.session_state['form1'] = 1
            


## STEP 2: GET POLYGON(S) WKT
if st.session_state.form1:
    cities_df = pd.DataFrame(city_name, columns=["City"])
    cities_df["WKT"] = cities_df["City"].apply(lambda x: argum(x)) #calling argum function to calculate Polygon
    
    
    with col2:
        st.subheader("City Boundaries:")
        st.dataframe(cities_df)
    
    with col3:
        m = selected_boundaries(cities_df)
        HtmlFile = open("map.html", 'r', encoding='utf-8')
        st.markdown('###')  
        components.html(HtmlFile.read(), height=300, width= 300, scrolling=True)

    with col4:
        st.markdown("###")
        st.markdown('<p style="color: #FCD900">If you are <strong>READY</strong> to start the query process, please provide a unique name: </p>', True)
    
    with col5: 
        st.markdown("###")
        text_input_container = st.empty()
        map_name = text_input_container.text_input("No space or dash (-) allowed; please use underscore'_'.", on_change=disable)
    
        if map_name != "":
            text_input_container.empty()
            st.info(map_name)

st.markdown("""<hr style="height:1px;border:none;background-color:#E98300;" /> """, True)



## STEP 3: SAVE DF TO S3 (AWS) as .tsv
if st.session_state["disabled"] and st.session_state["qtime"] == "":
    with col6:
        st.markdown("#")
        st.text("Query begins ...")
    
    filename= "app-1-" + datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S%f') + ".tsv"
    prefix = "street-supply/web-version/user-upload/" + filename[:-4] + "/"
    bucketname = "cities-data"

    with fs.open(bucketname+"/"+prefix+filename,"w") as f:
        cities_df.to_csv(f, sep="\t")



## STEP 4: CALL THE SAVED FILE FROM S3 TO ATHENA
    loc = "s3://" + bucketname + "/" + prefix
    database = "default"
    map_name = "osm_"+ map_name + "_" + str(np.random.randint(100,999))
    create_table= a_create_table(database, map_name, loc)

    input_table = athena_client.start_query_execution(
                    QueryString=create_table,
                    QueryExecutionContext={"Database": database},
                    ResultConfiguration={"OutputLocation": loc,})

    while True:
        try:
            table_results = athena_client.get_query_results(
                QueryExecutionId=input_table["QueryExecutionId"])
            break
        except Exception as err:
            if "Query has not yet finished" in str(err):
                    time.sleep(0.1)
            else:
                raise err      
    #table_results["ResponseMetadata"]["HTTPStatusCode"]
    


## STEP 5: QUERY!
    query = athena_road_aggregate(database,map_name)
    query_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": loc+"query_results/",}) # raw results are saved here in AWS too
    
    s = datetime.datetime.now()
    
    while True:
        try:
            results = athena_client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"])
            break
        except Exception as err:
            if "Query has not yet finished" in str(err):
                with st.spinner("Scanning data..."):
                        time.sleep(25)
            else:
                raise err
    t_d = str(datetime.datetime.now()-s).split(":")
    
    st.session_state.qtime = "Your query took {0} minute(s) and {1} seconds. :sparkles:".format(t_d[1], t_d[2][:6])
    #results["ResponseMetadata"]["HTTPStatusCode"]



## STEP 6: DISPLAY/SAVE QUERY RESULTS
if st.session_state["disabled"]:
    st.subheader("Road Lengths (km):")

    with st.expander("Results"):

        st.session_state.qtime

        if st.session_state["disabled"] and st.session_state["t"].empty:
            st.session_state["t"] = res_to_df(results)
            st.session_state["t1"] = st.session_state["t"]          

        # this part uses session_state to avoid re-calculation.
        #t = st.session_state["t1"].pivot(index= "city", columns="highway", values="total_km")
        t = st.session_state["t1"]
        #st.dataframe(t.style.format("{:.2f}"))

        ## Download button
        csv_file = convert_df_to_csv(t) 
        st.download_button(label="Download CSV", data = csv_file, file_name= "query.csv", mime="text/csv", on_click=form_callback)

        ## Plotting
        df = st.session_state["t1"]

        fig = px.bar(df, x="city", y="total_km", color="highway", barmode="group",
                  labels={"total_km": "Road Length (km)", "highway": "Road Type", "city": "City"},
                  color_discrete_sequence=px.colors.diverging.curl)
        fig.update_layout(yaxis=dict(showgrid=False))
        st.plotly_chart(fig)
