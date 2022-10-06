# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 17:51:27 2022

@author: heinl
"""
import streamlit as st
import pandas as pd
import osmnx as ox
from shapely import wkt
from shapely.geometry import Point
import folium
import geopandas as gpd
import plotly.express as px

@st.cache
def argum(x): #x assumes a dataframe
    """Extract WKT from OpenStreetMap using OSMNx, given the name of cities"""
    try:
        return ox.geocoder.geocode_to_gdf(x).to_wkt()["geometry"].values[0]
    except:
        return ox.geocoder.geocode_to_gdf(x, which_result=2).to_wkt()["geometry"].values[0]


def selected_boundaries(city_df):
    """city_df is a dataframe output from argum(x). Should have two columns: 1) city name and 2) 'WKT' 
    returns a folium map with user selected boundaries"""
    df = city_df.copy()
    df["geometry"] = df.WKT.apply(wkt.loads)
    gdf= gpd.GeoDataFrame(df, geometry="geometry")
    
    bounds = gdf.total_bounds
    loc_bound = gdf.bounds

    m = folium.Map(tiles='CartoDB positron')
    for i, r in df.iterrows():
    # Without simplifying the representation,the map might not be displayed
        sim_geo = gpd.GeoSeries(r['geometry']).simplify(tolerance=0.001)
        geo_j = sim_geo.to_json()
        geo_j = folium.GeoJson(data=geo_j,
                           style_function=lambda x: {'fillColor': 'orange'})
        geo_j.add_to(m)
        
        m.fit_bounds([[bounds[1],bounds[0]], [bounds[3],bounds[2]]])

        #m.fit_bounds([[loc_bound.iloc[i][1],loc_bound.iloc[i][0]], 
        #   [loc_bound.iloc[i][3],loc_bound.iloc[i][2]]])

    m.save("map.html")


@st.cache
def get_shape(m,radius=10000): #m is a dataframe with LONG and LAT INFO, #radius = 10km
    long, lat = m.LONG, m.LAT
    poi = (long, lat)
    projected_point, projection_crs = ox.projection.project_geometry(Point(poi))
    projected_buffer = projected_point.buffer(radius)
    buffer, _ = ox.projection.project_geometry(projected_buffer, crs=projection_crs, to_latlong=True)
    
    meh = 'POLYGON (('
    for i in np.asarray(buffer.exterior.coords):
        meh +=  str(i[0]) + " " + str(i[1]) + ", "
    meh = meh[:-2] + "))"
    return meh


@st.cache
def res_to_df(results):
    """
    takes query output (for aggregated highway tags) results from athena, and
    output highway df.
    """
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    listed_res = []
    for res in results["ResultSet"]["Rows"][1:]:
        values = []
        for field in res["Data"]:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(""))
        listed_res.append(dict(zip(columns,values)))
    k = pd.DataFrame(listed_res)
    k["total_km"] = pd.to_numeric(k["total_km"])
    return k


@st.cache
def convert_df_to_csv(df):
    return df.to_csv().encode("utf-8")


@st.cache
def plot_roadlength(df):
    """
    takes df (from res_to_df) and create a bar plot fig.
    """
    fig = px.bar(df, x="city", y="total_km", color="highway", barmode="group",
                  labels={"total_km": "Road Length (km)", "highway": "Road Type", "city": "City"},
                  color_discrete_sequence=px.colors.diverging.curl)
    fig.update_layout(yaxis=dict(showgrid=False))
    return fig

@st.cache
def a_create_table(database, map_name, loc): #assume the file is .tsv
    create_table = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {0}.{1}
    (Index tinyint,
    CITY string, 
    WKT string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
    WITH SERDEPROPERTIES ( 'serialization.format' = '\t', 'field.delim' = '\t' )
    LOCATION '{2}'
    TBLPROPERTIES ('skip.header.line.count'='1', 'has_encrypted_data'='false')
    """.format(database, map_name, loc)
    return create_table


@st.cache
def athena_road_aggregate(database,map_name):
    daylight = "daylight_osm_features"  #To occassionally update this file!
    query = """
    SELECT c.city,
       tags [ 'highway' ] AS highway,
       tags [ 'cycleway' ] AS cycleway,
       sum(linear_meters) / 1000 AS total_km
    FROM {0}.{1} AS d

    JOIN {2} AS c
    ON ST_CONTAINS(
        ST_GeometryFromText(c.wkt),
        ST_Point(
            (min_lon + max_lon) / 2,
            (min_lat + max_lat) / 2))
    WHERE release = 'v1.13'
        AND d.linear_meters > 0
        AND (tags[ 'highway'] IS NOT NULL OR tags[ 'cycleway'] IS NOT NULL)
    GROUP BY tags [ 'highway' ], tags [ 'cycleway'], c.city
    ORDER BY c.city, highway, cycleway
    """.format(database, daylight, map_name)
    return query