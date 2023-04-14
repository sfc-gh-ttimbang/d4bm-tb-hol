# Snowpark
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium, folium_static


# Create Session object
def create_session_object():
    connection_parameters = {
        "account": st.secrets.sf_credentials.account,
        "user": st.secrets.sf_credentials.user,
        "password": st.secrets.sf_credentials.password,
        "role":"TASTY_DATA_ENGINEER",
        "warehouse":"TASTY_DE_WH"
    }
    session = Session.builder.configs(connection_parameters).create()
    print(session.sql('select current_warehouse()').collect())
    return session

if __name__ == "__main__":
    st.title("Tasty Bytes: Visualizing Geospatial Data")
    session = create_session_object()

    #Query for top 10 locations
    df = pd.DataFrame(session.sql("SELECT TOP 10 o.location_id, o.location_name, o.longitude, o.latitude, SUM(o.price) AS total_sales_usd FROM frostbyte_tasty_bytes.analytics.orders_v o WHERE 1=1 AND o.primary_city = 'Paris' AND YEAR(o.date) = 2022 GROUP BY 1,2,3,4 ORDER BY total_sales_usd DESC").to_pandas())
    
    st.header("Top 10 Areas by Total Sales")
    st.subheader("Raw Data:")
    st.table(df)

    m = folium.Map(location=[df.LATITUDE.mean(), df.LONGITUDE.mean()], zoom_start=13, control_scale=True)

    #Loop through each row in the dataframe
    for i,row in df.iterrows():
        #Setup the content of the popup
        iframe = folium.IFrame('Location Name:' + str(row["LOCATION_NAME"]) + '<br><br>' + 'Total Sales:' + str(row["TOTAL_SALES_USD"]))
        
        #Initialise the popup using the iframe
        popup = folium.Popup(iframe, min_width=300, max_width=300)
        
        #Add each row to the map
        folium.Marker(location=[row['LATITUDE'],row['LONGITUDE']],
                    popup = popup, c=row['LOCATION_NAME']).add_to(m)
    
    # Draw the map
    st.subheader("Map View:")
    st_data = folium_static(m, width=700)



    # Section 2 - Bounding Area
    st.header("Minimum Bounding Area")
    df1 = pd.DataFrame(session.sql("WITH _top_10_locations AS ( SELECT TOP 10 o.location_id, ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point, SUM(o.price) AS total_sales_usd FROM frostbyte_tasty_bytes.analytics.orders_v o WHERE 1=1 AND o.primary_city = 'Paris' AND YEAR(o.date) = 2022 GROUP BY o.location_id, o.latitude, o.longitude ORDER BY total_sales_usd DESC ) SELECT ST_NPOINTS(ST_COLLECT(tl.geo_point)) AS count_points_in_collection, ST_COLLECT(tl.geo_point) AS collection_of_points, ST_ENVELOPE(collection_of_points) AS minimum_bounding_polygon, ROUND(ST_AREA(minimum_bounding_polygon)/1000000,2) AS area_in_sq_kilometers FROM _top_10_locations tl").to_pandas())
    st.subheader("Raw Data:")
    st.table(df1)
    df1a = pd.read_json(df1['MINIMUM_BOUNDING_POLYGON'][0])

    bounding_coords = list()
    for i in df1a["coordinates"][0]:
        bounding_coords.append((i[1],i[0]))

    folium.PolyLine(bounding_coords, tooltip="Minimum Bounding Area").add_to(m)

    st.subheader("Map View:")
    st_data = folium_static(m, width=700)

    # Section 2 - Finding our Top Selling Locations Center Point
    st.header("Top Selling Locations Center Point")
    df2 = pd.DataFrame(session.sql("WITH _top_10_locations AS ( SELECT TOP 10 o.location_id, ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point, SUM(o.price) AS total_sales_usd FROM frostbyte_tasty_bytes.analytics.orders_v o WHERE 1=1 AND o.primary_city = 'Paris' AND YEAR(o.date) = 2022 GROUP BY o.location_id, o.latitude, o.longitude ORDER BY total_sales_usd DESC ) SELECT ST_COLLECT(tl.geo_point) AS collect_points, ST_CENTROID(collect_points) AS geometric_center_point FROM _top_10_locations tl;").to_pandas())
    st.subheader("Raw Data:")
    st.table(df2)
    st.subheader("test")
    st.text(df2['GEOMETRIC_CENTER_POINT'][0])
    st.subheader("test2")
    st.text(type(df2['GEOMETRIC_CENTER_POINT'][0]))
    st.subheader("test3")
    df2a = pd.read_json(df2['GEOMETRIC_CENTER_POINT'][0])
    st.table(df2a)
    st.text(df2a['coordinates'][1])
    st.text(df2a['coordinates'][0])

    #df2['coordinates']

    ## Add center point
    #iframe = folium.IFrame('Top Sales Center Point')
    #popup = folium.Popup(iframe, min_width=200, max_width=200)
    #folium.Marker(location=[row['LATITUDE'],row['LONGITUDE']],
    #                popup = popup, c=row['LOCATION_NAME']).add_to(m)