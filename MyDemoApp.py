#---> STEP 1
import numpy as np
import json
import random

import streamlit as st
import snowflake.snowpark.functions as f
from snowflake.snowpark import Session

'## Customer Analytics'
st.subheader('Snowflake/Snowpark Rocks!!!')

#---> STEP 2 (Connect to Snowflake)
with open("/Users/nakincilar/Streamlit-VS/MyFirstApp/sf_creds.json") as c:
    connection_params = json.load(c)['secrets']

@st.experimental_singleton
def init_connection():
    st = Session.builder.configs(connection_params).create()
    return st

session = init_connection()

#---> STEP 3A (Define Dataframe Tables via Snowflake)
df_Customers = session.table("DEMO_DEPLOYMENT.PUBLIC.CUSTOMERS_HQ")  # 4K Rows

df_Orders = session.table("DEMO_DEPLOYMENT.PUBLIC.ORDERS_HQ")  # 207M Rows

#---> STEP 3B (Join 2 dataframes)
df_Sales = df_Orders.join(df_Customers, df_Orders.col("CUSTOMERID") == df_Customers.col("CUSTID"))  # JOIN TABLES

#---> STEP 3C (Summarize Sales by Customer)
df_SalesSummary = df_Sales.groupBy("CUSTID", "NAME", "STATE", "EMAIL", "CITY", "LAT", "LONG", "GENDER" ).agg([
    f.sum( f.column("QUANTITY") * f.column("UNITPRICE") ).alias("TotalSales") 
]).sort(f.column("TotalSales").desc())

#---> STEP 4 (Group Customers by State to get a distinct state list and fetch results to pandas df)
df_states = df_Customers.groupBy("STATE").count().select(f.col("STATE")).to_pandas()  # <--- 1st SF Query Executes

state_list = np.append(['ALL'], df_states)  # <-- Append 'ALL' option infront of all the states


#---> STEP 5 (Add StreamLit Dropdown populated with statelist and query SF with the selection)

#input variable
state_name = st.sidebar.selectbox("Pick A State", state_list)


if state_name == 'ALL':
    data_pd = df_SalesSummary.to_pandas() 
else:
    data_pd = df_SalesSummary.filter(f.col("STATE") == state_name).to_pandas()

data_pd = data_pd.rename(columns={"LONG":"lon", "LAT":"lat"})   




#---> STEP 6 (Add StreamLit KPI objects for Sales amount & customer count)

# -- Generate 2 random % numbers to use with metricKPI

some_rand_percent1 = "{:.1%}".format(round(random.uniform(-1.00,1.00),1))
some_rand_percent2 = "{:.1%}".format(round(random.uniform(-1.00,0.1),1))

total_sales = "${:,.0f}".format(data_pd["TOTALSALES"].sum())
cust_count = len(data_pd)

col1, col2 = st.columns(2)

with col1:
    st.metric("Number of customers in %s" %state_name, cust_count, some_rand_percent1)

with col2:
    st.metric("Total Sales in %s" %state_name, total_sales, some_rand_percent2)

#data_pd


#---> STEP 7-A ( Add Top N selection & A Checkbox to optionally display report)
top_n = st.sidebar.slider("First N Customers", 3,20,5,1)

if st.checkbox("Show customer details"): 
    data_pd_view = data_pd.head(top_n) 
    data_pd_view


#---> STEPS 9-11 (Add tabs)
tab1, tab2, tab3 = st.tabs(["Basic Map", "Advanced Map", "Sales by City"])


#---> STEP 8-A (Add Basic Map1)    
with tab1:
    st.map(data_pd)

#---> STEP 8-B (Add Advanced Map) 

import pydeck as pdk

with tab2:
    st.pydeck_chart(pdk.Deck(

     map_style=None,
     initial_view_state=pdk.ViewState(
         latitude = data_pd["lat"].mean(),
         longitude = data_pd["lon"].mean(),
         zoom=6,
         pitch=50,
     ),
     layers=[
         pdk.Layer(
            'HexagonLayer',
            data=data_pd,
            get_position='[lon, lat]',
            radius=2000,
            elevation_scale=10,
            elevation_range=[0, 20000],
            pickable=True,
            extruded=True,
         ),
         pdk.Layer(
             'ScatterplotLayer',
             data=data_pd,
             get_position='[lon, lat]',
             get_color='[200, 30, 0, 160]',
             get_radius=2000,
         ),
     ],
 ))

#---> STEPS 9-11 (Add Barchart as a new tab)
with tab3:
    df_salesHistory = data_pd[['CITY', 'TOTALSALES']].groupby(['CITY']).sum().reset_index()
    st.bar_chart(df_salesHistory, x='CITY', y='TOTALSALES')
