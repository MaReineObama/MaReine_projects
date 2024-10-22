import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.session import Session

# Snowflake connection setup
def create_snowflake_session():
    connection_params = {
        'user':'O_MARIE',
        'password':'Snowflake2024',
        'account':'xl16363.eu-central-1',
        'warehouse':'O_MARIE_WH',
        'database':'O_MARIE_DB',
        'schema':'OUTPUTS'
    }
    return Session.builder.configs(connection_params).create()

session = create_snowflake_session()

def get_suppliers():
    supplier_df = session.table("SFC_SAMPLES.TPCH_SF1.supplier").select("S_SUPPKEY", "S_NAME").to_pandas()
    return supplier_df

def get_supplier_revenue(supplier_name):
    supplier_key = session.table("SFC_SAMPLES.TPCH_SF1.supplier").filter(f"S_NAME = '{supplier_name}'").select("S_SUPPKEY").to_pandas().iloc[0, 0]



    # Query lineitem table for revenue year-over-year
    revenue_df = session.sql(f"""
        SELECT YEAR(L_SHIPDATE) AS year, 
        SUM(L_EXTENDEDPRICE) AS revenue,
        LAG(SUM(L_EXTENDEDPRICE), 1) OVER (ORDER BY EXTRACT(YEAR FROM L_SHIPDATE)) as previous_year_sales,
        (SUM(L_EXTENDEDPRICE) - LAG(SUM(L_EXTENDEDPRICE), 1) OVER (ORDER BY EXTRACT(YEAR FROM L_SHIPDATE))) / LAG(SUM(L_EXTENDEDPRICE), 1) OVER (ORDER BY EXTRACT(YEAR FROM L_SHIPDATE)) as yoy_growth                   
        FROM SFC_SAMPLES.TPCH_SF1.LINEITEM
        WHERE L_SUPPKEY = {supplier_key}
        GROUP BY YEAR(L_SHIPDATE)
        ORDER BY year
    """).to_pandas()
    
    return revenue_df


# Set page config
st.set_page_config(layout="wide")
image_url = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSVVe-6tSjB6UNSknq9NLko2l-MqX-2Noz1GQ&s"
# Display header
st.header("Want to know more about our suppliers?")


suppliers = get_suppliers()
#sidebar image header
st.sidebar.image(image_url, width=300)

# Supplier selection dropdown
selected_supplier = st.sidebar.selectbox("Select a Supplier:", suppliers['S_NAME'])

def get_supplier_info(supplier_name):
    supplier_info = session.table("SFC_SAMPLES.TPCH_SF1.supplier").filter(f"S_NAME = '{supplier_name}'").to_pandas()
    return supplier_info

supplier_info = get_supplier_info(selected_supplier)
supplier_info = supplier_info[['S_NAME', 'S_ADDRESS', 'S_PHONE','S_COMMENT']].rename(columns={'S_NAME': 'Name', 'S_ADDRESS': 'Address', 'S_PHONE': 'Phone Number','S_COMMENT': 'Client review'})
st.write("#### Supplier Information")
st.write(supplier_info)

revenue_df = get_supplier_revenue(selected_supplier)

st.write("#### 📈 Total Revenue Over the Years")
line_chart = alt.Chart(revenue_df).mark_line(point=alt.OverlayMarkDef(filled=True, size=100)).encode(
    x=alt.X('YEAR:O', title='Year'),
    y=alt.Y('REVENUE:Q', title='Revenue', axis=alt.Axis(format='$,f')),
    tooltip=['YEAR', alt.Tooltip('REVENUE', format='$,.2f')]
    ).properties(
        width=600,
        height=400
    ).configure_title(fontSize=16, anchor='middle')

st.altair_chart(line_chart, use_container_width=True)

# line chart for Year-over-Year Growth
st.write("#### 📈 Year-over-Year Growth:")
yoy_growth_chart = alt.Chart(revenue_df).mark_line(point=True, color='orange').encode(
    x=alt.X('YEAR:O', title='Year'),
    y=alt.Y('YOY_GROWTH:Q', title='YoY Growth (%)', axis=alt.Axis(format='%')),
    tooltip=['YEAR', 'YOY_GROWTH']
    ).properties(
        width=700,
        height=400
    )
st.altair_chart(yoy_growth_chart)


#Create a list of years for the select box
years = revenue_df.YEAR.unique()

# Create side-by-side drop-downs
col1, col2 = st.sidebar.columns(2)

# Add the start year drop-down in the first column
start_year = col1.selectbox('Start Year', years, index=5, key='start_year')

# Add the end year drop-down in the first column
end_year = col2.selectbox('End Year', years, index=len(years) - 1, key='end_year')

# Ensure that the start year is less than or equal to the End Year
if start_year > end_year:
    st.error("Start Year must be less than or equal to End Year")

# Create the form
st.write("#### 📝 We would like your opinion about our suppliers 🙂")
with st.form("grade_form"):
    grade = st.slider("Grade the supplier (1 to 10)", 1, 10)
    comment = st.text_area("Add a comment if you have one")
    submit_button = st.form_submit_button("Submit")

# Store the grade and comment in a table upon submission
if submit_button:
    session.sql(f"""
        INSERT INTO SUPPLIER_GRADES (S_SUPPKEY, GRADE, COMMENT)
        VALUES (
            (SELECT S_SUPPKEY FROM SUPPLIERS WHERE S_NAME = '{selected_supplier}'), 
            {grade}, 
            '{comment}'
        )
    """)
    st.success("Grade and comment submitted!")




