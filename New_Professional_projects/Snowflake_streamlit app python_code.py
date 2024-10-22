import streamlit as st
import altair as alt
import snowflake.connector
import pandas as pd

# Function to connect to Snowflake
def snowflake_connection():
    return snowflake.connector.connect(
        user='O_MARIE',
        password='Snowflake2024',
        account='xl16363.eu-central-1',
        warehouse='O_MARIE_WH',
        database='O_MARIE_DB',
        schema='OUTPUTS'
    )
image_url = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSVVe-6tSjB6UNSknq9NLko2l-MqX-2Noz1GQ&s"

# Example Snowflake query function
def execute_query(query):
    conn = snowflake_connection()
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

def create_table_if_not_exists():
    # SQL query to create the table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS O_MARIE_DB.OUTPUTS.supplier_grades (
        S_SUPPKEY STRING,
        grade INT,
        comment STRING
    );
    """
    # Execute the query
    execute_query(create_table_query)

st.set_page_config(layout="wide")
# Display header
st.header("Want to know more about our suppliers?")

# Query to get supplier names
suppliers_query = "SELECT S_SUPPKEY, S_NAME FROM SFC_SAMPLES.TPCH_SF1.supplier;"
suppliers_data = execute_query(suppliers_query)

# Convert result into a Pandas DataFrame
suppliers_df = pd.DataFrame(suppliers_data, columns=['S_SUPPKEY', 'S_NAME'])

#sidebar image header
st.sidebar.image(image_url, width=300)

# Supplier selection dropdown
selected_supplier = st.sidebar.selectbox("Select a Supplier:", suppliers_df['S_NAME'])

# Get the selected supplier's key
selected_suppkey = suppliers_df[suppliers_df['S_NAME'] == selected_supplier]['S_SUPPKEY'].values[0]

# Query to fetch supplier information based on selected supplier
supplier_info_query = f"""
SELECT * FROM SFC_SAMPLES.TPCH_SF1.supplier WHERE S_SUPPKEY = '{selected_suppkey}';
"""
supplier_info = execute_query(supplier_info_query)
supplier_info_df = pd.DataFrame(supplier_info, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBALL', 'S_COMMENT'])[['S_NAME', 'S_ADDRESS', 'S_PHONE','S_COMMENT']]
supplier_info_df = supplier_info_df.rename(columns={'S_NAME': 'Name', 'S_ADDRESS': 'Address', 'S_PHONE': 'Phone Number','S_COMMENT': 'Client review'})

# Display supplier information
st.write("#### 🏢 Supplier Information")
st.write(supplier_info_df)

# Query to calculate supplier revenue year-over-year
revenue_query = f"""
SELECT YEAR(L_SHIPDATE) AS Year, 
        SUM(L_EXTENDEDPRICE) AS Revenue,
        LAG(SUM(L_EXTENDEDPRICE), 1) OVER (ORDER BY EXTRACT(YEAR FROM L_SHIPDATE)) as Previous_year_sales,
        (SUM(L_EXTENDEDPRICE) - LAG(SUM(L_EXTENDEDPRICE), 1) OVER (ORDER BY EXTRACT(YEAR FROM L_SHIPDATE))) / LAG(SUM(L_EXTENDEDPRICE), 1) OVER (ORDER BY EXTRACT(YEAR FROM L_SHIPDATE)) as Yoy_growth                   
        FROM SFC_SAMPLES.TPCH_SF1.LINEITEM
        WHERE L_SUPPKEY = {selected_suppkey}
        GROUP BY YEAR(L_SHIPDATE)
        ORDER BY year
"""
revenue_data = execute_query(revenue_query)
revenue_df = pd.DataFrame(revenue_data, columns=['Year', 'Revenue', 'Previous_year_sales', 'Yoy_growth'])


st.write("#### 📈 Total Revenue Over the Years")
line_chart = alt.Chart(revenue_df).mark_line(point=alt.OverlayMarkDef(filled=True, size=100)).encode(
    x=alt.X('Year:O', title='Year'),
    y=alt.Y('Revenue:Q', title='Revenue', axis=alt.Axis(format='$,f')),
    tooltip=['Year', alt.Tooltip('Revenue', format='$,.2f')]
    ).properties(
        width=600,
        height=400
    ).configure_title(fontSize=16, anchor='middle')

st.altair_chart(line_chart, use_container_width=True)

# line chart for Year-over-Year Growth
st.write("#### 📈 Year-over-Year Growth:")
yoy_growth_chart = alt.Chart(revenue_df).mark_line(point=True, color='orange').encode(
    x=alt.X('Year:O', title='Year'),
    y=alt.Y('Yoy_growth:Q', title='YoY Growth (%)', axis=alt.Axis(format='%')),
    tooltip=['Year', 'Yoy_growth']
    ).properties(
        width=700,
        height=400
    )
st.altair_chart(yoy_growth_chart)


#Create a list of years for the select box
years = revenue_df.Year.unique()

# Create side-by-side drop-downs
col1, col2 = st.sidebar.columns(2)

# Add the start year drop-down in the first column
start_year = col1.selectbox('Start Year', years, index=5, key='start_year')

# Add the end year drop-down in the first column
end_year = col2.selectbox('End Year', years, index=len(years) - 1, key='end_year')

# Ensure that the start year is less than or equal to the End Year
if start_year > end_year:
    st.error("Start Year must be less than or equal to End Year")

st.write("#### 📝 We would like your opinion about our suppliers 🙂")

# Grading form
with st.form(key='grading_form'):
    grade = st.slider("Grade the supplier (1 to 10):", 1, 10)
    comment = st.text_area("Additional comments:")

    # Submit button
    submit_button = st.form_submit_button(label="Submit")

# Save the form data
if submit_button:
    #create the table to save supplier grade if that table does not exist
    create_table_if_not_exists()
    # Query to insert grade and comment into the table
    insert_query = f"""
    INSERT INTO O_MARIE_DB.OUTPUTS.supplier_grades (S_SUPPKEY, grade, comment)
    VALUES ('{selected_suppkey}', {grade}, '{comment}');
    """
    execute_query(insert_query)
    st.success("Grading submitted successfully!")



