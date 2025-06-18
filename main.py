import streamlit as st
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import snowflake.snowpark.functions as f
import re
from io import StringIO
import pandas as pd
import time
from pandas.io.formats.style import Styler

# --- Snowflake Connection Setup for SSO ---
# It's highly recommended to use environment variables or a secrets file (.streamlit/secrets.toml)
# for production applications instead of hardcoding credentials.
connection_parameters = {
    "account": "WB19670-C2GPARTNERS", # e.g., 'xyz12345.us-east-1'
    "user": "AMAN.GUPTA@BLEND360.COM", # Often your email address for SSO
    "authenticator": "externalbrowser", # This enables browser-based SSO
    "role": "PUBLIC", # e.g., 'ACCOUNTADMIN' or a custom role
    "warehouse": "POWERHOUSE",
    "database": "SANDBOX", # Ensure this matches your setup
    "schema": "DS" # Ensure this matches your setup
}

# Initialize Snowpark Session
@st.cache_resource
def get_snowpark_session():
    """Establishes and caches a Snowpark session using SSO."""
    try:
        st.info("Initiating Snowflake SSO login. A browser window should open shortly.")
        session = snowpark.Session.builder.configs(connection_parameters).create()
        st.success("Successfully connected to Snowflake via SSO!")
        return session
    except Exception as e:
        st.error(f"Error connecting to Snowflake via SSO: {e}")
        st.stop() # Stop the Streamlit app if connection fails

session = get_snowpark_session()
session.use_warehouse("POWERHOUSE")
session.sql("SELECT current_warehouse()").show()
# --- Streamlit App Starts Here ---
st.title("AI Chatbot")

def process_question(session: snowpark.Session, prompt: str):
    check_df = session.createDataFrame([[prompt]], schema=["prompt"])
    has_insight = check_df.filter(f.contains(f.lower(col("prompt")), f.lit('insight')) | f.contains(f.lower(col("prompt")), f.lit('why'))).count() > 0
    table_name = 'SAMPLE_SUPER_STORE'
    if (check_df.filter(f.contains(f.lower(col("prompt")), f.lit('define'))).count()) or \
       (check_df.filter(f.contains(f.lower(col("prompt")), f.lit('calculat'))).count()) > 0:
        table_name = 'SAMPLE_SUPER_STORE_GLOSSARY'
    else:
        table_name = 'SAMPLE_SUPER_STORE'

    get_columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        AND TABLE_SCHEMA = '{connection_parameters['schema'].upper()}'
        AND TABLE_CATALOG = '{connection_parameters['database'].upper()}'
        ORDER BY ORDINAL_POSITION
        """
    try:
        columns_result = session.sql(get_columns_query).collect()
    except Exception as e:
        return f"Error fetching columns for table {table_name}: {e}. Please ensure the table and schema exist and are accessible."

    column_names = ""
    for row in columns_result:
        column_names += f"{row['COLUMN_NAME']} {row['DATA_TYPE']}, "

    db_schema_for_prompt = f'"{connection_parameters["database"].upper()}"."{connection_parameters["schema"].upper()}"'

    if has_insight:
        data_prompt = f"""SELECT SNOWFLAKE.cortex.complete(
            'claude-3-5-sonnet',
            'Translate the provided natural language query into a valid SQL query for the Snowflake database, ensuring to adhere to specific considerations for SQL generation.
            - Use only the specified tables.
            - Apply the `lower` or `upper` function to fields and text in the `WHERE` clause.
            - If no relationship exists between the tables, create separate queries.
            - Ensure that any column used in the `ORDER BY` clause is also included in the `SELECT` statement.
            # Steps
                1. Parse the provided natural language query to identify tables, columns, conditions, and any desired SQL clauses (e.g. `ORDER BY`, `WHERE`).
                2. Map the identified elements to the appropriate components of the SQL query structure.
                3. Apply the `lower` or `upper` function to fields and text in the `WHERE` clause as necessary.
                4. Determine the relationship between tables (if any) and generate either a single SQL query or separate queries for unrelated tables.
                5. If an `ORDER BY` clause is present, ensure that the ordering column is included in the `SELECT` statement.
                6. Format the final SQL query according to Snowflake SQL standards.
            # Output Format
                Produce a valid SQL query string based on the input natural language query. The SQL query should be formatted for readability, adhering to the rules provided.
            # Examples
                **Input:**
                    Natural Language Query: "Get the name and age from the users table where age is greater than 25, ordered by name."
                **Output:**
                    ```sql
                    SELECT name, age
                    FROM {db_schema_for_prompt}.users
                    WHERE age > 25
                    ORDER BY name;
                    ```
                (Note: For real examples, ensure that all referenced columns and tables are valid within the specified database schema, using placeholders like `name`, `age`, `users` accordingly.)
            # Notes
                    - Pay attention to any potential SQL injection risks and ensure the query is constructed with security best practices in mind.
                    - Validate table names and column names against the provided schema for accuracy.
                    - Consider using placeholder values for any specific data examples not provided in the input.
            Natural Language Query: {prompt}
            Database Schema: {db_schema_for_prompt}
            Tables:
            - {table_name}
            ({column_names})
            Use date format as: DD-MM-YYYY
            SQL Query:') as response;"""
    else:
        data_prompt = f"""SELECT SNOWFLAKE.cortex.complete(
            'mistral-large',
            'Translate the following natural language query into a valid SQL query for Snowflake based on below table fields
            considerations for SQL generation:
            - Use lower or upper function for fields and text in where condition.
            - Use separate queries if no relation between tables
            - Include column in select statement as well if used in "order by" clause
            Natural Language Query: {prompt}
            Database Schema: {db_schema_for_prompt}
            Tables:
            - {table_name}
            ({column_names})
            Use date format as: DD-MM-YYYY
            SQL Query:') as response;"""

    data_query = session.sql(data_prompt).collect()[0]["RESPONSE"]
    if data_query and not re.search(r"```sql", data_query, re.IGNORECASE):
        data_query = "```sql\n" + data_query + "\n```"

    data_summaries = []
    extracted_sql = re.findall(r"```SQL(.*?)```", data_query, re.DOTALL | re.IGNORECASE)
    if not extracted_sql:
        data_summaries.append("No SQL query found.")
    else:
        for sql_query in extracted_sql:
            sql_query_only = sql_query.strip()
            try:
                data_df = session.sql(sql_query_only)
                data_summary = ""
                column_headers = data_df.columns
                if column_headers:
                    data_summary += ", ".join(column_headers)
                    data_summary += "\n"
                    rows = data_df.collect()
                    if rows:
                        for row in rows:
                            row_values = [str(value) for value in row]
                            row_string = ", ".join(row_values)
                            data_summary += row_string + "\n"
                    else:
                        data_summary += "No data found for this query.\n"
                else:
                    data_summary = "No columns in the result."
                data_summaries.append(data_summary)
            except Exception as e:
                data_summaries.append(f"Error executing SQL: {e}")

    combined_data_summary = "\n".join(data_summaries)

    if has_insight:
        sql_query = f"""SELECT SNOWFLAKE.cortex.complete(
        'claude-3-5-sonnet',
        'Give me the statistical detailed insights based on the data for below query:
        Query: {prompt}
        {combined_data_summary.replace('"', ' ').replace("'", ' ')}
        Answer:') as response;"""
    elif (check_df.filter(f.contains(f.lower(col("prompt")), f.lit('define'))).count()) or \
         (check_df.filter(f.contains(f.lower(col("prompt")), f.lit('calculat'))).count()) > 0:
        sql_query = f"""SELECT SNOWFLAKE.cortex.Summarize(
        'Natural Language Query: {prompt}
        Output: {combined_data_summary.replace('"', ' ').replace("'", ' ')}
        ') as response;"""
    else:
        sql_query = f"""SELECT SNOWFLAKE.cortex.complete(
        'mistral-large',
        '
        Natural Language Query: {prompt}
        Output: {combined_data_summary.replace('"', ' ').replace("'", ' ')}
        ') as response;"""

    try:
        response_df = session.sql(sql_query).collect()
        final_response = response_df[0]["RESPONSE"] if response_df else "No response generated."
    except Exception as e:
        final_response = f"Error generating final response: {e}"

    time.sleep(1) # Simulate some processing time
    return final_response


def main(session: snowpark.Session, questions_df):
    question_column_name = 'Questions' # Assuming this is the column in your CSV
    status_message = ""
    responses = []
    try:
        if question_column_name not in questions_df.columns:
            status_message = f"Error: Column '{question_column_name}' not found in the uploaded file. Please ensure your CSV has a column named '{question_column_name}'."
            st.error(status_message)
            return None

        for index, row in questions_df.iterrows():
            question = row[question_column_name]
            st.info(f"Processing question: {question}")
            response = process_question(session, question)
            st.success(f"Answer: {response}")
            responses.append({'question': question, 'response': response})

        responses_df = pd.DataFrame(responses)
        return responses_df

    except Exception as e:
        status_message = f"An error occurred during file processing: {e}"
        st.error(status_message)
        return None


if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

uploaded_files = st.file_uploader(
    "Choose a CSV file (or type your question below)", accept_multiple_files=True
)

if uploaded_files:
    for uploaded_file in uploaded_files:
        file_content = uploaded_file.read().decode('utf-8')
        questions_df = pd.read_csv(StringIO(file_content))
        st.subheader(f"Processing file: {uploaded_file.name}")

        with st.chat_message("assistant"):
            st.write(f"Processing questions from uploaded file: **{uploaded_file.name}**")
            responses_df = main(session, questions_df)
            if responses_df is not None:
                st.subheader(f"Responses for file: {uploaded_file.name}")
                html_table = Styler(responses_df).to_html()
                st.markdown(html_table, unsafe_allow_html=True)
                st.session_state.messages.append({"role": "assistant", "content": f"Processed file {uploaded_file.name}. See table above for responses."})

prompt = st.chat_input("Ask me anything...")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.spinner("Processing question..."):
        response = process_question(session, prompt)
    with st.chat_message("assistant"):
        st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})