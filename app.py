import streamlit as st
import pandas as pd
from io import BytesIO
import pyodbc
import time

def to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    processed_data = output.getvalue()
    return processed_data


@st.cache_data(show_spinner="Querying database...")
def foo_cached(data_list, _message_placeholder):

    DB_SERVER = st.secrets["db_server"]
    DB_NAME = st.secrets["db_name"]
    DB_USER = st.secrets["db_user"]
    DB_PASSWORD = st.secrets["db_password"]
     
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
            "TrustServerCertificate=yes;"
        )
    except pyodbc.Error as e:
        _message_placeholder.error(f"Database connection error: {e}")
        time.sleep(2)
        _message_placeholder.empty()
        st.stop()
        return

    cursor = conn.cursor()
    
    placeholders = ','.join(f"'{id}'" for id in data_list)

    query = f'''
    SELECT c.identifiers_opensrp_id, c.site_code, c.gender, c.firstname
    FROM vital_target.vr.client c
    WHERE c.identifiers_opensrp_id IN ({placeholders});
    '''
    
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    df = pd.DataFrame.from_records(rows, columns=columns)

    cursor.close()
    conn.close()
    
    return df


def main():
    st.title("File Upload and Download App")

    # File upload
    uploaded_file = st.file_uploader("Upload a file", type=["xlsx"])

    if uploaded_file is not None:
        message_placeholder = st.empty()
        message_placeholder.success(f"Uploaded file: {uploaded_file.name}")
        time.sleep(1.5)
        message_placeholder.empty()

        if uploaded_file.name.endswith('.xlsx'):
            df_input = pd.read_excel(uploaded_file)
        else:
            df_input = pd.read_csv(uploaded_file)

        if df_input.empty:
            st.error("The uploaded file is empty. Please upload a valid file.")
            return
        col = df_input.columns[0]

        if col.startswith('Unnamed'):
            st.error("The ID column should start from the first column with a column valid name.")
            return
        elif df_input[col].isnull().all():
            st.error("The ID column is empty. Please upload a file with valid IDs.")
            return

        data = df_input[col].dropna()
        data = data.apply(lambda x: x.replace("'", "").strip()).unique().tolist()

        # Call the cached function
        df = foo_cached(data,message_placeholder)
        if type(df) is pd.DataFrame:
            st.write("File content preview")
            st.dataframe(df.head())

            if uploaded_file.name.endswith('.xlsx'):
                to_download = to_excel_bytes(df)
            else:
                to_download = df.to_csv(index=False).encode('utf-8')

            c = st.download_button(
                label="Download file",
                data=to_download,
                file_name=uploaded_file.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if uploaded_file.name.endswith('.xlsx') else "text/csv"
            )

            if c:
                message_placeholder.success("File downloaded successfully!")
   
            

        



if __name__ == "__main__":
    main() 
