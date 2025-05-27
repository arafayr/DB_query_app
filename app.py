import streamlit as st
import pandas as pd
from io import BytesIO
import pyodbc
import time

def to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def query_database(data_list, _message_placeholder):
    with st.spinner("Querying database..."):
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
                "TrustServerCertificate=yes;",
                timeout=5
            )
        except pyodbc.Error as e:
            _message_placeholder.error(f"Database connection error: {e}")
            time.sleep(2)
            _message_placeholder.empty()
            st.stop()
            return None

    _message_placeholder.success("Connected to the database successfully!")
    time.sleep(1.5)
    _message_placeholder.empty()

    cursor = conn.cursor()
    placeholders = ','.join(f"'{id}'" for id in data_list)

    query = f'''
    select site_code,site_name,identifiers_opensrp_id,firstname ,
    CASE WHEN site_code in ('AG','BH','AE','IE','QB','KMG','Saindad Goth','Yusuf Saab Goth','JG','SG') then 'IRP'
    WHEN site_code in ('RG','IH') then 'Prisma'
    else 'client'
    end as project
    from vital_target.vr.client c where client_type_target='MOTHER' AND c.identifiers_opensrp_id IN ({placeholders});
    '''

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columns)

    cursor.close()
    conn.close()

    return df

def main():
    st.title("Finance Data")

    uploaded_file = st.file_uploader("Upload a file", type=["xlsx"])

    

    if uploaded_file:
        file_changed = (
            ("uploaded_file_name" not in st.session_state or # for the first upload
            st.session_state.uploaded_file_name != uploaded_file.name) or
            ("uploaded_data" not in st.session_state or # for the first upload
            not st.session_state.uploaded_data.equals(pd.read_excel(uploaded_file)))
             # if the file name has changed
        )

        if file_changed:
            message_placeholder = st.empty()
            st.session_state.uploaded_file_name = uploaded_file.name
             
            message_placeholder.success(f"Uploaded file: {uploaded_file.name}")
            time.sleep(1.5)
            message_placeholder.empty()

            try:
                df_input = pd.read_excel(uploaded_file)
                st.session_state.uploaded_data = df_input
            except Exception as e:
                message_placeholder.error(f"Failed to read file: {e}")
                return

            if df_input.empty:
                message_placeholder.error("The uploaded file is empty. Please upload a valid file.")
                return

            col = df_input.columns[0]
            if col.lower().startswith("unnamed") or col == "":
                message_placeholder.error("The ID column should start from the first column with a valid column name.")
                return
            elif df_input[col].isnull().all():
                message_placeholder.error("The ID column is empty. Please upload a file with valid IDs.")
                return

            data = df_input[col].dropna().apply(lambda x: x.replace("'", "").strip()).unique().tolist()

            df_result = query_database(data, message_placeholder)
            if df_result is None or df_result.empty:
                message_placeholder.error("No data found for the provided IDs.")
                return

            st.session_state.df_result = df_result
            st.session_state.download_bytes = to_excel_bytes(df_result)

     

        if "uploaded_data" in st.session_state:
            if "df_result" in st.session_state:
                st.write("File content preview")
                st.dataframe(st.session_state.df_result.head())
    
                c = st.download_button(
                    label="Download file",
                    data=st.session_state.download_bytes,
                    file_name=st.session_state.uploaded_file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
                if c:
                    message_placeholder.success("File downloaded successfully!")
                    file_changed = None

if __name__ == "__main__":
    main()
