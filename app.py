import streamlit as st
import pandas as pd
from io import BytesIO
import time
from sqlalchemy import create_engine

def to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    st.session_state.download_clicked = True
    return output.getvalue()

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def query_database(data_list, _message_placeholder):
    with st.spinner("Connecting to the database...") :
        DB_SERVER = st.secrets["db_server"]
        DB_NAME = st.secrets["db_name"]
        DB_USER = st.secrets["db_user"]
        DB_PASSWORD = st.secrets["db_password"]

        try:
            connection_string = (
                f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
            )
            engine = create_engine(connection_string, connect_args={"timeout": 1})
            pd.read_sql("SELECT 1", engine)  # Test connection
            
        except:
            _message_placeholder.error(f"Database connection error check credentials")
            st.stop()
            return 
            

    _message_placeholder.success("Connected to the database successfully!")
    time.sleep(1.5)
    _message_placeholder.empty()

    chunk_size = 10000  
    result_dfs = []

    with st.spinner("Querying database..."):
        for chunk in chunk_list(data_list, chunk_size):
            placeholders = ','.join(f"'{id}'" for id in chunk)

            query = f'''
            WITH Filtered_data AS (
            SELECT
                site_code,
                site_name,
                identifiers_opensrp_id,
                firstname,
                CASE
                    WHEN site_code IN ('AG', 'BH', 'AE', 'IE', 'QB', 'KMG', 'Saindad Goth', 'Yusuf Saab Goth', 'JG', 'SG') THEN 'IRP'
                    WHEN site_code IN ('RG', 'IH') THEN 'Prisma'
                    ELSE 'client'
                END AS project
            FROM
                vital_target.vr.client c
            WHERE
                client_type_target = 'MOTHER'
                AND c.identifiers_opensrp_id IN ({placeholders})
            ) 
                SELECT
                *,
                CASE
                    WHEN Filtered_data.project = 'IRP' THEN 'Yes'
                    ELSE 'No'
                END AS IRP,
                CASE
                    WHEN Filtered_data.project = 'Prisma' THEN 'Yes'
                    ELSE 'No'
                END AS Prisma,
                CASE
                    WHEN Filtered_data.project = 'client' THEN 'Yes'
                    ELSE 'No'
                END AS Client
            FROM
                Filtered_data;
            '''

            try:
                chunk_df = pd.read_sql(query, engine)
                result_dfs.append(chunk_df)
            except Exception as e:
                _message_placeholder.error(f"Query failed on chunk: {e}")
                

    if not result_dfs:
        return None

    df = pd.concat(result_dfs, ignore_index=True)
    return df

def process_file(uploaded_file):
    global message_placeholder

    st.session_state.uploaded_file_name = uploaded_file.name
    message_placeholder.success(f"Uploaded file: {uploaded_file.name}")
    time.sleep(1.5)
    message_placeholder.empty()

    try:
        df_input = pd.read_excel(uploaded_file)
        st.session_state.uploaded_data = df_input
    except Exception as e:
        message_placeholder.error(f"Failed to read file: {e}")
        return None

    if df_input.empty:
        message_placeholder.error("The uploaded file is empty. Please upload a valid file.")
        st.session_state.df_result = None
        st.stop()
        return None

    col = df_input.columns[0]
    if col.lower().startswith("unnamed") or col == "":
        message_placeholder.error("The ID column should start from the first column with a valid column name.")
        st.session_state.df_result = None
        st.stop()
        return None
    elif df_input[col].isnull().all():
        message_placeholder.error("The ID column is empty. Please upload a file with valid IDs.")
        st.session_state.df_result = None
        st.stop()
        return None

    data = df_input[col].dropna().apply(lambda x: str(x).replace("'", "").strip()).unique().tolist()
    df_result = query_database(data, message_placeholder)

    if df_result is None or df_result.empty:
        message_placeholder.error("No Data Found against provided ids.")
        st.stop()
        return None

    st.session_state.df_result = df_result
    return True

def main():
    st.title("Finance Bill Reconciliation")
    global message_placeholder

    def flag():
        download_clicked = st.session_state.get("download_clicked", False)
        if download_clicked:
            st.session_state.download_clicked = False

    uploaded_file = st.file_uploader("Upload a file", type=["xlsx"], on_change=flag)
    message_placeholder = st.empty()

    if uploaded_file:
        if not st.session_state.get("download_clicked", False):
            process_file(uploaded_file)

        if "uploaded_data" in st.session_state and st.session_state.df_result is not None:
            st.write("File content preview")
            st.dataframe(st.session_state.df_result.head())

            download_clicked = st.download_button(
                label="Download file",
                data=to_excel_bytes(st.session_state.df_result),
                file_name=st.session_state.uploaded_file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            if download_clicked:
                message_placeholder.success("File downloaded successfully!")

if __name__ == "__main__":
    main()
