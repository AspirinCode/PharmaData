import os
import pandas as pd
import psycopg2

#https://aact.ctti-clinicaltrials.org/psql
ct_conn_string = "host='aact-db.ctti-clinicaltrials.org' port = 5432 dbname='aact' user='aact' password='aact'"

# Need to use psycopg2 as AACT data is hosted on postgres
ct_conn = psycopg2.connect(ct_conn_string)
ct_curs = ct_conn.cursor()

public_tables = "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = 'public'"
df_public_tables = pd.read_sql(public_tables, ct_conn)
public_table_lst = df_public_tables["table_name"].tolist()

def save_table_structure():
    for tbl in public_table_lst:
        #this works too but less info
        public_table_struct = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS where table_name = '{TableName}';".format(TableName = tbl)
        df_struct = pd.read_sql(public_table_struct, ct_conn)
        parent_path = "C:\\Users\\bretg\\Documents\\Programming\\PharmaData\\ClinicalTrials\\CSVData\\Structure"
        local_path = "\\" + tbl + '_structure.csv'
        f_path = os.path.join("{ParentPath}{LocalPath}".format(ParentPath = parent_path, LocalPath = local_path))
        df_struct.to_csv(path_or_buf = f_path, sep = ',')

# Saves all data as a CSV in specified folder
def ct_data_csv_all():
    for tbl in public_table_lst:
        get_all_ct_data = "SELECT * FROM {TableName}".format(TableName = tbl)
        ct_data_file = os.path.join("C:\\Users\\bretg\\Documents\\Programming\\PharmaData\\ClinicalTrials\\CSVData\\{TableFile}".format(TableFile = tbl + '.csv'))
        df_ct = pd.read_sql(get_all_ct_data, ct_conn)
        df_ct.to_csv(path_or_buf = ct_data_file)
        print("{TableName}...done".format(TableName = tbl))