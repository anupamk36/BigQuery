import subprocess
import os
from datetime import datetime
# from google.cloud import bigquery

# from tkinter.tix import COLUMN
# import pandas as pd
dt = datetime.now()
link = "/export/home/nz/google-cloud-sdk/bin/"
login_prod = (
    """{}gcloud config set project thc-dna-prod-gcs-project""").format(link)
subprocess.call(login_prod, shell=True)
bucket_list = ("""{}gsutil ls > bucket_list.txt""").format(link)
subprocess.call(bucket_list, shell=True)
file = open('bucket_list.txt')
# table = open('all_table_list.txt','w')
# load_table = open('load_table_list.txt')
for line in file:
    line = line.rstrip()
    path = line+'legacy/'
    load_list = ("""{}gsutil ls {} > load_table_list.txt""").format(link, path)
    subprocess.call(load_list, shell=True)
    tb_list = open('all_table_list.txt', 'r')
    tb_list = tb_list.read()
    tb_list = tb_list.split('\n')
    load_tb_list = open('load_table_list.txt', 'r')
    load_tb_list = load_tb_list.read()
    load_tb_list = load_tb_list.split('\n')
    non_duplicate_tb = list(set(load_tb_list)-set(tb_list))
    print(non_duplicate_tb)
    login_dev = ("""{}gcloud config set project thcdnadevdata""").format(link)
    subprocess.call(login_dev, shell=True)
    for line in non_duplicate_tb:
        line = line.strip()
        if '@' in line:
            try:
                l = line.split('@')
                k = line.split('/')
                bkt = k[2]
                ds = l[0]
                cmd1 = l[3]
                cmd2 = l[4].split('.')
                cmd2 = cmd2[0]
                ds = ds.split('/')
                ds = ds[-1]
                tablename = l[2]
                db = l[1]
                tablename = tablename.split('.')
            except IndexError:
                print("index error at: ")
        cmd = ("""{}bq query --nouse_legacy_sql "insert into thcdnadevdata.staging.ingestion_config_details values
            ('{}','{}','{}','{}','{}','{}','','{}','staging_daac')" """).format(link, ds, tablename[1], bkt, db, cmd1, cmd2, dt)
        subprocess.call(cmd, shell=True)
        print(tablename[1]+" Loaded")
    all_list = ("""{}gsutil ls {} >> all_table_list.txt""").format(link, path)
    subprocess.call(all_list, shell=True)
file.close()


"""
insert into table values (ds, tablename[1], bkt, db, cmd1, cmd2, dt)
"""
