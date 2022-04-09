## pass file name as command line argument

import subprocess
import sys
from datetime import datetime
dt=datetime.now()
file = open(sys.argv[1])
lst=[]
link="/export/home/nz/google-cloud-sdk/bin/" 
prev=''
for line in file:
    if '@' in line:
        l = line.split('@')
        k=line.split('/')
        bkt=k[2]
        # print(l[0])
        ds=l[0]
        cmd1=l[3]
        cmd2=l[4].split('.')
        cmd2=cmd2[0]
        ds=ds.split('/')
        ds=ds[-1]
        print(bkt)
        tablename = l[2]
        db=l[1]
        tablename = tablename.split('.')
            
        cmd = ("""{}bq query --nouse_legacy_sql "insert into thcdnadevdata.staging.ingestion_config_details values 
            ('{}','{}','{}',
            '{}','{}','{}','','{}','staging_daac')" """).format(link,ds,tablename[1],bkt,db,cmd1,cmd2,dt)   
        subprocess.call(cmd,shell=True)
        print(tablename[1]+" Loaded")
file.close()

# insert into  thcdnadevdata.staging.ingestion_config_details values ('dsCCDAACPATSERVIP','DAAC_PATSERVIP_HIST','bkt-thc-dna-dev-daac-001','PBAR','Truncate','Insert',' ipssevcd, ipllchd, ipllcht','2022-04-04','STAGING')