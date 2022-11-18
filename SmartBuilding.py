
import glob
import sched
import time
import csv
import os
import pandas as pd
import pymysql
import dotenv
import json

config = dotenv.dotenv_values(".env")

s=sched.scheduler(time.time,time.sleep)

#-----------------------------------------------------DATABASE------------------------------------------------------------------------------




conn=None
cur=None

sql=""
conn=pymysql.connect(host=config['hosturl'],user='root',password=config['password'],db='smartbuilding',charset='utf8')
cur=conn.cursor()


def database():
    sql = "CREATE TABLE IF NOT EXISTS userTable (id char(4), userName char(10), email char(15), birthYear int)"



    cur.execute(sql)	# 커서로 sql문 실행 

    conn.commit()	# 저장



    conn.close()	# 종료



#---------------------------------------------------------------open csv 파일 데이터 가져오기--------------------------------------------------------------------



# get all files inside a specific folder
dir_path = '/Users/user/Desktop/project/SmartBuildingDB/RoomList/*'
recentfolders=[]
csvfiles=["co2.csv","humidity.csv","light.csv","pir.csv","temperature.csv"]


def job():
    folders = glob.glob(dir_path)
    for folder in folders:
        if folder not in recentfolders:
            print("삽입",folder)
            recentfolders.append(folder)
            csvfiles=[file for file in os.listdir(folder) if file.endswith('csv')]
            df= pd.DataFrame()
            table=[]            
            data=[]
            for i in csvfiles:
                data.append(pd.read_csv(folder+"/"+i))
                # print(data,"end")
                table.append(i.replace(".csv",""))
                
    for recentfolder in recentfolders:

        if recentfolder not in folders:
            print("삭제",recentfolder)
            recentfolders.remove(recentfolder)
        
    print("workingg...")




#------------------------------------------------------------------scheduler--------------------------------------------------------------------------    
    s.enter(5,1,job)



s.enter(5,1,job,())
s.run()


