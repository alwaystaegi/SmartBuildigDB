
import glob
import sched
import datetime
import csv
import os
import pandas as pd
import pymysql
import dotenv
import json
import time
import numpy as np
config = dotenv.dotenv_values(".env")

s=sched.scheduler(time.time,time.sleep)

#-----------------------------------------------------DATABASE------------------------------------------------------------------------------




conn=None
cur=None

sql=""

conn=pymysql.connect(host=config['hosturl'],user=config['userID'],password=config['password'],db='smartbuilding',charset='utf8')
cur=conn.cursor()



#---------------------------------------------------------------open csv 파일 데이터 가져오기--------------------------------------------------------------------


# get all files inside a specific folder
dir_path = '/Users/user/Desktop/project/SmartBuildingDB/RoomList/*'
recentfolders=[]
roomlist=[]

roomdatas=[]


countnow=0
def job():
    global countnow
    folders = glob.glob(dir_path)
    for folder in folders:
        if folder not in recentfolders:
            print("새로운 방 : ",folder)
            recentfolders.append(folder)
            csvfiles=[file for file in os.listdir(folder) if file.endswith('csv')]
            df= None
            table=[] 
            timestamp=None           
            co2=None
            humidity=None
            light=None
            pir=None
            temperature=None
            roomname=folder.replace("/Users/user/Desktop/project/SmartBuildingDB/RoomList\\","")
            roomlist.append(roomname)
            name=roomname
            def setname(i):
                return roomname
            def getdf(item):
                if item=="co2.csv":
                    return (pd.read_csv(folder+"/"+item,header=None,names=["ctime","co2"]))
                    # timestamp=temp[0].values
                    # co2=temp[1].values
                elif item=="humidity.csv":    
                    return pd.read_csv(folder+"/"+item,header=None,names=["htime","humidity"])
                    # humidity=temp[1].values
                elif "light.csv"==item:
                    return pd.read_csv(folder+"/"+item,header=None,names=["ltime","light"])
                    # light=temp[1].values
                elif "pir.csv"==item:
                    # pir=temp[1].values
                    return pd.read_csv(folder+"/"+item,header=None,names=["ptime","pir"])    
                else :
                    # temperature=temp[1].values     
                    return pd.read_csv(folder+"/"+item,header=None,names=["ttime","temperature"])

            # ss=pd.concat(getdf(item) for item in csvfiles)
            # ss.index=[roomname for i in range(len(ss.index))]

            for i in csvfiles:
                # temp=pd.read_csv(folder+"/"+i,header=None)

                if i=="co2.csv":
                    if df is None:
                        df=pd.read_csv(folder+"/"+i,header=None,names=["ctime","co2"])
                        continue
                    df=pd.concat([df,pd.read_csv(folder+"/"+i,header=None,names=["ctime","co2"])],axis=1)
                    # timestamp=temp[0].values
                    # co2=temp[1].values
                elif i=="humidity.csv":
                    if df is None:
                        df=pd.read_csv(folder+"/"+i,header=None,names=["htime","humidity"])
                        continue       
                    df=pd.concat([df,pd.read_csv(folder+"/"+i,header=None,names=["htime","humidity"])],axis=1)
                elif "light.csv"==i:
                    if df is None:
                        pdf=pd.read_csv(folder+"/"+i,header=None,names=["ltime","light"])
                        continue
                    df=pd.concat([df,pd.read_csv(folder+"/"+i,header=None,names=["ltime","light"])],axis=1)


                elif "pir.csv"==i:
                    if df is None:
                        df=pd.read_csv(folder+"/"+i,header=None,names=["ptime","pir"])
                        continue
                    df=pd.concat([df,pd.read_csv(folder+"/"+i,header=None,names=["ptime","pir"])],axis=1)
                else :
                    if df is None:
                        df=pd.read_csv(folder+"/"+i,header=None,names=["ttime","temperature"])
                        continue
                    df=pd.concat([df,pd.read_csv(folder+"/"+i,header=None,names=["ttime","temperature"])],axis=1)
                table.append(i.replace(".csv",""))
            df = df.astype(object).where(pd.notnull(df), None)
            df.index=[roomname for i in range(len(df.index))]

            roomdatas.append(df)

        #roomdata에 있는 값들을 tuple값으로 바꿔서 반환시키기.  
    def loaddata(data):
        value=data.iloc[countnow]
        if countnow%2==0:
            return (str(value.name),int(value['ctime']),int(value['co2']),int(value['htime']),float(value['humidity']),int(value['ltime']),int(value['light']),int(data.iloc[int(countnow/2)]['ptime']),int(data.iloc[int(countnow/2)]['pir']),int(value['ttime']),float(value['temperature']))
        else:
            return (str(value.name),int(value['ctime']),int(value['co2']),int(value['htime']),float(value['humidity']),int(value['ltime']),int(value['light']),int(value['ttime']),float(value['temperature']))

    #우선은 pir 값이 다른 센서에 비해 2배 적기 때문에 
    if countnow%2==0:
        sql='INSERT INTO roomdata (Room,ctime,co2,htime,humidity,ltime,light,ptime,pir,ttime,temperature) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'
    else:
        sql='INSERT INTO roomdata (Room,ctime,co2,htime,humidity,ltime,light,ttime,temperature) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s")'
    #
    cur.executemany(sql,[loaddata(t) for t in roomdatas])
    print(cur.rowcount,"만큼 입력됨")
    conn.commit()	# 저장
    countnow=countnow+1;    



    s.enter(1,1,job)




#------------------------------------------------------------------scheduler--------------------------------------------------------------------------    



s.enter(1,1,job,())
s.run()


