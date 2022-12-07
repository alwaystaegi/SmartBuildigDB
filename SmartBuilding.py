
import glob
import sched
from datetime import datetime
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
#mysql 연결용 변수
conn=pymysql.connect(host=config['hosturl'],user=config['userID'],password=config['password'],db='smartbuilding',charset='utf8')
cur=conn.cursor()



#---------------------------------------------------------------open csv 파일 데이터 가져오기--------------------------------------------------------------------


# RoomList에 smartbuildingsystem 데이터를 넣을 예정 . room 파일을 넣으면 그 파일을 읽음.
dir_path = '/Users/user/Desktop/project/SmartBuildingDB/RoomList/*'
recentfolders=[]
roomlist=[]

roomdatas=[]



#-------------------------------------------------------------------상태 인식------------------------------------------------------------------------------------
def checkstate(data):
    problem=""
    
    #co2 상태 체크하기
    if int(data['co2'])>=1000 and int(data['co2'])<1500:
        problem=problem+" co2Warning"
    elif int(data['co2'])>=1500:
        problem=problem+" co2Danger"

    
    #온도 체크하기

    
    #습도 체크하기







countnow=0
#scheduler에 사용할 함수. 매 1초마다 작동(이미 정해져있는 데이터이고,시간값도 파일에 있는 데이터를 가져오기 때문에 시간을 짧게 함, 만약 실제 센서를 이용한다면 5초~10초 사용하기)
def job():
    #smartbuilding데이터중 pir센서만 10초마다 나머지는 5초마다 작동해서 우선 임시로 2번중 1번만 pir센서를 읽기 위해서 count변수를 사용함
    global countnow
    folders = glob.glob(dir_path)
    for folder in folders:
        # 새로운 폴더인지 아닌지 확인하여 새로운 폴더일 경우 안에 있는 파일을 읽어옴,안에 있는 새로운 csv 파일들까지 다 확인하기에는 조금 어려울것같다.
        if folder not in recentfolders:
            
            recentfolders.append(folder)
            csvfiles=[file for file in os.listdir(folder) if file.endswith('csv')]
            df= None
            roomname=folder.replace("/Users/user/Desktop/project/SmartBuildingDB/RoomList\\","")
            print("새로운 방 : ",roomname)
            roomlist.append(roomname)

            for i in csvfiles:

                if i=="co2.csv":
                    #처음엔 dataframe타입이 아니라 None타입으로 들어있어 두 데이터프레임을 합치는 것이 아닌 대입 연산자를 적용 
                    if df is None:
                        df=pd.read_csv(folder+"/"+i,header=None,names=["ctime","co2"])
                        continue
                    df=pd.concat([df,pd.read_csv(folder+"/"+i,header=None,names=["ctime","co2"])],axis=1)

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
            df = df.astype(object).where(pd.notnull(df), None)
            #방 이름을 데이터프레임 인덱스로 지정
            df.index=[roomname for i in range(len(df.index))]
            print(df)
            roomdatas.append(df)


    #roomdata에 있는 값들을 tuple형식으로 바꿔서 반환시키기.  
    def loaddata(data):
        #dataframe.iloc[int] int번째 행을 선택 
        value=data.iloc[countnow]

        if countnow%2==0:
            return (str(value.name),(datetime.fromtimestamp(int(value['ctime']))).strftime('%Y-%m-%d %H:%M:%S'),int(value['co2']),(datetime.fromtimestamp(int(value['htime']))).strftime('%Y-%m-%d %H:%M:%S'),float(value['humidity']),(datetime.fromtimestamp(int(value['ltime']))).strftime('%Y-%m-%d %H:%M:%S'),int(value['light']),(datetime.fromtimestamp(int(data.iloc[int(countnow/2)]['ptime']))).strftime('%Y-%m-%d %H:%M:%S'),int(data.iloc[int(countnow/2)]['pir']),(datetime.fromtimestamp(int(value['ttime']))).strftime('%Y-%m-%d %H:%M:%S'),float(value['temperature']))
        else:
           return (str(value.name),(datetime.fromtimestamp(int(value['ctime']))).strftime('%Y-%m-%d %H:%M:%S'),int(value['co2']),(datetime.fromtimestamp(int(value['htime']))).strftime('%Y-%m-%d %H:%M:%S'),float(value['humidity']),(datetime.fromtimestamp(int(value['ltime']))).strftime('%Y-%m-%d %H:%M:%S'),int(value['light']),(datetime.fromtimestamp(int(value['ttime']))).strftime('%Y-%m-%d %H:%M:%S'),float(value['temperature']))

    #우선은 pir 값이 다른 센서에 비해 2배 적기 때문에 현재 count값에 따라 sql을 달리함
    if countnow%2==0:
        sql='INSERT INTO roomdata (Room,ctime,co2,htime,humidity,ltime,light,ptime,pir,ttime,temperature) values (%s,%s,"%s",%s,"%s",%s,"%s",%s,"%s",%s,"%s")'
    else:
        sql='INSERT INTO roomdata (Room,ctime,co2,htime,humidity,ltime,light,ttime,temperature) values (%s,%s,"%s",%s,"%s",%s,"%s",%s,"%s")'

    # DB에 sql문을 내보내는 함수, excutemany로 하여 모든 방의 countnow번째 데이터를 INSERT한다.

    for data in roomdatas:
        checkstate(data.iloc[countnow])
    cur.executemany(sql,[loaddata(data) for data in roomdatas])
    print(cur.rowcount,"만큼 입력됨")
    conn.commit()	# 저장
    countnow=countnow+1;    



    s.enter(1,1,job)




#------------------------------------------------------------------scheduler--------------------------------------------------------------------------    



s.enter(1,1,job,())
s.run()


