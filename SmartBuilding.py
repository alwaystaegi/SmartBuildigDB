
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
config = dotenv.dotenv_values(".env")

s=sched.scheduler(time.time,time.sleep)

#-----------------------------------------------------DATABASE------------------------------------------------------------------------------




conn=None
cur=None

sql=""
conn=pymysql.connect(host=config['hosturl'],user='root',password=config['password'],db='smartbuilding',charset='utf8')
cur=conn.cursor()




class Data:
    name=str
    co2=None
    humidity=None
    light=None
    pir=None
    temperature=None

#---------------------------------------------------------------open csv 파일 데이터 가져오기--------------------------------------------------------------------


# get all files inside a specific folder
dir_path = '/Users/user/Desktop/project/SmartBuildingDB/RoomList/*'
recentfolders=[]
roomlist=[]

#todo 클래스로 데이터 시도해보기
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
            df= pd.DataFrame()
            table=[]            
            co2=None
            humidity=None
            light=None
            pir=None
            temperature=None
            roomname=folder.replace("/Users/user/Desktop/project/SmartBuildingDB/RoomList\\","")
            roomlist.append(roomname)
            name=roomname
            for i in csvfiles:
                temp=pd.read_csv(folder+"/"+i,header=None)
            
                if i=="co2.csv":
                    co2=temp[1].values
                elif i=="humidity.csv":    
                    humidity=temp[1].values
                elif "light.csv"==i:
                    light=temp[1].values
                elif "pir.csv"==i:
                    pir=temp[1].values
                else :
                    temperature=temp[1].values        
                
                table.append(i.replace(".csv",""))
            roomdatas.append([name,co2,humidity,light,pir,temperature])
    j=0
    for roomdata in roomdatas:
        
        print(roomdata[0])
        j=j+1
        if(countnow%2==0):
            sql='INSERT INTO roomdata (Room,time,co2,humidity,light,pir,temperature) values ("{}","{}","{}","{}","{}","{}","{}")'.format(roomdata[0],datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),roomdata[1][countnow],roomdata[2][countnow],roomdata[3][countnow],roomdata[4][int(countnow/2)],roomdata[5][countnow])
        else:
            sql='INSERT INTO roomdata (Room,time,co2,humidity,light,temperature) values ("{}","{}","{}","{}","{}","{}")'.format(roomdata[0],datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),roomdata[1][countnow],roomdata[2][countnow],roomdata[3][countnow],roomdata[5][countnow])
        print(sql)
        cur.execute(sql)	# 커서로 sql문 실행 

                
        # conn.close()	# 종료
        # print(roomdata.name[countnow]+'","'+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'","'+str(roomdata.co2[countnow])+str(roomdata.humidity[countnow])+str(roomdata.light[countnow])+str(roomdata.temperature[countnow]))  
    conn.commit()	# 저장
    countnow=countnow+1;    
        # print(table)
        # print(data)    
        

    for recentfolder in recentfolders:

        if recentfolder not in folders:
            print("삭제",recentfolder)
            recentfolders.remove(recentfolder)
        
    print("workingg...")
    s.enter(1,1,job)




#------------------------------------------------------------------scheduler--------------------------------------------------------------------------    



s.enter(1,1,job,())
s.run()


