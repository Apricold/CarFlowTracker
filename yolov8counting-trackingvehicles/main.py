
#---------------Importamos librerias-------------------#
import cv2
import pandas as pd
from ultralytics import YOLO
from tracker_2 import Tracker as Tr_2
from tracker_1 import Tracker as Tr_1
import sys
from vidgear.gears import CamGear
import pafy
import random
import time
import os
#-----------------importacion trackeo-----------------#
from supervision.draw.color import ColorPalette
from supervision.geometry.dataclasses import Point
from supervision.video.dataclasses import VideoInfo
from supervision.video.source import get_video_frames_generator
from supervision.video.sink import VideoSink
from supervision.notebook.utils import show_frame_in_notebook
from supervision.tools.detections import Detections, BoxAnnotator
from supervision.tools.line_counter import LineCounter, LineCounterAnnotator
#----------------importacion conexion a drive------------#
import pandas as pd
import datetime
from Drive_conection import Conexion_drive
#----------------------MODELO YOLO-----------------------#


model=YOLO('yolov8n.pt')
CLASS_NAMES_DICT = model.model.names
CLASS_ID = [2, 3, 5, 7]

def trad(w):
    if w ==2:
        return "Carro"
    elif w==3:
        return "Motocicleta"
    elif w==5:
        return "Bus"
    else:
        return "Camion"

dic_cred='credentials_module.json'
ID_DATA_SHEET="1V-CuZNP5FEnE4sA0wh0enxMTZGzmHQDFbYc0UVocZ_A"
#---------------------------LEYENDO EL LIVE STREAM-----------------#
url = "https://www.youtube.com/watch?v=-Xc9TcoZXxs"
video = pafy.new(url)
best = video.getbest(preftype="mp4")
cap=cv2.VideoCapture(best.url)

#---------------------------LEYENDO VIDEO-----------------#
# cap = cv2.VideoCapture("veh2.mp4")


#----------------Carros Subiendo y Carros Bajando-------#
vh_down={}
vh_up={}
#----------------------------------------------Contadores---------------------------#
counter=[]
counter1=[]
columns=["id","Velocidad","Fecha-hora","Tipo de vehiculo","contador_arriba","contador_abajo"]
df=pd.DataFrame(columns=columns)
df=df.astype({'id':'int',"Velocidad":"int"})
df['Fecha-hora'] = pd.to_datetime(df['Fecha-hora'],format="%Y-%m-%d %H:%M:%E*S")

#---------------------------------------------------------------------------------------------#

cy1=240
cy2=288
offset=10
tracker = Tr_2()
colors = [(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)) for j in range(10)]
count=0
detection_threshold = 0.2
while True:    
    ret,frame = cap.read()
    if not ret:
        break
    count += 1
    if count % 3 != 0:
        continue
    frame=cv2.resize(frame,(1020,500))
    results = model(frame,classes=CLASS_ID)
    for result in results:
        detections = []
        for r in result.boxes.data.tolist():
            x1, y1, x2, y2, score, class_id = r
            x1 = int(x1)
            x2 = int(x2)
            y1 = int(y1)
            y2 = int(y2)
            class_id = int(class_id)
            if score > detection_threshold:
                detections.append([x1, y1, x2, y2, score])


#---------------Tracker_2---------------#
        tracker.update(frame, detections)
#--------------------------------------#
        for track in tracker.tracks:
            bbox = track.bbox
            # bbox = track
            x1, y1, x2, y2 = bbox
            cx=int(x1+x2)//2
            cy=int(y1+y2)//2
            track_id = track.track_id
 #-------------------IMPORTAMOS LOS DATOS--------------#
            cv2.putText(frame,f"{str(track_id)}",(int(x1), int(y1)),cv2.FONT_ITALIC,0.8,(0,255,255),2)
            cv2.putText(frame,f"{class_id}",(int(x1), int(y2)),cv2.FONT_ITALIC,0.3,(0,240,0),2)
            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (colors[track_id % len(colors)]), 3)

        ##---------------Abajo--------------------#
            if cy1<(cy+offset) and cy1 > (cy-offset):
                vh_down[track_id]=time.time()
            if track_id in vh_down:
                if cy2<(cy+offset) and cy2 > (cy-offset):
                    elapsed_time=time.time() - vh_down[track_id]
                    if counter.count(track_id)==0:
                        counter.append(track_id)
                        distance = 10 # mpip install supervision==0.1.0ters
                        a_speed_ms = distance / elapsed_time
                        a_speed_kh = a_speed_ms * 3.6
                        current_time = datetime.datetime.now()
                        cv2.circle(frame,(cx,cy),4,(0,0,255),-1)
                        cv2.putText(frame,str(track_id),(int(x1),int(y1)),cv2.FONT_ITALIC,0.6,(255,255,255),1)
                        cv2.putText(frame,str(class_id),(int(x1+10),int(y1+10)),cv2.FONT_ITALIC,0.6,(255,255,255),1)   
                        cv2.putText(frame,str(int(a_speed_kh))+'Km/h',(int(x2),int(y2)),cv2.FONT_ITALIC,0.8,(0,255,0),2)

                        #-------------------IMPORTAMOS LOS DATOS--------------#

                        row={
                            "id":track_id,
                            "Velocidad":int(round(a_speed_kh,2)),
                            "Fecha-hora":current_time,
                            "Tipo de vehiculo":f"{class_id}",
                            "contador_arriba":len(counter),
                            "contador_abajo":len(counter1),
                        }
                        df=df.append(row,ignore_index=True)




                        
        ##------------------Arriba-------------#####     
            if cy2<(cy+offset) and cy2 > (cy-offset):
                vh_up[track_id]=time.time()
            if track_id in vh_up:
                if cy1<(cy+offset) and cy1 > (cy-offset):
                    elapsed1_time=time.time() - vh_up[track_id]
                    if counter1.count(track_id)==0:
                        counter1.append(track_id)      
                        distance1 = 10 # meters
                        a_speed_ms1 = distance1 / elapsed1_time
                        a_speed_kh1 = a_speed_ms1 * 3.6
                        cv2.circle(frame,(cx,cy),4,(0,0,255),-1)
                        cv2.putText(frame,str(track_id),(int(x1),int(y1)),cv2.FONT_ITALIC,0.6,(255,255,255),1)         
                        cv2.putText(frame,str(class_id),(int(x1+10),int(y1+10)),cv2.FONT_ITALIC,0.6,(255,255,255),1)                  
                        cv2.putText(frame,str(int(a_speed_kh1))+'Km/h',(int(x2),int(y2)),cv2.FONT_ITALIC,0.8,(0,255,0),2)
                        
                        
                        #-------------------IMPORTAMOS LOS DATOS--------------#
                        current_time = datetime.datetime.now()
                        row={
                            "id":track_id,
                            "Velocidad":int(round(a_speed_kh1,2)),
                            "Fecha-hora":current_time,
                            "Tipo de vehiculo":class_id,
                            "contador_arriba":len(counter),
                            "contador_abajo":len(counter1),
                        }
                        df=df.append(row,ignore_index=True)

        #-------------Pop--------------------#



            
    cv2.line(frame,(1020,cy1),(0,cy1),(255,255,255),1)
    cv2.putText(frame,('L1'),(297,cy1),cv2.FONT_ITALIC,0.8,(0,255,0),2)
    cv2.line(frame,(1020,cy2),(0,cy2),(255,255,255),1)
    cv2.putText(frame,('L2'),(192,cy2),cv2.FONT_ITALIC,0.8,(0,255,0),2)  
    cv2.putText(frame,('DOWN -> ')+str(len(counter)),(60,90),cv2.FONT_ITALIC,0.8,(0,255,0),2)
    cv2.putText(frame,('UP -> ')+str(len(counter1)),(60,130),cv2.FONT_ITALIC,0.8,(0,255,0),2)
    cv2.imshow("RGB", frame)


    if cv2.waitKey(10) & 0xFF == ord("q"):
        break
#-----------------------Convertimos la nueva base de datos a csv-----------------------------#
PATH="D:\Electronica-UNAL\\2023-2\Analitica de datos\\Trabajo Final\\yolov8counting-trackingvehicles\\Df_local\\df_lqocal.csv"
if os.path.exists(PATH):
    df_local=pd.read_csv(PATH)
    mx_num_up=df_local["contador_arriba"].max()
    mx_num_down=df_local["contador_abajo"].max()
    df["contador_arriba"]=df["contador_arriba"]+mx_num_up
    df["contador_abajo"]=df["contador_abajo"]+mx_num_down
    df=df_local.append(df, ignore_index=True)
    df['Velocidad'] = pd.to_numeric(df['Velocidad'])
    df['Fecha-hora'] = pd.to_datetime(df['Fecha-hora'])
    print(df)
    df.to_csv(PATH, index=False)
else:
    print(df)
    df.to_csv(PATH,index=False)
# #-------------Conexion con drive del csv --------------#
cd=Conexion_drive()
drive=cd.entrar(dic_cred)
# cd.crear_db(PATH,"Prueba",drive)
cd.update_file_content(drive,ID_DATA_SHEET,PATH)
cap.release()
cv2.destroyAllWindows()
print(df)