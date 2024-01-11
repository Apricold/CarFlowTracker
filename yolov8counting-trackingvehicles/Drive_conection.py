import pickle
import os
import os.path
import numpy as np
import pandas as pd
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import sys
import requests
import csv
import openpyxl
import unidecode
#--------------CREDENCIALES PARA INICIAR DRIVE  ----------#
dic_cred='credentials_module.json'
gauth=GoogleAuth()
gauth.LocalWebserverAuth()
class Conexion_drive():
    ### LOGIN CUENTA DE GOOGLE ###
    def entrar(self,x):
        '''
        Funcion que recibe las credenciales para hacer la conexion 
        con google drive 
        Se necesita tener el archivo json con el client id en la misma carpeta
        para que pydrive pueda acceder
        devuelve el objeto drive 
        '''
        GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = x
        gauth=GoogleAuth()
        gauth.LoadCredentialsFile(x)
        if gauth.access_token_expired:
            gauth.Refresh()
            gauth.SaveCredentialsFile(x)
        else:
            gauth.Authorize()
        return GoogleDrive(gauth)
    def crear_db(self,db,DB_NAME,drive):
        import os
        print(os.path.exists(f"{DB_NAME}.csv"))
        if os.path.exists(f"{DB_NAME}.csv")== False:
            df=drive.CreateFile({"title":f"{DB_NAME}.csv"})
        df.SetContentFile(db)
        df.Upload({'convert': True})
        print("Archivo  Creado en drive")
    def update_file_content(self,drive, file_id,path):
        """
        Update a file content of an exiting file on Google Drive.
        Parameters
        ----------
        drive: GoogleDrive object
        file_id : str
            exiting file File ID
        Returns
        -------
        str
            updated file's File ID
        """
        old_file = drive.CreateFile(
            {'id': f"{file_id}", 'mimeType': 'application/csv'})
        old_file.SetContentFile(path)
        old_file.Upload()
        old_file_id = old_file['id']
        return old_file_id


    ### PREPROCESAMIENTO DE LOS DATOS ACENTOS/COLUMNAS/ESPACIOS BLANCOS###
    def prepro_da(self,list_df):
        '''
        Funcion que recibe una lista de df y preprocesa cada una de ellas para rellenar valores nan y acomodar tipo de datos 
        devuelve una lista de dataframes
        
        '''
        for df in list_df:
            df['NOMBRE']=df['NOMBRE'].str.capitalize()
            #---------------Rellenamos las columnas de municipio/departamento/colegio/fc inicio test si es nan------------#
            df['MUNICIPIO'] = df['MUNICIPIO'].fillna(df['MUNICIPIO'].value_counts().index[0])
            df['DEPARTAMENTO'] = df['DEPARTAMENTO'].fillna(df['DEPARTAMENTO'].value_counts().index[0])
            df['COLEGIO'] = df['COLEGIO'].fillna(df['COLEGIO'].value_counts().index[0])
            df['FECHA INICIO TEST'] = df['FECHA INICIO TEST'].fillna(df['FECHA INICIO TEST'].value_counts().index[0])
            df['FECHA INICIO TEST'] = pd.to_datetime(df['FECHA INICIO TEST'], format='%Y-%m-%d')
            df['DOCUMENTO'] =df['DOCUMENTO'].astype('Int64')
        print('Preprocesamiento de los archivos completado.')

        return list_df

    ###  CONCATENAR LA LISTA DE PANDAS DF EN UN DF
    def pd_con(self,df,exportar=False):
        '''
        Funcion que recibe una lista de df y las une en cascada para luego exportar 
        en archivo xlsx
        '''
        df=pd.concat(df,axis=0,ignore_index=True)
        df['NOMBRE'].replace('',np.nan,inplace=True)
        df=df.dropna(subset=['NOMBRE'])
        if exportar:
            df.to_excel('Base_datos_conct.xlsx',index=False)
            print("Base de datos concatenada Subida")
        else:
            print("Base de datos concatenada Lista")
        return df


    ### ENCONTRAR ARCHIVOS CSV TERMINADOS EN ANALISIS####
    def enc_co(self,KEY_WORD,drive):
        '''
        Funcion que recibe una palabra clave y el objeto drive 
        
        devuelve una lista de pandas dataframes con los archivos excel encontrados en el drive  
        '''
        df=[]
        ### LISTA DE ARCHIVOS name contains '(Con análisis)'###
        file_list = drive.ListFile({"q":f"title contains {KEY_WORD}",}).GetList()
        n_files=len(file_list)
        print(f'Se encontraron {n_files} Bases de datos del drive con la palabra clave {KEY_WORD}')
        for i,file1 in enumerate(file_list):
            #FILTRAMOS LAS BASES DE DATOS --> COPIA Y REGISTROS
            nom_tile=file1['title'].split()
            print(f'Descargando archivo {i} de {n_files}\ntitle: %s, id: %s' % (file1['title'], file1['id']))
            if 'Copia' in nom_tile or 'Registro'in nom_tile :
                continue 
            #OBTENEMOS EL CONTENIDO EXCEL DE CADA ARCHIVO EN LA LISTA
            file1.GetContentFile(file1['title'],mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            df1=pd.read_excel(file1['title'],sheet_name=0)
            columns=df1.columns
            #REEMPLAZAMOS COLUMNA ESTILO COGNITIVO POR PUNTAJE EFT
            if 'PD - Estilo cognitivo' in columns:
                df1=df1.rename(columns={'PD - Estilo cognitivo':'Puntaje EFT','Estilo cognitivo':'Calificación EG'})
            df.append(df1)
            print(f'Descargado archivo {i} de {n_files}\ntitle: %s, id: %s' % (file1['title'], file1['id']))
        print('Archivos descargados.')
        return df

    ##### UNION DE PUNTAJES EFT CON LA BASE DE DATOS ORIGINAL #####
    def union(self,df_original,df_puntajes,exportar=False):
        '''
        Funcion que recibe la base de datos original y la base de datos con los puntajes eft 
        y procede a crear una nueva columna en la base de datos original con los puntajes 
        devuelve la base original con la nueva columna  '''

        df_puntajes['DOCUMENTO'] =df_puntajes['DOCUMENTO'].astype('Int64')
        #Localizamos todos los valores nan de la columna documento 
        df_2=df_puntajes.loc[df_puntajes['DOCUMENTO'].isna().values==True]
        
        print(f'Existen {df_2.shape[0]} Registros los cuales no tiene documento')
        #agregamos todos los puntajes eft a los registros los cuales tengan documento
        df_puntajes=df.dropna(subset=['DOCUMENTO'])
        df_original['PUNTAJE_EFT']=df_original['DOCUMENTO'].map(df_puntajes.set_index('DOCUMENTO')['Puntaje EFT'])
        df_original['Calificacion_EG']=df_original['DOCUMENTO'].map(df_puntajes.set_index('DOCUMENTO')['Calificación EG'])
        #agregamos los nuevos registros SIN documento 
        
        for i in df_2.columns:
            #Cambiamos el nombre de las columnas para que concuerden con la df_original
            if i =='Puntaje EFT':
                b=i.upper()
                b=b.replace(' ', '_').replace('.', '_').replace('/', '_')
                df_2.rename(columns={i:b},inplace=True)
                continue
            b=unidecode.unidecode(i)
            b=b.replace(' ', '_').replace('.', '_').replace('/', '_')
            df_2.rename(columns={i:b},inplace=True)
        #Juntamos las 2 df en una lista y concatenamos
        list_df=[df_original,df_2]
        df_original=pd.concat(list_df,axis=0,ignore_index=True)
        #para no obtener columnas de df_2 repitentes limitamos las columnas hasta calificacion
        df_original = df_original.loc[:, :'Calificacion_EG']
        print(df_original.info())
        if exportar:
            df_original.to_excel('Base_de_datos_Unificada.xlsx')
            print("Base de datos Unificada Subida")
        else:
            print("Base de datos Unificada Lista")




# cd=Conexion_drive()
#  #Entramos a drive utilizando el api de pydrive con el archivo client_screts.json en la misma carpeta
# drive=cd.entrar(dic_cred)
# path="D:\Electronica-UNAL\\2023-2\Sistemas Inteligentes Computacionales\Aprendizaje y Redes\\banknotes\\banknotes.csv"
# # path=pd.read_csv(path)
# # print(path)
# cd.crear_db(path,"Prueba",drive)
#  ##Palabra clave
# KEY_WORD="'(Con análisis)'"
#  #Lista con las bases de datos en dataframe type
# list_df=cd.enc_co(KEY_WORD,drive=drive)
#  #-------------Preprocesamos los datos---------#
# list_df=cd.prepro_da(list_df)
#  #Mandamos la lista de base de datos a la funcion para concatenar y exportar
# df=cd.pd_con(list_df,True)
#  #-------------leemos la base de datos original a la cual agregaremos los datos---------#
# df_original=pd.read_excel('Registros (1).xlsx')
# df=pd.read_excel('UNAL_EN_LA_REGION\Puntajeseft\Base_datos_conct.xlsx')
# # print(df['DOCUMENTO'].head())
# # print(df_original.DOCUMENTO.dtype)
# cd.union(df_original,df,True)






