#!/usr/bin/env python
# coding: utf-8

# # **Proyecto final - Ingenierìa de datos**
# 
# **Integrantes:**
# - Diego Alfonso Rivas Araniva
# - Edwin Josué Olmedo López 
# - Emerson Francisco Cartagena Candelario
# - Raúl Anibal Arévalo Alvarado

# # **Importacion de dependencias, configuracion de logger y de variables de entorno**

# In[2]:


import requests as rq
import pandas as pd
import os
import pickle
import logging
import datetime
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv

# --- Configuración de logging ---
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    handlers=[
        logging.FileHandler("logs.json", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Carga de variables de entorno
load_dotenv()
logging.info("Configuraciones iniciales cargadas correctamente.")


# # **Extracción de datos**

# In[4]:


def extracion_datos_empresas():

    # Listado de monedas a consultar
    coins = ["bitcoin", "ethereum"]
    # Lista para almacenar los DataFrames de cada moneda
    all_data = []

    # Iterar sobre cada moneda para obtener los datos de las empresas que invierten en ellas
    for coin in coins:
        try:
            url_company = f"{os.environ.get('VARIABLE-COMPANIES-URL')}/{coin}"
            response_company = rq.get(url_company)

            # Verificar si la solicitud fue exitosa
            if response_company.status_code == 200:
                logging.info(f"Datos de empresas con monedas: {coin} obtenidos exitosamente...")
                data = response_company.json()

                # Normalizar archivo JSON y agregar columna de moneda (bajo encabezado 'id', para que coincida en el merge)
                df_company = pd.json_normalize(data["companies"])
                df_company["id"] = coin
                # Agregar DataFrame a la lista
                all_data.append(df_company)
            else:
                logging.error(f"Error al obtener empresa con moneda: {coin}. Detalle: {response_company.status_code}")
        except Exception as e:
            logging.error(f"Error: {str(e)}")

    logging.info("Datos de empresas obtenidos exitosamente...")
    # Concatenar todos los DataFrames en uno solo
    return pd.concat(all_data, ignore_index=True)

def extracion_datos_monedas():
    try:
        url_coin = os.environ.get("VARIABLE-COINS-URL")
        response_coin = rq.get(url_coin)

        # Verificar si la solicitud fue exitosa
        if response_coin.status_code == 200:
            logging.info("Datos de monedas obtenidos exitosamente...")
            # Normalizar archivo JSON
            return pd.json_normalize(response_coin.json())
        else:
            logging.error(f"Error: {response_coin.status_code}")
    except Exception as e:
        logging.error(f"Error: {str(e)}")


# # **Limpieza y transformacion de datos**

# In[6]:


def combinar_dataframes(df_companies, df_coins):
    logging.info("Combinando DataFrames...")

    # Verificar que ambos DataFrames no estén vacíos antes de combinar
    if not df_companies.empty and not df_coins.empty:
        # Combinar DataFrames usando la columna 'id' como clave
        dataframe = pd.merge(df_companies, df_coins, on="id")
        logging.info("DataFrames combinados correctamente.")
        # Mostrar las primeras 200 filas del DataFrame combinado para fines comparativos
        return dataframe
    else:
        logging.error("Error: Uno o ambos DataFrames están vacíos.")

def transformar_dataframe(dataframe):
    logging.info("Verificando tipos de datos de columnas...")
    logging.info("Iniciando transformación...")
    # Tipar columnas numéricas como categoricas
    for col in ["name_x","symbol_x","country","id","symbol_y","name_y","roi.currency"]:
        dataframe[col] = dataframe[col].astype("category")

    # Tipar columnas string como fechas
    for col in ['ath_date', 'atl_date', 'last_updated']:
        dataframe[col] = pd.to_datetime(dataframe[col])

    logging.info("Transformación finalizada exitosamente.")

def limpiar_dataframe(dataframe):
    logging.info("Iniciando limpieza de datos...")
    # Lista de columnas numéricas a rellenar con 0 en caso de NaN
    number_columns = [
        "total_holdings", "total_entry_value_usd", "total_current_value_usd", "percentage_of_total_supply",
        "current_price", "high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h",
        "market_cap_change_percentage_24h", "ath", "ath_change_percentage", "atl", "atl_change_percentage",
        "roi.times", "roi.percentage"
    ]
    # Creando categoría 'N/A' para la columna 'roi.currency'
    dataframe['roi.currency'] = dataframe['roi.currency'].cat.add_categories('N/A')

    # Rellenando NaN en columnas numéricas con 0 y en 'roi.currency' con 'N/A'
    dataframe[number_columns] = dataframe[number_columns].fillna(0)
    dataframe['roi.currency'] = dataframe['roi.currency'].fillna('N/A')
    logging.info("Limpieza de datos ejecutada satisfactoriamente.")

def eliminar_elementos_innecesarias(dataframe):
    logging.info("Eliminando duplicados (si hay)...")
    # Eliminando duplicados
    dataframe.drop_duplicates(inplace=True)
    logging.info("Eliminando columnas innecesarias...")
    # Eliminando columnas innecesarias, por ejemplo, el id tiene el mismo valor que name_x, la columna image no aporta ningun calculo financiero y la columna roi tiene un diccionario que ya se desgloso en otras columnas cuando se hizo el merge
    dataframe.drop(['id', 'image', 'roi'], axis=1, inplace=True)
    logging.info("Columnas eliminadas correctamente.")

def renombrar_columnas (dataframe):
    logging.info("Renombrando columnas al español...")
    # Creando un diccionario para renombrar las columnas al español
    dataframe.rename(columns={
        "name_x": "empresa",
        "symbol_x": "ticker_empresa",
        "country": "pais",
        "total_holdings": "total_monedas",
        "total_entry_value_usd": "valor_inicial_usd",
        "total_current_value_usd": "valor_actual_usd",
        "percentage_of_total_supply": "porcentaje_total_supply",
        "symbol_y": "ticker_activo",
        "name_y": "activo",
        "current_price": "precio_actual",
        "ath": "maximo_historico",
        "ath_change_percentage": "cambio_desde_ath_pct",
        "ath_date": "fecha_ath",
        "atl": "minimo_historico",
        "atl_change_percentage": "cambio_desde_atl_pct",
        "atl_date": "fecha_atl",
        "last_updated": "ultima_actualizacion",
        "roi.times": "roi_multiplo",
        "roi.currency": "roi_moneda",
        "roi.percentage": "roi_porcentaje",
        "market_cap": "capitalizacion_mercado",
        "market_cap_rank": "ranking_capitalizacion",
        "fully_diluted_valuation": "valor_total_diluido_usd",
        "total_volume": "volumen_total_24h",
        "high_24h": "maximo_24h",
        "low_24h": "minimo_24h",
        "price_change_24h": "cambio_precio_24h",
        "price_change_percentage_24h": "cambio_precio_pct_24h",
        "market_cap_change_24h": "cambio_capitalizacion_24h",
        "market_cap_change_percentage_24h": "cambio_capitalizacion_pct_24h",
        "circulating_supply": "supply_circulante",
        "total_supply": "supply_total",
        "max_supply": "supply_maximo"
    }, inplace=True)

    logging.info("Columnas renombradas al español correctamente.")
    # Mostrar las primeras 200 filas del DataFrame final con fines comparativos


# # **Carga de datos (datalake improvisado en GDrive)**

# In[8]:


def cargar_datos_gdrive(dataframe):
    logging.info("Iniciando carga de datos a Google Drive...")
    # Variable creada para asignar nombre al archivo con la fecha actual
    date_now = datetime.date.today()
    # Guardando DataFrame como archivo CSV localmente
    dataframe.to_csv(f"Dataset {date_now}.csv", index=False)
    # Configuración de la API de Google Drive
    scopes = ['https://www.googleapis.com/auth/drive.file']
    # Variable para almacenar las credenciales
    creds = None

    try:
        # Verificar si el archivo token.pickle existe para cargar las credenciales
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # Si no hay credenciales válidas, iniciar el flujo de autenticación
        if not creds or not creds.valid:
            # Refrescar las credenciales si están expiradas
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                logging.info("Por favor, ingrese sus credenciales de Google.")
                # Obtener nuevas credenciales desde el archivo client_secrets.json
                flow = InstalledAppFlow.from_client_secrets_file(os.environ.get("VARIABLE-AUTH-SECRET"), scopes)
                # Asignar las credenciales obtenidas a la variable creds
                creds = flow.run_local_server(port=0)
            # Guardar las credenciales para la próxima ejecución
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        logging.info("Subiendo archivo a Google Drive...")
        # Construir el servicio de Google Drive
        drive_service = build('drive', 'v3', credentials=creds)

        # Metadata del archivo a subir
        file_metadata = {
            'name': f'Dataset {date_now}.csv',
            'parents': [os.environ.get("VARIABLE-FOLDER-ID")]
        }
        media = MediaFileUpload(f"Dataset {date_now}.csv", mimetype='text/csv')

        # Subir el archivo a Google Drive
        drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()

        logging.info("Archivo subido exitosamente a Google Drive.")
    except Exception as e:
        logging.error(f"Error: {str(e)}")


# # **Pipeline ETL**

# In[10]:


# --- Función pipeline ---

def trigger_pipeline():
    try:
        logging.info("Iniciando pipeline ETL...")

        df_companies = extracion_datos_empresas()
        df_coins = extracion_datos_monedas()

        dataframe_final = combinar_dataframes(df_companies, df_coins)
        transformar_dataframe(dataframe_final)
        limpiar_dataframe(dataframe_final)
        eliminar_elementos_innecesarias(dataframe_final)
        renombrar_columnas(dataframe_final)
        cargar_datos_gdrive(dataframe_final)
        logging.info("Pipeline ETL completado exitosamente.")

    except Exception as e:
        logging.exception(f"Error durante la ejecución del pipeline: {str(e)}")


# In[11]:


# Ejecutando el pipeline
trigger_pipeline()


# In[ ]:




