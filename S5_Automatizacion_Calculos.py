# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 19:21:59 2024

@author: x958860
"""
import pandas as pd
import numpy as np
import datetime
import time
import openpyxl
import cx_Oracle
from typing import Any
from pathlib import Path
from importlib import reload
from S0_Loaders import Parameter_Loader
from S1_Parametros_Calculo import carga_parametros
from S2_Funciones import automatizacion_querys
from S4_Calculos_Renovacion import calculos_renovacion



files: Parameter_Loader = Parameter_Loader(excel_file='Inputs Archivos Excel.xlsx', open_wb=True, ruta_extensa='')
files.get_reference(reference='archivo_calculos')
files.get_reference(reference='archivo_querys')
files.get_reference(reference='archivo_parametros')

ruta_salidas: str = '2 Output\\Resultados 2024-12-20\\'
Path(ruta_salidas).mkdir(parents=True, exist_ok=True)
contratos_ejecutar: list[str] = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']
contratos_consolidar_catxl: list[str] = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']


if __name__=='__main__':
    querys: Parameter_Loader = files.parameters['archivo_querys']
    tables: Parameter_Loader = files.parameters['archivo_parametros']
    for contrato_ejecutar in contratos_ejecutar:
        print(f'Ejecutando contrato {contrato_ejecutar}')
        excel_parametros = openpyxl.load_workbook(archivo_calculos)
        excel_parametros['Parametros'].cell(7,2).value=contrato_ejecutar
        excel_parametros.save(archivo_calculos)
        excel_parametros.close()
        
        parameters: Parameter_Loader = Parameter_Loader(excel_file=archivos.parameters['archivo_calculos'], open_wb=True)
        carga_parametros(files, parameters)
        # tipo_calculo=excel_parametros[next(excel_parametros.defined_names['tipo_calculo'].destinations)[0]][next(excel_parametros.defined_names['tipo_calculo'].destinations)[1]].value
        # fecha_cierre = excel_parametros[next(excel_parametros.defined_names['fecha_cierre'].destinations)[0]][next(excel_parametros.defined_names['fecha_cierre'].destinations)[1]].value
        # periodo = fecha_cierre.year*100+fecha_cierre.month
        calculos_renovacion(ruta_salidas)
    df_catxl_uso_interno = pd.DataFrame()
    df_catxl_reaseguradores= pd.DataFrame()
    for contrato_consolidar in contratos_consolidar_catxl:
        df_uso_interno = pd.read_csv(f'{ruta_salidas}Detalle Licitacion {contrato_consolidar} Uso Interno.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
        df_reaseguradores = pd.read_csv(f'{ruta_salidas}Detalle Licitacion {contrato_consolidar} Reaseguradores.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
        df_catxl_uso_interno = pd.concat([df_catxl_uso_interno,df_uso_interno])
        df_catxl_reaseguradores = pd.concat([df_catxl_reaseguradores,df_reaseguradores])
    df_catxl_uso_interno.to_csv(f'{ruta_salidas}Detalle Licitacion Cat XL Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    df_catxl_reaseguradores.to_csv(f'{ruta_salidas}Detalle Licitacion Cat XL Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)

    # Ejecucion de Querys
    # automatizacion_querys()
    # Ejecuta Procesos    
    # ejecuta_contratos(ruta_salidas)