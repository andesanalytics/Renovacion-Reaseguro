# -*- coding: utf-8 -*-
"""
Modulo que realiza los calculos de renovacion para cada uno de los contratos solicitados, para luego consolidar en los formatos establecidos.
"""
import pandas as pd
import openpyxl
from typing import Any
from pathlib import Path
from S0_Loaders import Parameter_Loader
from S1_Parametros_Calculo import carga_parametros
from S2_Funciones import automatizacion_querys
from S4_Calculos_Renovacion import calculos_renovacion


# * Carga de variables iniciales
# Cargamos el Parameter_Loader que contendrá la informacion de los excel de parametros a utilizar durante el proceso
files: Parameter_Loader = Parameter_Loader(excel_file='Inputs Archivos Excel.xlsx', open_wb=True, ruta_extensa='')
files.get_reference(reference='archivo_calculos')
files.get_reference(reference='archivo_querys')
files.get_reference(reference='archivo_parametros')
# Rutas
ruta_salidas: str = '2 Output\\Resultados Validacion V1\\'
Path(ruta_salidas).mkdir(parents=True, exist_ok=True)
# Contratos a ejecutar y contratos a consolidar en el reporte de CAT XL
contratos_ejecutar: list[str] = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']
contratos_consolidar_catxl: list[str] = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']

# ? Control: Solo ejecutará este script si es ejecutado desde aqui y no es llamado desde otro script
if __name__=='__main__':
    # Ciclo para elegir si extraemos o no la data de expuestos conectandose a SQL
    while True:
        opcion: str = input("Desea Ejecutar Extracción de Querys (Y/N)")
        if opcion == "Y":
            automatizacion_querys(files)
            break
        elif opcion == "N":
            print('No se ejecutan extracciones de querys')
            break
        else:
            print("\nOpción no válida. Por favor, elige una opcion correcta (Y/N).")
    # Cargamos una instancia de Parameter_Loader que contendrá las tablas de parametros
    tables: Parameter_Loader = Parameter_Loader(excel_file=files.parameters['archivo_parametros'], open_wb=True, ruta_extensa='')
    # ? Ciclo que ejecuta los contratos de renovacion
    for contrato_ejecutar in contratos_ejecutar:
        print(f'Ejecutando contrato {contrato_ejecutar}')
        # Cambia en el excel de parametros de calculo los datos del contrato que ejecutaremos
        excel_parametros = openpyxl.load_workbook(files.parameters['archivo_calculos'])
        excel_parametros['Parametros'].cell(7,2).value=contrato_ejecutar
        excel_parametros.save(files.parameters['archivo_calculos'])
        excel_parametros.close()
        # Una vez cambiado eso, genera una isntancia Parameter_Loader que contendrá los parametros de calculo
        parameters: Parameter_Loader = Parameter_Loader(excel_file=files.parameters['archivo_calculos'], open_wb=True)
        # cargamos tablas y ejecutamos los calculos de renovacion
        carga_parametros(files, parameters)
        calculos_renovacion(parameters, tables, ruta_salidas)
    # ? Ciclo que consolida los contratos de renovacion para el reporte CAT XL
    print('Comienza el proceso de consolidación de reportes')
    df_catxl_uso_interno = pd.DataFrame()
    df_catxl_reaseguradores= pd.DataFrame()
    # Por cada contrato extraemos el reporte de uso interno y el reporte que va a los reaseguradores, y los vamos consolidando
    for contrato_consolidar in contratos_consolidar_catxl:
        print(f'Leyendo data del contrato {contrato_consolidar}')
        df_uso_interno: pd.DataFrame = pd.read_csv(f'{ruta_salidas}Detalle Renovacion {contrato_consolidar} Uso Interno.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
        df_reaseguradores: pd.DataFrame = pd.read_csv(f'{ruta_salidas}Detalle Renovacion {contrato_consolidar} Reaseguradores.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
        df_catxl_uso_interno = pd.concat([df_catxl_uso_interno,df_uso_interno])
        df_catxl_reaseguradores = pd.concat([df_catxl_reaseguradores,df_reaseguradores])
    print('Exportamos Reportes Finales!')
    # exportaciones finales
    df_catxl_uso_interno.to_csv(f'{ruta_salidas}Detalle Renovacion Cat XL Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    df_catxl_reaseguradores.to_csv(f'{ruta_salidas}Detalle Renovacion Cat XL Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)


