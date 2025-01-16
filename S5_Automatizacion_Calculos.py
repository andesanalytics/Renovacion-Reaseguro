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
from pathlib import Path
from importlib import reload
from S0_Inputs import archivo_calculos, archivo_querys, ruta_extensa

ruta_salidas='2 Output\\Resultados 2024-12-20\\'
Path(ruta_salidas).mkdir(parents=True, exist_ok=True)


def ejecuta_contratos(ruta_salidas):
    contratos_ejecutar = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']
    contratos_consolidar_catxl = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']
    # contratos_ejecutar = []
    # contratos_consolidar_catxl = []
    count = 1
    for contrato_ejecutar in contratos_ejecutar:
        print(f'Ejecutando contrato {contrato_ejecutar}')
        excel_parametros = openpyxl.load_workbook(archivo_calculos)
        excel_parametros['Parametros'].cell(7,2).value=contrato_ejecutar
        excel_parametros.save(archivo_calculos)
        excel_parametros.close()
        # tipo_calculo=excel_parametros[next(excel_parametros.defined_names['tipo_calculo'].destinations)[0]][next(excel_parametros.defined_names['tipo_calculo'].destinations)[1]].value
        # fecha_cierre = excel_parametros[next(excel_parametros.defined_names['fecha_cierre'].destinations)[0]][next(excel_parametros.defined_names['fecha_cierre'].destinations)[1]].value
        # periodo = fecha_cierre.year*100+fecha_cierre.month
        if count==1: 
            import S1_Parametros_Calculo, S2_Funciones
            reload(S1_Parametros_Calculo)
            reload(S2_Funciones)
            import S3_Pre_Procesamiento, S4_Calculos_Renovacion
        else:
            reload(S1_Parametros_Calculo)
            reload(S2_Funciones)
            reload(S3_Pre_Procesamiento)
            reload(S4_Calculos_Renovacion)
        S4_Calculos_Renovacion.calculos_licitacion(ruta_salidas)
        count = count + 1
    df_catxl_uso_interno = pd.DataFrame()
    df_catxl_reaseguradores= pd.DataFrame()
    for contrato_consolidar in contratos_consolidar_catxl:
        df_uso_interno = pd.read_csv(f'{ruta_salidas}Detalle Licitacion {contrato_consolidar} Uso Interno.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
        df_reaseguradores = pd.read_csv(f'{ruta_salidas}Detalle Licitacion {contrato_consolidar} Reaseguradores.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
        df_catxl_uso_interno = pd.concat([df_catxl_uso_interno,df_uso_interno])
        df_catxl_reaseguradores = pd.concat([df_catxl_reaseguradores,df_reaseguradores])
    df_catxl_uso_interno.to_csv(f'{ruta_salidas}Detalle Licitacion Cat XL Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    df_catxl_reaseguradores.to_csv(f'{ruta_salidas}Detalle Licitacion Cat XL Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)


def automatizacion_querys() -> None:
    # Parametros de la consulta
    wb = openpyxl.load_workbook(ruta_extensa + archivo_querys)
    periodo_inicio=wb[next(wb.defined_names['periodo_inicio'].destinations)[0]][next(wb.defined_names['periodo_inicio'].destinations)[1]].value
    periodo_fin=wb[next(wb.defined_names['periodo_fin'].destinations)[0]][next(wb.defined_names['periodo_fin'].destinations)[1]].value
    wb.close()
    parametros_split=pd.read_excel(io=ruta_extensa + archivo_querys, sheet_name='Split Querys').replace(np.nan, '', regex=True)
    parametros_querys=pd.read_excel(io=ruta_extensa + archivo_querys, sheet_name='Diccionario Querys').replace(np.nan, '', regex=True)
    diccionario_querys=parametros_querys.set_index('QUERY').to_dict()
    
    for consulta in parametros_querys['QUERY']:
        aplica=diccionario_querys['APLICA'][consulta]
        if aplica==1: 
            ejecuta_query(consulta,periodo_inicio,periodo_fin,diccionario_querys,parametros_split)
            
def ejecuta_query(consulta, periodo_inicio, periodo_fin, diccionario_querys, parametros_split, name_file = None):
    # Tiempo inicial
    start_time = time.time()
    
    # Mostramos en pantalla que query estamos realizando
    print('Realizando consulta {}'.format(consulta))
    
    # Calculos sobre el diccionario de contratos
    columnas=diccionario_querys['CAMPOS QUERY'][consulta].split(',')
    cols_date = diccionario_querys['CAMPOS FECHAS'][consulta].split(',') if diccionario_querys['CAMPOS FECHAS'][consulta] else []
    sistema=diccionario_querys['SISTEMA'][consulta]
    desfase_meses=diccionario_querys['DESFASE'][consulta]
    tipo_exportar=diccionario_querys['TIPO EXPORTAR'][consulta]
    carpeta=diccionario_querys['CARPETA'][consulta]
    subcarpeta=diccionario_querys['SUBCARPETA'][consulta]
    tipo_calculo=diccionario_querys['TIPO CALCULO'][consulta]
    
    # Calculo de fechas y periodos y otros parametros
    fecha_inicio=datetime.datetime(int(periodo_inicio/100),periodo_inicio%100,1)
    fecha_inicio=fecha_inicio-pd.offsets.MonthEnd(desfase_meses+1)+datetime.timedelta(days=1)
    fecha_fin=datetime.datetime(int(periodo_fin/100),periodo_fin%100,1)
    fecha_fin=fecha_fin-pd.offsets.MonthEnd(desfase_meses)
    periodo_fin=fecha_fin.year*100+fecha_fin.month
    periodo_inicio=fecha_inicio.year*100+fecha_inicio.month
    if carpeta: ruta_exportar_query=f'{ruta_extensa}1 Input\\{tipo_calculo}\\{subcarpeta}\\{carpeta}\\'
    else: ruta_exportar_query=f'{ruta_extensa}1 Input\\{tipo_calculo}\\{subcarpeta}\\'
    # Traemos el archivo de la query
    with open(ruta_extensa+'0 Querys Automaticas\\'+consulta+'.sql', 'r') as query_txt: query = query_txt.read().replace('\n',' ').replace('fecha_inicio',str(fecha_inicio)[0:10]).replace('fecha_fin',str(fecha_fin)[0:10]).replace('periodo_fin',str(periodo_fin)[0:10]).replace('año_proceso',str(fecha_fin.year)).replace('mes_proceso',str(fecha_fin.month))

    # Conexion sql
    if sistema=='GES': connection = cx_Oracle.connect(user="USU_BCATALDO", password="SAmu3l.20204*",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    # if sistema=='GES': connection = cx_Oracle.connect(user="USU_SLABRIN", password="ZS_3Ngreso",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    # if sistema=='GES': connection = cx_Oracle.connect(user="USU_JTOBAR", password="31415Fermat",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    # if sistema=='GES': connection = oracledb.connect(user="USU_JTOBAR", password="31415Fermat",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida")
    # if sistema=='GES': connection = cx_Oracle.connect(user="USU_YDAVILA", password="Actuarial7777!",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    if sistema=='IAXIS':connection = cx_Oracle.connect(user="USR_ZS_BCATALDO", password="SAturn0.20204*",dsn="zsiaxisbd.santanderseguros.cl.bsch:1521/praxis",encoding="UTF-8")    
    # if sistema=='IAXIS':connection = oracledb.connect(user="USR_ZS_BCATALDO", password="Inicio_01",dsn="zsiaxisbd.santanderseguros.cl.bsch:1521/praxis")    
    if sistema=='IAXIS TEST':connection = cx_Oracle.connect(user="AXIS_D401", password="F|=fX10JZ{p9",dsn="180.153.43.74:1521/praxis_predb",encoding="UTF-8")    
    # Lectura de datos y pasamos a dataframe
    cursor = connection.cursor()
    print('Ejecutando query')
    cursor.execute(query)
    # cursor.execute("select t.aserut rut from altavida.t0058 t where rownum<100")
    print('Pegando resultados de la query')
    resultado_query = cursor.fetchall()
    df=pd.DataFrame(list(resultado_query))
    if df.empty: 
        print(f'La consulta - {consulta} - arrojó 0 registros como resultado. REVISAR!')
        if name_file is not None:
            archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {name_file}.txt','a')
            archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Se ejecutó query {consulta}, pero el resultado arrojó una respuesta vacia. REVISAR!\n')
            archivo_reporte_global.close()
        return
    df.columns=columnas
    # Conversión a fechas de las columnas
    if len(cols_date)>0:    
        for col in cols_date:
            if df[col].dtype==object: df[col]=pd.to_datetime(df[col],format='%Y-%m-%d', errors='coerce')
    
    # Exportamos la data
    print('Exportando datos de la query. La consulta tiene {} registros'.format(df.shape[0]))
    
    if tipo_exportar=='historico':terminacion_archivo='.txt' 
    if (tipo_exportar=='periodo')&(periodo_fin==periodo_inicio):terminacion_archivo=' '+str(periodo_fin)+'.txt'
    if (tipo_exportar=='periodo')&(periodo_fin!=periodo_inicio):terminacion_archivo=' '+str(periodo_inicio)+'-'+str(periodo_fin)+'.txt'
    if tipo_exportar=='fecha':terminacion_archivo=' ('+str(datetime.datetime.now())[0:10]+').txt'
    nombre_archivo_salida=consulta+terminacion_archivo
    ######## DEBEMOS PEGAR DIRECTAMENTE EN LA RUTA QUE CORRESPONDE
    ######## ADEMAS, DEBEMOS IMPORTAR EL SEPARADOR Y DECIMAL INPUT DE PARAMETROS PARA QUE COINCIDA CON LO QUE VAMOS A LEER EN EL CALCULO
    if carpeta:
        Path(ruta_exportar_query).mkdir(parents=True, exist_ok=True)
        df.to_csv(ruta_exportar_query+nombre_archivo_salida,sep=';',decimal='.',encoding='UTF-8',date_format='%d-%m-%Y',index=False)
    parametros_split_filter=parametros_split[parametros_split['QUERY']==consulta]
    if not parametros_split_filter.empty : split_querys(df,parametros_split_filter,ruta_exportar_query,terminacion_archivo,sistema)
    # Mido tiempo de ejecucion
    total_time = round((time.time()-start_time)/60, 2)
    print('El tiempo total de ejecución fue de %s minutos' % total_time)
    if name_file is not None:
        archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {name_file}.txt','a')
        archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Se ejecutó query {consulta} \n')
        archivo_reporte_global.close()


def split_querys(df,parametros_split_filter,ruta_exportar_query,terminacion_archivo,sistema):
    df_aux=df.copy()
    for index,row in parametros_split_filter.iterrows():
        contrato=row['CONTRATO']
        productos=list(map(int,row['PRODUCTOS CONTRATO'].split('-'))) if row['PRODUCTOS CONTRATO'] else ''
        polizas=list(map(int,row['POLIZAS CONTRATO'].split('-'))) if row['POLIZAS CONTRATO'] else ''
        tipo_condicion=row['TIPO CONDICION']
        aplica_split=row['APLICA']
        cond_prods=pd.Series(np.full(df_aux.shape[0],True)) if not productos else df_aux['PRODUCTO'].isin(productos) if tipo_condicion==1 else ~df_aux['PRODUCTO'].isin(productos)
        cond_pols=pd.Series(np.full(df_aux.shape[0],True)) if not polizas else df_aux['POLIZA'].isin(polizas) if tipo_condicion==1 else ~df_aux['POLIZA'].isin(polizas)
        df_export=df_aux[cond_prods&cond_pols].copy()
        df_aux=df_aux.loc[df_aux.index.difference(df_export.index)].reset_index(drop=True)
        terminacion_ges=' GES' if sistema=='GES' else ''
        Path(f'{ruta_exportar_query}{contrato}\\').mkdir(parents=True, exist_ok=True)
        if (not df_export.empty)&(aplica_split==1): df_export.to_csv(f'{ruta_exportar_query}{contrato}\\Expuestos {contrato}{terminacion_ges}{terminacion_archivo}',sep=';',decimal='.',encoding='UTF-8',date_format='%d-%m-%Y',index=False)

        

if __name__=='__main__':
    # Ejecucion de Querys
    # automatizacion_querys()
    # Ejecuta Procesos    
    ejecuta_contratos(ruta_salidas)