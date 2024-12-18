"""
PRIMA DE REASEGURO
"""
import pandas as pd
import numpy as np
import time
import datetime
import openpyxl
import cx_Oracle
from importlib import reload
from pathlib import Path
import os
from zipfile import ZipFile, ZIP_DEFLATED
import shutil
from S0_Inputs import archivo_calculos, archivo_querys, ruta_extensa

try:
    cx_Oracle.init_oracle_client(lib_dir=r"C:\app\instantclient_11_2")
except Exception as err:
    print("Whoops!")
    print(err)
    # sys.exit(1)

def ajusta_excel_parametros():
    fecha_inicio_mes=datetime.datetime(datetime.datetime.today().year,datetime.datetime.today().month,1)
    fecha_cierre=fecha_inicio_mes-datetime.timedelta(days=1)
    periodo=fecha_cierre.year*100+fecha_cierre.month
    excel_parametros = openpyxl.load_workbook(ruta_extensa+archivo_calculos)
    excel_parametros['Parametros'].cell(8,2).value=fecha_cierre
    excel_parametros.save(archivo_calculos)
    excel_parametros.close()
    excel_parametros = openpyxl.load_workbook(ruta_extensa+archivo_querys)
    excel_parametros['Parametros'].cell(6,2).value=periodo
    excel_parametros['Parametros'].cell(7,2).value=periodo
    excel_parametros.save(archivo_querys)
    excel_parametros.close()
    

def corre_procesos(exact_time_name_file):
    archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','a')
    archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Comienzo de calculo de Primas y Siniestros de Reaseguro \n')
    archivo_reporte_global.close()
    corridas=pd.read_excel(io=ruta_extensa+archivo_calculos,sheet_name='Corridas Automaticas')
    corridas=corridas[corridas['APLICA']==1]
    count=1
    procesos=corridas['NOMBRE PROCESO'].unique()
    for proceso in procesos:
        corridas_proceso=corridas[corridas['NOMBRE PROCESO']==proceso].copy()
        steps=corridas_proceso['STEP'].unique()
        for step in steps:
            print(f'Ejecutando el proceso {proceso} - paso nro {step}\n')
            corridas_proceso_step=corridas_proceso[corridas_proceso['STEP']==step].copy()
            excel_parametros = openpyxl.load_workbook(ruta_extensa+archivo_calculos)
            for index, row in corridas_proceso_step.iterrows():
                excel_parametros[row['HOJA']].cell(row['FILA'],row['COLUMNA']).value=row['VALOR']
            excel_parametros.save(archivo_calculos)
            tipo_calculo=excel_parametros[next(excel_parametros.defined_names['tipo_calculo'].destinations)[0]][next(excel_parametros.defined_names['tipo_calculo'].destinations)[1]].value
            contrato=excel_parametros[next(excel_parametros.defined_names['contrato'].destinations)[0]][next(excel_parametros.defined_names['contrato'].destinations)[1]].value
            fecha_cierre = excel_parametros[next(excel_parametros.defined_names['fecha_cierre'].destinations)[0]][next(excel_parametros.defined_names['fecha_cierre'].destinations)[1]].value
            periodo = fecha_cierre.year*100+fecha_cierre.month
            excel_parametros.close()
            if count==1: 
                import S1_Parametros_Calculo, S2_Funciones
                reload(S1_Parametros_Calculo)
                reload(S2_Funciones)
                import S3_Pre_Procesamiento, S4_Calculos_Actuariales
            else:
                reload(S1_Parametros_Calculo)
                reload(S2_Funciones)
                reload(S3_Pre_Procesamiento)
                reload(S4_Calculos_Actuariales)
            diccionario_procesos={'Prima de Reaseguro':S4_Calculos_Actuariales.prima_reaseguro,'Siniestros de Reaseguro':S4_Calculos_Actuariales.siniestros_reaseguro}
            diccionario_procesos[tipo_calculo]()
            archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','a')
            archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Calculo de {tipo_calculo} - contrato {contrato} - periodo {periodo} ejecutado correctamente\n')
            archivo_reporte_global.close()
            count+=1


def consolida_reportes(exact_time_name_file):
    archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','a')
    archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Comienza proceso de consolidacion de reportes \n')
    archivo_reporte_global.close()
    wb = openpyxl.load_workbook(ruta_extensa+archivo_calculos)
    separador_output=wb[next(wb.defined_names['separador_output'].destinations)[0]][next(wb.defined_names['separador_output'].destinations)[1]].value
    decimal_output=wb[next(wb.defined_names['decimal_output'].destinations)[0]][next(wb.defined_names['decimal_output'].destinations)[1]].value
    reportes=pd.read_excel(io=archivo_calculos,sheet_name='Consolida Reportes')
    reportes=reportes[reportes['APLICA']==1]
    if reportes.empty: return
    procesos=reportes['NOMBRE PROCESO'].unique()
    for proceso in procesos:
        reportes_proceso=reportes[reportes['NOMBRE PROCESO']==proceso].copy()
        df_consolida=pd.DataFrame()
        for index, row in reportes_proceso.iterrows():
            tipo_calculo=str(row['TIPO CALCULO'])
            contrato=str(row['CONTRATO'])
            periodo_reporte=str(int(row['PERIODO']))
            subcarpeta=str(row['SUBCARPETA'])
            nombre_reporte=str(row['NOMBRE ARCHIVO INPUT'])
            # Rutas de Salida
            if subcarpeta==None:subcarpeta=''
            else: subcarpeta=subcarpeta+'\\'
            ruta='2 Output\\'+tipo_calculo+'\\'+str(periodo_reporte)+'\\'+contrato+'\\'+subcarpeta
            reporte=pd.read_csv(ruta+nombre_reporte,sep=separador_output,decimal=decimal_output,encoding='latin-1')
            df_consolida=pd.concat([df_consolida,reporte],axis=0)
        df_consolida.to_csv(f'3 Reportes y Respaldos\\{proceso}.csv',sep=separador_output,decimal=decimal_output,encoding='latin-1',date_format='%d-%m-%Y',index=False)
        archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','a')
        archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Reporte {proceso} exportado correctamente \n')
        archivo_reporte_global.close()


def respaldar_proceso(exact_time_name_file):
    archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','a')
    archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Comienza proceso de respaldo de los calculos \n')
    archivo_reporte_global.close()
    # Cargar el archivo Excel con los parámetros
    respaldos = pd.read_excel(archivo_calculos, sheet_name='Respaldos')
    respaldos=respaldos[respaldos['APLICA']==1]
    if respaldos.empty: return
    respaldos=respaldos.replace(np.nan, '', regex=True)
    # Crear una nueva columna con la ruta completa para cada archivo
    respaldos['RUTA COMPLETA'] = np.where(respaldos['TIPO SALIDA']=='2 Output'\
                                          ,respaldos['TIPO SALIDA'] + '\\' + respaldos['TIPO CALCULO'] + '\\' + respaldos['PERIODO'].astype('string').str[0:6] + '\\' + respaldos['CONTRATO'] + '\\' + respaldos['SUBCARPETA']\
                                              ,respaldos['TIPO SALIDA'] + '\\' + respaldos['TIPO CALCULO'] + '\\' + respaldos['SUBCARPETA'] + '\\' + respaldos['CONTRATO'])
    procesos=respaldos['NOMBRE PROCESO'].unique()
    for proceso in procesos:
        ruta_salida='3 Reportes y Respaldos\\'+proceso
        Path(ruta_salida).mkdir(parents=True, exist_ok=True)
        respaldos_proceso=respaldos[respaldos['NOMBRE PROCESO']==proceso].copy()
        # Agrupar por NOMBRE ARCHIVO y comprimir los archivos 
        for index,row in respaldos_proceso.iterrows():
            nombre_archivo_input=row['NOMBRE ARCHIVO INPUT']
            nombre_ruta=row['RUTA COMPLETA']
            nombre_archivo_output=row['NOMBRE ARCHIVO OUTPUT']
            elimina_origen=row['ELIMINA ORIGEN']
            # Comprimir archivos del grupo en un archivo zip
            ruta_zip = os.path.join(ruta_salida, nombre_archivo_output + '.zip')
            with ZipFile(ruta_zip, 'w',ZIP_DEFLATED) as zipf:
                # Verificar si hay un 'NOMBRE ARCHIVO INPUT' para este archivo
                # Si hay un nombre de archivo, usar ese archivo en lugar de la ruta completa
                if nombre_archivo_input:
                    archivo_origen = os.path.join(nombre_ruta, nombre_archivo_input)
                    zipf.write(archivo_origen, nombre_archivo_input)
                else:
                    archivo_origen = nombre_ruta
                    for root, _, files in os.walk(archivo_origen):
                        # Iteramos sobre la lista de archivos en la carpeta actual
                        for file in files:
                            # Construimos la ruta completa de cada archivo en la carpeta actual
                            ruta_completa = os.path.join(root, file)
                            # print(ruta_completa)
                            # Agregamos el archivo al archivo zip
                            zipf.write(ruta_completa, os.path.relpath(ruta_completa, archivo_origen))
                            # print(archivo_origen)
                            # print(os.path.relpath(ruta_completa, archivo_origen))
                # Verificar si se debe eliminar el origen
            if elimina_origen == 1:
                #Verifica si es un archivo y lo elimina
                if os.path.isfile(archivo_origen):
                    os.remove(archivo_origen)
                #Elimina directorio
                else:
                    shutil.rmtree(archivo_origen)
            print(f'Proceso {proceso} - {nombre_archivo_output} realizado correctamente')
            archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','a')
            archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Respaldo {proceso} - {nombre_archivo_output} realizado correctamente \n')
            archivo_reporte_global.close()


def automatizacion_querys(exact_time_name_file) -> None:
    archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','a')
    archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Comienzo de extraccion de querys \n')
    archivo_reporte_global.close()
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
            ejecuta_query(consulta,periodo_inicio,periodo_fin,diccionario_querys,parametros_split,exact_time_name_file)
            
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
    # if sistema=='GES': connection = cx_Oracle.connect(user="USU_BCATALDO", password="Inicio_01.",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    if sistema=='GES': connection = cx_Oracle.connect(user="USU_SLABRIN", password="ZS_3Ngreso",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    # if sistema=='GES': connection = cx_Oracle.connect(user="USU_JTOBAR", password="31415Fermat",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    # if sistema=='GES': connection = oracledb.connect(user="USU_JTOBAR", password="31415Fermat",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida")
    # if sistema=='GES': connection = cx_Oracle.connect(user="USU_YDAVILA", password="Actuarial7777!",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
    if sistema=='IAXIS':connection = cx_Oracle.connect(user="USR_ZS_BCATALDO", password="JFhfehif23s.",dsn="zsiaxisbd.santanderseguros.cl.bsch:1521/praxis",encoding="UTF-8")    
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
    # Ajuste de los excel de parametros y querys para ejecutar el cierre
    # ajusta_excel_parametros()
    
    t = time.localtime()
    exact_time_name_file = time.strftime("%Y-%m-%d %H%M%S", t)
    archivo_reporte_global=open(f'{ruta_extensa}4 Reportes del Proceso\\Reporte Proceso Cierre {exact_time_name_file}.txt','w')
    archivo_reporte_global.write(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))} - Comienzo de reporte calculos del proceso de cierre \n')
    archivo_reporte_global.close()
    
    # Ejecucion de Querys
    automatizacion_querys(exact_time_name_file)
    
    # Proceso de Calculo de Prima y Siniestros de Reaseguro
    # corre_procesos(exact_time_name_file)
    
    # Consolida Reportes
    # consolida_reportes(exact_time_name_file)
    
    # Respaldo de los Inputs y Resultados del Proceso
    # respaldar_proceso(exact_time_name_file)
