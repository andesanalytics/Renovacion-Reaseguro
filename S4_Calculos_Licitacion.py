# -*- coding: utf-8 -*-
"""
CALCULO DE BASE DE LICITACION (EXCEDENTE 3,000 Y CAT XL VIDA)
"""

# Ignorar warnings asociados a la lista desplegable del excel de parametros
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

# Importamos librerias que vamos a utilizar
import pandas as pd
import numpy as np
from S1_Parametros_Calculo import archivo_parametros, tipo_calculo, contrato, ruta_otros
from S2_Funciones import asignacion_contratos, asignacion_vigencias, cumulos, recargos, cruce_left, identificador_anonimo
from S3_Pre_Procesamiento import pre_procesamiento

# df_0[df_0['MESES RENTA'].isnull()]['CODIGO COBERTURA'].unique()
# sum(df_0['EDAD RENOVACION']<18)

def calculos_licitacion(ruta_salidas):
    # Traemos tablas de parametrizaciones que vamos a usar
    contrato_cob = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
    parametros_contratos = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Vigencias')
    nombre_prods = pd.read_excel(io=archivo_parametros, sheet_name='Nombre Productos Licitacion')
    ramo_reas_otros = pd.read_excel(io=archivo_parametros, sheet_name='Ramo Reas Otros')
    ramo_reas_desgnl = pd.read_excel(io=archivo_parametros, sheet_name='Ramo Reas Desg NL')
    
    
    # Calculos habituales del cierre
    df = pre_procesamiento(tipo_calculo)
    df = identificador_anonimo(df, ['RUT'])
    df['REGISTROS']=1
    df = recargos(df,calcula_recargos=0)
    df = asignacion_contratos(df, contrato_cob, mantiene_na = 1)
    
    # Momentaneo, solo para poder revisar bien l contrato excedente 3000
    # for col in set(df.columns).difference(set(df.columns)):
    #     df[col] = np.where(df[col].isin([88,101,193]), np.nan, df[col])
    print(f'Se eliminarán {sum(df["PRODUCTO"].isin([88,101,193,369,370,371,372]))} registros que pertenecen a los productos de Hospitalario 100% y productos fallecimiento COVID')
    df = df[~df['PRODUCTO'].isin([88,101,193,369,370,371,372])].copy()

    df,df_deleted_vigencia = asignacion_vigencias(df,parametros_contratos,tipo_calculo, mantiene_na = 1)
    df = cumulos(df, 'RIESGO LIMITE INDIVIDUAL')
    df = cumulos(df, 'RIESGO LIMITE CONTRATO')
    df = cumulos(df, 'RIESGO RETENCION EXCEDENTE')
    df['CAPITAL CEDIDO POST EXCEDENTE'] = df['CAPITAL POST LIMITE CONTRATO'] * (1-df['PORCENTAJE RETENCION EXCEDENTE'])
    df['CAPITAL RETENIDO POST QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] * (1-np.where(df['CESION QS'].isnull(),0,df['CESION QS']))
    df['CAPITAL CEDIDO QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] - df['CAPITAL RETENIDO POST QS']
    df['CAPITAL RETENIDO TOTAL'] = df['MONTO ASEGURADO']-(df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS'])
    df['CAPITAL CEDIDO TOTAL'] = df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS']
    df['PORCENTAJE CEDIDO FINAL']=np.where(df['MONTO ASEGURADO']>0,df['CAPITAL CEDIDO TOTAL']/df['MONTO ASEGURADO'],df['CESION QS']*df['PORCENTAJE LIMITE INDIVIDUAL']*df['PORCENTAJE LIMITE CONTRATO']*df['PORCENTAJE RETENCION EXCEDENTE'])
    
    # Funcion para la licitacion
    if contrato=='Desgravamen No Licitado':
        df = cruce_left(df, ramo_reas_desgnl, ['COBERTURA DEL CONTRATO'], ['COBERTURA DEL CONTRATO'],name='ramo_reas_desgnl', ruta_output = ruta_salidas)
    elif contrato in ['Digital Klare','K-Fijo','AP + Urgencias Medicas','Multisocios']:
        df = cruce_left(df, ramo_reas_otros, ['POL_PROD','CODIGO COBERTURA'], ['POL_PROD','CODIGO COBERTURA'],name='ramo_reas_otros', ruta_output = ruta_salidas)
    df = cruce_left(df, nombre_prods, ['PRODUCTO','BASE'], ['PRODUCTO','BASE'],name='nombre_prods', ruta_output = ruta_salidas)
    if contrato == 'K-Fijo':
        df['MESES RENTA'] = 1
        df['TIPO ASEGURADO'] = 'Titular'
    campos_productos = ['FECHA_CIERRE','BASE','IDENTIFICADOR','RUT','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','NOMBRE_PRODUCTO','PLAN','CODIGO_COBERTURA','CODIGO_COBERTURA_IAXIS','CONTRATO_REASEGURO','COBERTURA_DEL_CONTRATO','RAMO_REAS','COB_REAS','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FEC_NAC','EDAD','SEXO','TIPO_POLIZA','FORMA_PAGO_CODIGO','MESES_RENTA','INNOMINADA','PRIMA_NETA_ANUAL','ICAPITAL','MONTO_ASEGURADO','CAPITAL_RETENIDO_TOTAL','CAPITAL_CEDIDO_TOTAL','RECARGO']
    campos_renovacion = ['FECHA_CIERRE','BASE','IDENTIFICADOR','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO_COBERTURA','RAMO_REAS','COB_REAS','TIPO_POLIZA','FECHA_EFECTO','FECHA_VENCIMIENTO','FEC_NAC','EDAD','SEXO','MONTO_ASEGURADO'] 

    # Reporteria
    df[df['CONTRATO REASEGURO'].notnull()].groupby(['BASE','PRODUCTO','POL_PROD','TIPO_POLIZA_LETRA','CODIGO COBERTURA','CODIGO COBERTURA IAXIS','COBERTURA DEL CONTRATO'])[['REGISTROS','MONTO ASEGURADO','CAPITAL RETENIDO TOTAL','CAPITAL CEDIDO TOTAL']].sum().reset_index().to_csv(ruta_salidas+f'Resumen Reaseguro - {contrato}.csv',sep=';',index=False, date_format='%d-%m-%Y',decimal='.')
    df[df['CONTRATO REASEGURO'].isnull()].groupby(['BASE','PRODUCTO','POL_PROD','TIPO_POLIZA_LETRA','CODIGO COBERTURA','CODIGO COBERTURA IAXIS']).size().reset_index().to_csv(ruta_salidas+f'Resumen Registros sin Asignacion de Reaseguro - {contrato}.csv',sep=';',index=False, date_format='%d-%m-%Y',decimal='.')
    # df.groupby(['POL_PROD','PRODUCTO','TIPO_POLIZA_LETRA'])[['ICAPITAL','MONTO ASEGURADO','REGISTROS']].sum().reset_index().to_csv(ruta_salidas+'Montos Asegurados df POL_PROD '+contrato+'.csv',sep=';',index=False)
    cols_new = []
    for col in df.columns:
        cols_new.append(col.replace(' ','_'))
    df.columns = cols_new    
    
    df[df['CONTRATO_REASEGURO'].notnull()][campos_productos].to_csv(ruta_salidas+f'Detalle Licitacion {contrato} Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    df[df['CONTRATO_REASEGURO'].notnull()][campos_renovacion].to_csv(ruta_salidas+f'Detalle Licitacion {contrato} Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    # respaldar_proceso(nombre_archivo, ruta_salidas, elimina_origen=1)
    
    
def resumen_bases_2023():
    cti=pd.read_csv(ruta_otros+'1. Inputs Auxiliares\\Otros\\'+'CTI.txt',sep=';',decimal='.',encoding='latin-1',low_memory=False)
    df_prev = pd.read_csv(f'1 Input\\Base {contrato} 2023.txt',sep=';',decimal=',',parse_dates=['FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION'],date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
    df_prev['CTI']=np.where(df_prev['PRODUCTO'].isin(list(cti['PRODUCTO'])),1,0)
    df_prev['POL_PROD']=np.where((df_prev['TIPO_POLIZA_LETRA']=='I')|(df_prev['CTI']==1),df_prev['PRODUCTO'],df_prev['POLIZA'])
    df_prev.groupby(['BASE','PRODUCTO','POL_PROD','TIPO_POLIZA_LETRA','CODIGO COBERTURA','CODIGO COBERTURA IAXIS','COBERTURA DEL CONTRATO'])[['MONTO ASEGURADO','CAPITAL RETENIDO TOTAL','CAPITAL CEDIDO TOTAL']].sum().reset_index().to_csv(f'2 Output\\Resumen {contrato} 2023.csv',sep=';',index=False, date_format='%d-%m-%Y',decimal='.')



