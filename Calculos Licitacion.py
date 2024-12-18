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
from pathlib import Path
from S1_Parametros_Calculo import archivo_parametros, tipo_calculo, contrato
from S2_Funciones import asignacion_contratos, asignacion_vigencias, cumulos, licitado_desg_nl, recargos
from S3_Pre_Procesamiento import pre_procesamiento



def calculos_licitacion():
    # Traemos tablas de parametrizaciones que vamos a usar
    ruta_salidas='2 Output\\Prima de Reaseguro\\Pruebas Data Light\\'
    Path(ruta_salidas).mkdir(parents=True, exist_ok=True)
    contrato_cob = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
    parametros_contratos = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Vigencias')
    tasas_reaseguro = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Reaseguradores')
    cobs_old = pd.read_excel(io=archivo_parametros, sheet_name='Cobs Reas Desg NL Licitacion')
    ramo_reas_final = pd.read_excel(io=archivo_parametros, sheet_name='Ramo Reas Final Desg NL Licitac')
    nombre_prods = pd.read_excel(io=archivo_parametros, sheet_name='Nombre Productos Licitacion')
    ramo_reas_otros = pd.read_excel(io=archivo_parametros, sheet_name='Cobs Reas Otros Licitacion')
    
    # Calculos habituales del cierre
    df_0 = pre_procesamiento(tipo_calculo)
    df_0['REGISTROS']=1
    df_0 = recargos(df_0,calcula_recargos=0)
    df_1 = asignacion_contratos(df_0, contrato_cob, mantiene_na = 1)
    
    # Momentaneo, solo para poder revisar bien l contrato excedente 3000
    # for col in set(df_1.columns).difference(set(df_0.columns)):
    #     df_1[col] = np.where(df_1[col].isin([88,101,193]), np.nan, df_1[col])
    df_1 = df_1[~df_1['PRODUCTO'].isin([88,101,193])].copy()

    df_2,df_deleted_vigencia = asignacion_vigencias(df_1,parametros_contratos,tipo_calculo)
    df_3 = cumulos(df_2, 'RIESGO LIMITE INDIVIDUAL')
    df_4 = cumulos(df_3, 'RIESGO LIMITE CONTRATO')
    df_5 = cumulos(df_4, 'RIESGO RETENCION EXCEDENTE')
    df_5['CAPITAL CEDIDO POST EXCEDENTE'] = df_5['CAPITAL POST LIMITE CONTRATO'] * (1-df_5['PORCENTAJE RETENCION EXCEDENTE'])
    df_5['CAPITAL RETENIDO POST QS'] = df_5['CAPITAL RETENIDO POST EXCEDENTE'] * (1-np.where(df_5['CESION QS'].isnull(),0,df_5['CESION QS']))
    df_5['CAPITAL CEDIDO QS'] = df_5['CAPITAL RETENIDO POST EXCEDENTE'] - df_5['CAPITAL RETENIDO POST QS']
    df_5['CAPITAL RETENIDO TOTAL'] = df_5['MONTO ASEGURADO']-(df_5['CAPITAL CEDIDO POST EXCEDENTE'] + df_5['CAPITAL CEDIDO QS'])
    df_5['CAPITAL CEDIDO TOTAL'] = df_5['CAPITAL CEDIDO POST EXCEDENTE'] + df_5['CAPITAL CEDIDO QS']
    df_5['PORCENTAJE CEDIDO FINAL']=np.where(df_5['MONTO ASEGURADO']>0,df_5['CAPITAL CEDIDO TOTAL']/df_5['MONTO ASEGURADO'],df_5['CESION QS']*df_5['PORCENTAJE LIMITE INDIVIDUAL']*df_5['PORCENTAJE LIMITE CONTRATO']*df_5['PORCENTAJE RETENCION EXCEDENTE'])
    
    # Eliminamos df que no usaremos
    del df_2, df_3, df_4
    
    
    # Funcion para la licitacion
    
    if contrato=='Desgravamen No Licitado':
        df_5=df_5.merge(cobs_old,how='left',on=['CODIGO COBERTURA'])
    elif contrato in ['Digital Klare','K-Fijo','AP + Urgencias Medicas','Multisocios']:
        df_5=df_5.merge(ramo_reas_otros,how='left',on=['POL_PROD','CODIGO COBERTURA'])
    df_5=df_5.merge(nombre_prods,how='left',on=['PRODUCTO','BASE'])
    df_5['RAMO REAS CORREGIDO']=np.where(('DESG' not in df_5['NOMBRE PRODUCTO'])&(df_5['RAMO REAS']=='DESGRAVAMEN'),'VIDA',df_5['RAMO REAS'])
    df_5=df_5.merge(ramo_reas_final,how='left',on=['TIPO_POLIZA_LETRA','RAMO REAS CORREGIDO'])
    if contrato!='K-Fijo':campos=['RUT','SEXO','FEC_NAC','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO COBERTURA IAXIS','PLAN','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','ICAPITAL','PRIMA NETA ANUAL','FORMA_PAGO_CODIGO','BASE','TIPO_POLIZA_LETRA','CODIGO COBERTURA','EDAD INGRESO','EXPOSICION MENSUAL','TIPO ASEGURADO','EDAD RENOVACION','MESES RENTA','MONTO ASEGURADO','CONTRATO REASEGURO','COBERTURA DEL CONTRATO','CAPITAL RETENIDO TOTAL','CAPITAL CEDIDO TOTAL','PORCENTAJE CEDIDO FINAL','RAMO REAS','RAMO REAS CORREGIDO','COB REAS','PRODUCTO GES','RAMO REAS FINAL','NOMBRE PRODUCTO','RECARGO']
    else: campos=['RUT','SEXO','FEC_NAC','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO COBERTURA IAXIS','PLAN','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','ICAPITAL','PRIMA NETA ANUAL','FORMA_PAGO_CODIGO','BASE','TIPO_POLIZA_LETRA','CODIGO COBERTURA','EDAD INGRESO','EXPOSICION MENSUAL','EDAD RENOVACION','MONTO ASEGURADO','CONTRATO REASEGURO','COBERTURA DEL CONTRATO','CAPITAL RETENIDO TOTAL','CAPITAL CEDIDO TOTAL','PORCENTAJE CEDIDO FINAL','RAMO REAS','RAMO REAS CORREGIDO','COB REAS','PRODUCTO GES','RAMO REAS FINAL','NOMBRE PRODUCTO','RECARGO']

    # Reporteria
    # df_1[df_1['CONTRATO REASEGURO'].isnull()].to_csv(ruta_salidas+f'Registros sin Asignación de Reaseguro - {contrato}.csv',sep=';',index=False, date_format='%d-%m-%Y',decimal='.')
    df_1[df_1['CONTRATO REASEGURO'].notnull()].groupby(['BASE','PRODUCTO','POL_PROD','TIPO_POLIZA_LETRA','CODIGO COBERTURA','CODIGO COBERTURA IAXIS','COBERTURA DEL CONTRATO']).size().reset_index().to_csv(ruta_salidas+f'Resumen Asignaciones de Reaseguro - {contrato}.csv',sep=';',index=False, date_format='%d-%m-%Y',decimal='.')
    df_1[df_1['CONTRATO REASEGURO'].isnull()].groupby(['BASE','PRODUCTO','POL_PROD','TIPO_POLIZA_LETRA','CODIGO COBERTURA','CODIGO COBERTURA IAXIS']).size().reset_index().to_csv(ruta_salidas+f'Resumen Registros sin Asignacion de Reaseguro - {contrato}.csv',sep=';',index=False, date_format='%d-%m-%Y',decimal='.')
    # df_0.groupby(['POL_PROD','PRODUCTO','TIPO_POLIZA_LETRA'])[['ICAPITAL','MONTO ASEGURADO','REGISTROS']].sum().reset_index().to_csv(ruta_salidas+'Montos Asegurados df_0 POL_PROD '+contrato+'.csv',sep=';',index=False)
    # df_2.groupby(['POL_PROD','PRODUCTO','TIPO_POLIZA_LETRA'])[['ICAPITAL','MONTO ASEGURADO','REGISTROS']].sum().reset_index().to_csv(ruta_salidas+'Montos Asegurados df_2 POL_PROD '+contrato+'.csv',sep=';',index=False)
    # df_5.groupby(['POL_PROD','PRODUCTO','TIPO_POLIZA_LETRA'])[['ICAPITAL','MONTO ASEGURADO','REGISTROS']].sum().reset_index().to_csv(ruta_salidas+'Montos Asegurados df_5 POL_PROD '+contrato+'.csv',sep=';',index=False)
    df_5[campos].to_csv(ruta_salidas+'Detalle Licitacion '+contrato+'.txt',sep=';',decimal=',',date_format='%d-%m-%Y',index=False)
    

