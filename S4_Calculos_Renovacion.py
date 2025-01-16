# -*- coding: utf-8 -*-
"""Modulo para calcular la renovación de reaseguro para un contrato específico.
"""


# Ignorar warnings asociados a la lista desplegable del excel de parametros
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
# Importamos librerias que vamos a utilizar
import pandas as pd
import numpy as np
from pathlib import Path
# Importamos parametros necesarios de scripts previos del modelo
from S1_Parametros_Calculo import archivo_parametros, tipo_calculo, contrato, ruta_otros
# Importamos las funciones que requerimos del script de funciones
from S2_Funciones import asignacion_contratos, asignacion_vigencias, cumulos, recargos, cruce_left, identificador_anonimo
# Importamos la funcion de preprocesamiento del script que preprocesa los datos
from S3_Pre_Procesamiento import pre_procesamiento


def calculos_renovacion(ruta_salidas: str) -> None:
    """Realiza los calculos de asociados a la renovacion de reaseguro para un contrato en específico.

    Parameters
    ----------
    ruta_salidas : str
        Contiene la ruta donde se guardarán los resultados principales de los calculos
    """
    
    # * Traemos tablas de parametrizaciones que vamos a usar
    
    # Data que contiene matriz de asignacion de contratos de reaseguro por distintas variables (poliza, producto, cobertura, entre otras variables)
    contrato_cob: pd.DataFrame = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
    # Matriz que permite asignar a que vigencia del contrato está cada registro
    parametros_contratos: pd.DataFrame = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Vigencias')
    # Matriz para asignar el nombre del producto
    nombre_prods: pd.DataFrame = pd.read_excel(io=archivo_parametros, sheet_name='Nombre Productos Renovacion')
    # Matrices para asignar los campos RAMO_REAS y COB_REAS 
    # ! Estos campos son solicitados por el área de productos y son dos matrices porque una es para el contrato de reaseguro de Desgravamen No Licitado, mientras que la otra matriz es para el resto de contratos
    ramo_reas_otros: pd.DataFrame = pd.read_excel(io=archivo_parametros, sheet_name='Ramo Reas Otros')
    ramo_reas_desgnl: pd.DataFrame = pd.read_excel(io=archivo_parametros, sheet_name='Ramo Reas Desg NL')
    
    # * Calculos previos del proceso antes de asignar el contrato de reaseguro
    # Preprocesamiento de la Data: A las querys que se extraen de los sistemas de administracion de BBDD (GES e Iaxis) se le realizan ciertas transformaciones a la data antes de asignar contratos de reaseguro y realizar los calculos
    df = pre_procesamiento(tipo_calculo)
    # ! Solicitado por el área de productos, anonimizamos el campo RUT
    df = identificador_anonimo(df, ['RUT'])
    #  ! Solicitado por el área de productos, eliminamos los productos de Hospitalario 100% y productos fallecimiento COVID'
    print(f'Se eliminarán {sum(df["PRODUCTO"].isin([88,101,193,369,370,371,372]))} registros que pertenecen a los productos de Hospitalario 100% y productos fallecimiento COVID')
    df = df[~df['PRODUCTO'].isin([88,101,193,369,370,371,372])].copy()
    # Campo para luego contar la cantidad de registros
    df['REGISTROS']=1
    # Identificamos los registros que poseen recargos de sobreprima o extraprima
    df = recargos(df,calcula_recargos=0)
    
    # * Calculos de asignacion de contratos de reaseguro
    # Asignamos contrato de reaseguro a los registros
    df = asignacion_contratos(df, contrato_cob, mantiene_na = 1)
    # Asignamos vigencia del contrato a la que pertenecen los registros
    df,df_deleted_vigencia = asignacion_vigencias(df,parametros_contratos,tipo_calculo, mantiene_na = 1)

    # * Calculos de cumulos asociados a los contratos
    # Cumulo sobre el monto asegurado que proviene de cada persona individualmente
    df = cumulos(df, 'RIESGO LIMITE INDIVIDUAL')
    # Cumulo sobre el monto asegurado que proviene del contrato en su conjunto
    df = cumulos(df, 'RIESGO LIMITE CONTRATO')
    # Cumulo sobre el monto asegurado que aplica sobre contratos de excedente
    df = cumulos(df, 'RIESGO RETENCION EXCEDENTE')


    # * Calculos de capitales cedidos y retenidos
    df['CAPITAL CEDIDO POST EXCEDENTE'] = df['CAPITAL POST LIMITE CONTRATO'] * (1-df['PORCENTAJE RETENCION EXCEDENTE'])
    df['CAPITAL RETENIDO POST QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] * (1-np.where(df['CESION QS'].isnull(),0,df['CESION QS']))
    df['CAPITAL CEDIDO QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] - df['CAPITAL RETENIDO POST QS']
    df['CAPITAL RETENIDO TOTAL'] = df['MONTO ASEGURADO']-(df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS'])
    df['CAPITAL CEDIDO TOTAL'] = df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS']
    df['PORCENTAJE CEDIDO FINAL']=np.where(df['MONTO ASEGURADO']>0,df['CAPITAL CEDIDO TOTAL']/df['MONTO ASEGURADO'],df['CESION QS']*df['PORCENTAJE LIMITE INDIVIDUAL']*df['PORCENTAJE LIMITE CONTRATO']*df['PORCENTAJE RETENCION EXCEDENTE'])
    
    #  ! Solicitado por el área de productos
    # asignamos campos RAMO_REAS y COB_REAS 
    if contrato=='Desgravamen No Licitado':
        df = cruce_left(df, ramo_reas_desgnl, ['COBERTURA DEL CONTRATO'], ['COBERTURA DEL CONTRATO'],name='ramo_reas_desgnl', ruta_output = ruta_salidas)
    elif contrato in ['Digital Klare','K-Fijo','AP + Urgencias Medicas','Multisocios']:
        df = cruce_left(df, ramo_reas_otros, ['POL_PROD','CODIGO COBERTURA'], ['POL_PROD','CODIGO COBERTURA'],name='ramo_reas_otros', ruta_output = ruta_salidas)
    # Asignamos nombre de producto
    df = cruce_left(df, nombre_prods, ['PRODUCTO','BASE'], ['PRODUCTO','BASE'],name='nombre_prods', ruta_output = ruta_salidas)
    # Contrato K-Fijo no posee estos campos, así que los creamos para que la base sea uniforme para todos los contratos
    if contrato == 'K-Fijo':
        df['MESES RENTA'] = 1
        df['TIPO ASEGURADO'] = 'Titular'
    # Campos seleccionados tanto para la salida de uso interno como para la salida que se entregará a los reaseguradores        
    campos_productos = ['FECHA_CIERRE','BASE','IDENTIFICADOR','RUT','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','NOMBRE_PRODUCTO','PLAN','CODIGO_COBERTURA','CODIGO_COBERTURA_IAXIS','CONTRATO_REASEGURO','COBERTURA_DEL_CONTRATO','RAMO_REAS','COB_REAS','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FEC_NAC','EDAD','SEXO','TIPO_POLIZA','FORMA_PAGO_CODIGO','MESES_RENTA','INNOMINADA','PRIMA_NETA_ANUAL','ICAPITAL','MONTO_ASEGURADO','CAPITAL_RETENIDO_TOTAL','CAPITAL_CEDIDO_TOTAL','RECARGO']
    campos_renovacion = ['FECHA_CIERRE','BASE','IDENTIFICADOR','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO_COBERTURA','RAMO_REAS','COB_REAS','TIPO_POLIZA','FECHA_EFECTO','FECHA_VENCIMIENTO','FEC_NAC','EDAD','SEXO','MONTO_ASEGURADO'] 

    # * Reporteria
    # pequeño script para que todos nuestros campos tengan guin bajo en vez de espacio
    cols_new = []
    for col in df.columns:
        cols_new.append(col.replace(' ','_'))
    df.columns = cols_new    
    # Exportamos la salida de uso interno y la salida para los reaseguradores
    df[df['CONTRATO_REASEGURO'].notnull()][campos_productos].to_csv(ruta_salidas+f'Detalle Renovacion {contrato} Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    df[df['CONTRATO_REASEGURO'].notnull()][campos_renovacion].to_csv(ruta_salidas+f'Detalle Renovacion {contrato} Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    
    # respaldar_proceso(nombre_archivo, ruta_salidas, elimina_origen=1)
    
if __name__=='__main__':
    ruta_salidas='2 Output\\Resultados 2024-12-20\\'
    Path(ruta_salidas).mkdir(parents=True, exist_ok=True)
    calculos_Renovacion(ruta_salidas)