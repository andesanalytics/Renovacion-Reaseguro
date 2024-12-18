"""
FUNCIONES DE REASEGURO
"""
    

# Importamos librerias que vamos a utilizar
import pandas as pd
import numpy as np
import time
import datetime
import calendar
import math 
import cx_Oracle
from pathlib import Path
import os
from zipfile import ZipFile, ZIP_DEFLATED
import shutil
import sys
from itertools import chain, combinations
from pandas.tseries.offsets import MonthEnd
from dateutil.relativedelta import relativedelta
from S0_Inputs import archivo_querys, archivo_calculos, archivo_parametros, ruta_extensa
from S1_Parametros_Calculo import fecha_cierre, tipo_contrato, tipo_calculo, contrato, ruta_input, ruta_output, periodo, fecha_inicio_mes, dias_exposicion, archivo_compara, separador_input, decimal_input, separador_output, decimal_output, periodo_historico, ruta_historico_output, fecha_cierre_mes_anterior, archivo_reporte, archivo_input, archivo_input_ges, campo_rut_duplicados, tipo_prima, uso_fecha_anulacion_historico, tipo_proceso, ruta_historico_input, ruta_pyme, ruta_recargos, ruta_regiones, ruta_reservas, ruta_si, clasificacion_contrato, periodo_anterior, subcarpeta_compara, separador_compara, decimal_compara, pivotea_df, edad_casos_perdidos, ruta_lob

# Prueba de Ejecucion del codigo
print(f'El script {__name__} se está ejecutando')


# Importamos tablas de parametrizaciones
cumulos_individuales=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Individual')
cumulos_contrato=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Contrato')
cumulos_excedente=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Excedente')
cumulos_individuales_siniestros=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Individual Sinies')
cumulos_excedente_siniestros=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Excedente Sinies')
contrato_cob = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
parametros_contratos = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Vigencias')
cesion_reaseguradores = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Reaseguradores')
ocurrencias=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Ocurrencias')
parametros_cesantia=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Parametros Cesantia')
comisiones=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Comisiones')
iva_coberturas=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='IVA')
quitar_reaseguro=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Quitar Reaseguro')
tabla_uf=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Tabla UF')
coc_conceptos=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='CoC Conceptos')
coc_institucion=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='CoC Institucion')
coc_reaseguradores=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='CoC Reaseguradores')
coc_contratos=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='CoC Contratos')
fecu_vida=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Ramo FECU CoC Vida')
nombre_prods=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Nombre Productos Licitacion')
lob_ges=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='CoC LOB GES')
nombres_cardiff=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Nombres Cardiff')
planes_cardiff=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Planes Cardiff')
fecu_desc=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='FECU Descripcion')
negocio=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Negocio')
tipos_pago=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Tipos Pago')
lob_primas=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='LOB Generales')
cobs_ges=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Coberturas GES')
contratos_cesantia=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Contratos Cesantia')
periodos_bdx_cesantia=pd.read_excel(io=ruta_extensa+archivo_calculos,sheet_name='BDXs Cesantia')
cumulos_is=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Cumulos I&S')


""" Diccionario para campos de cumulos
Entregamos variables claves relacionadas con el tipo de cumulo que hacemos:
0:= Nombre del campo de cumulo total
1:= Nombre del campo de porcentaje
2:= Nombre del campo de la retencion o limite
3:= Tambla de retenciones o limites
4:= Sobre que campo de montos asegurados hacemos los cumulos
5:= Nombre del campo resultante de montos asegurados despues de aplicar el limite o retencion
6:= Nombre del campo de cumulo de pagados (solo aplica para siniestros)
"""
diccionario_cumulos={\
'RIESGO LIMITE INDIVIDUAL':['CUMULO LIMITE INDIVIDUAL','PORCENTAJE LIMITE INDIVIDUAL','LIMITE INDIVIDUAL',cumulos_individuales,'MONTO ASEGURADO','CAPITAL POST LIMITE INDIVIDUAL','CUMULO PAGADOS LIMITE INDIVIDUAL'],\
'RIESGO LIMITE CONTRATO':['CUMULO LIMITE CONTRATO','PORCENTAJE LIMITE CONTRATO','LIMITE CONTRATO',cumulos_contrato,'CAPITAL POST LIMITE INDIVIDUAL','CAPITAL POST LIMITE CONTRATO','CUMULO PAGADOS LIMITE CONTRATO'],\
'RIESGO RETENCION EXCEDENTE':['CUMULO RETENCION EXCEDENTE','PORCENTAJE RETENCION EXCEDENTE','RETENCION EXCEDENTE',cumulos_excedente,'CAPITAL POST LIMITE CONTRATO','CAPITAL RETENIDO POST EXCEDENTE','CUMULO PAGADOS RETENCION EXCEDENTE'],\
'RIESGO LIMITE INDIVIDUAL SINIESTROS':['CUMULO LIMITE INDIVIDUAL','PORCENTAJE LIMITE INDIVIDUAL','LIMITE INDIVIDUAL',cumulos_individuales_siniestros,'MONTO SINIESTRO UF','MONTO SINIESTRO RETENIDO POST LIM INDIVIDUAL','CUMULO PAGADOS LIMITE INDIVIDUAL'],\
'RIESGO RETENCION EXCEDENTE SINIESTROS':['CUMULO RETENCION EXCEDENTE','PORCENTAJE RETENCION EXCEDENTE','RETENCION EXCEDENTE',cumulos_excedente_siniestros,'MONTO SINIESTRO RETENIDO POST LIM INDIVIDUAL','MONTO SINIESTRO RETENIDO POST EXCEDENTE','CUMULO PAGADOS RETENCION EXCEDENTE']}

# Diccionario de Tramos de Edades
diccionario_tramos_edades={'Familia Protegida':[0,18,51,66,121],'Full Oncologico':[0,51,66,121],'Oncologico UC':[0,50,60,70,80,121],'Complementario UC':[0,18,50,60,70,80,99]}
# Diccionario de Tramos de Capital
diccionario_tramos_capital={'K-Fijo':[0,120,250,400,1000,9999999]}
# Diccionario de Tramos de Plazo
diccionario_tramos_plazo={'K-Fijo':[0,25,49,61,73,97,1000]}


def escribe_reporta(reporte,texto):
    reporte.write(texto)
    reporte.write('\n')
    print(texto)


def get_all_subsets(s):
    """
    Generate all subsets of a set 's'.
        Parameters:
        s (set): The input set for which subsets need to be generated.
         Returns:
        list: A list of sets representing all subsets of 's'.
    """
    # Convert the input set into a list to ensure order of elements in subsets.
    s_list = list(s)
    # Generate all possible combinations of elements in the list.
    all_combinations = chain.from_iterable(combinations(s_list, r) for r in range(len(s_list) + 1))
    # Convert each combination into a set to obtain subsets.
    all_subsets = [set(comb) for comb in all_combinations]
    return all_subsets


def filtra_una_combinacion(df,lista_campos,tabla_parametros,combinacion,cols_cruce):
    """ Funcion que filtra un dataframe de acuerdo a las caracteristicas de 1 tipo_calculo de reaseguro especificado
        Ademas, tiene la funcion de poder hacer un merge_asof cuando el tipo_calculo asignado para un producto cambia en el tiempo"""
    df_filtrado=df.copy()
    df_filtrado['INDICE']=df_filtrado.index
    tabla_parametros_filtrada=tabla_parametros.copy()
    combinacion=list(combinacion)
    combi_out=list(set(lista_campos).difference(combinacion))
    tabla_parametros_filtrada.dropna(subset=combinacion,inplace=True)
    tabla_parametros_quitar=tabla_parametros_filtrada.dropna(subset=combi_out,how='all')
    tabla_parametros_filtrada=tabla_parametros_filtrada.loc[tabla_parametros_filtrada.index.difference(tabla_parametros_quitar.index)].reset_index(drop=True)
    # for col in combi_out:
    #     tabla_parametros_filtrada=tabla_parametros_filtrada[tabla_parametros_filtrada[col].isnull()]
    if tabla_parametros_filtrada.empty: return pd.DataFrame(),df,tabla_parametros
    else:
        tpf_sin_inicio=tabla_parametros_filtrada[tabla_parametros_filtrada['INICIO DEL CONTRATO'].isnull()].copy()
        tpf_con_inicio=tabla_parametros_filtrada[~tabla_parametros_filtrada['INICIO DEL CONTRATO'].isnull()].copy()
        # Pregunta si necesitamos hacer un merge o merge_asof, en caso de tener cambio de contratos en el tiempo
        if not tpf_sin_inicio.empty: df_filtrado_sin_inicio=df_filtrado.merge(tpf_sin_inicio[combinacion+cols_cruce],how='inner',on=combinacion)
        else: df_filtrado_sin_inicio=pd.DataFrame()
        if not tpf_con_inicio.empty:
            #################### ESTA PARTE DEBEMOS "DESGRANAR" CUALES SON LOS CAMPOS CON INICIO DE CONTRATO NO NULO Y CRUZARLOS CON EL MERGE.ASOF
            ######### debemos quitar duplicados de los campos de la combinacion e ir iterando por cada uno de ellos haciendo merge asof por separado
            tpf_con_inicio_unicos=tpf_con_inicio[combinacion].drop_duplicates()
            df_filtrado_con_inicio=pd.DataFrame()
            # Recorre el dataframe para cada registro de filtro que deba hacer, para ir concatenandolos en el df que necesitamos
            for index, row in tpf_con_inicio_unicos.iterrows():
                # lista_valores=[353.0, 12.0]
                lista_valores=list(row)
                df_filtrado_con_inicio_aux=df_filtrado.copy()
                tpf_con_inicio_filtrada=tpf_con_inicio.copy()
                for col,valor in zip(combinacion,lista_valores):
                    df_filtrado_con_inicio_aux=df_filtrado_con_inicio_aux[df_filtrado_con_inicio_aux[col]==valor]
                    tpf_con_inicio_filtrada=tpf_con_inicio_filtrada[tpf_con_inicio_filtrada[col]==valor]
                df_filtrado_con_inicio_aux=pd.merge_asof(df_filtrado_con_inicio_aux.sort_values('FECHA CRUCE VIGENCIAS'),tpf_con_inicio_filtrada[cols_cruce].sort_values('INICIO DEL CONTRATO'),left_on='FECHA CRUCE VIGENCIAS',right_on='INICIO DEL CONTRATO')
                df_filtrado_con_inicio_aux=df_filtrado_con_inicio_aux.dropna(subset=cols_cruce)
                df_filtrado_con_inicio=pd.concat([df_filtrado_con_inicio,df_filtrado_con_inicio_aux])
        else: df_filtrado_con_inicio=pd.DataFrame()
        df_filtrado=pd.concat([df_filtrado_con_inicio,df_filtrado_sin_inicio])
        # Resta el
        df_a_filtrar=df.loc[df.index.difference(df_filtrado['INDICE'])]
        df_filtrado=df_filtrado.drop(columns=['INDICE'])
        tabla_parametros_a_filtrar=tabla_parametros
        return df_filtrado,df_a_filtrar,tabla_parametros_a_filtrar


def asignacion_contratos(df,tabla_parametros,mantiene_na=0):
    """ Asignacion de todos los contratos de reaseguro, de acuerdo al tipo_calculo seleccionado
        Utiliza la funcion anterior tantas veces como sea necesario hasta filtrar todo lo que el tipo_calculo tiene"""
    # Definiciones preliminares del proceso
    original_rows=df.shape[0]
    # Solo tomo los registros asociados al proceso que estoy corriendo. Mayor info en el diccionario de contratos
    if tipo_calculo == 'Prima de Reaseguro': tabla_parametros=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato]
    cols_cruce=['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','INICIO DEL CONTRATO']
    lista_campos=list(set(list(tabla_parametros.columns)).difference(cols_cruce))
    lista_combinaciones=get_all_subsets(lista_campos)
    lista_combinaciones.remove(set())
    df_final=pd.DataFrame()
    escribe_reporta(archivo_reporte,'COMIENZA LA ASIGNACION DE CONTRATOS DE REASEGURO:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # Asigna la fecha que debemos tener en consideracion para asignar tipo_calculo
    # Si el tipo_calculo es por suscripcion, se toma la fecha de inicio vigencia
    # Si el tipo_calculo es por ocurrencia, se toma la fecha de cierre
    df=df.merge(ocurrencias,how='left',on=['POLIZA'])
    if pd.concat([df_final,df]).shape[0]>original_rows: escribe_reporta(archivo_reporte, 'El dataframe posterior a la tabla de ocurrencias tiene más registros. REVISAR!')
    df['PERIODO DEL CONTRATO'] = df['PERIODO DEL CONTRATO'].fillna('Ocurrencia')
    if tipo_calculo == 'Siniestros de Reaseguro': df['FECHA CRUCE VIGENCIAS']=np.where(df['PERIODO DEL CONTRATO']=='Ocurrencia',df['FECHA_SINIESTRO'],df['INICIO_VIGENCIA'])
    elif tipo_prima=='Prima Unica': df['FECHA CRUCE VIGENCIAS']=df['FECHA_EFECTO'] 
    else : df['FECHA CRUCE VIGENCIAS']=df['FECHA CIERRE']
    # Recorre el dataframe para cada registro de filtro que deba hacer, para ir concatenandolos en el df que necesitamos
    for combinacion in lista_combinaciones:
        if all(x in df.columns for x in combinacion):        
            df_filtrado,df,tabla_parametros=filtra_una_combinacion(df,lista_campos,tabla_parametros,combinacion,cols_cruce)
            df_final=pd.concat([df_final,df_filtrado],axis=0)
    if list(set(lista_campos) - set(df.columns)) != []: escribe_reporta(archivo_reporte, f'Las siguientes columnas no se encuentran en el df para poder asignar contratos\n{list(set(lista_campos) - set(df.columns))}\n')
    if pd.concat([df_final,df]).shape[0]>original_rows: escribe_reporta(archivo_reporte, 'El dataframe posterior a la asignación de contratos tiene más registros. REVISAR!')
    if mantiene_na==1: return pd.concat([df_final,df],axis=0)
    else: return df_final


def asignacion_vigencias(df,tabla_parametros,tipo_calculo,mantiene_na=0):
    """ Para siniestros, asigna a la vigencia a la cual pertenece 
        La asignacion puede ser por tipo_calculo, o por tipo_calculo y cobertura, dandole mayor flexibilidad a la forma de cruzar data
        Lo que tambien lo hace mas eficiente"""
    escribe_reporta(archivo_reporte,'COMIENZA LA ASIGNACION DE VIGENCIAS DE LOS CONTRATOS DE REASEGURO:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    cols_iniciales=list(df.columns)
    df_nuls=df[df['CONTRATO REASEGURO'].isnull()].copy()
    df_final=pd.DataFrame()
    
    if tipo_calculo == 'Prima de Reaseguro':
        tabla_parametros=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato]
        cobs_reaseguro=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato]['COBERTURA DEL CONTRATO'].unique()
        # Ciclo que recorre cada uno de los elementos de la losta para ir filtrando y asignado vigencias
        for cobertura_reaseg in cobs_reaseguro:
            df_filtrado=df[df['COBERTURA DEL CONTRATO']==cobertura_reaseg]
            tabla_filtrada=tabla_parametros[tabla_parametros['COBERTURA DEL CONTRATO']==cobertura_reaseg]
            df_final=pd.concat([df_final,pd.merge_asof(df_filtrado.sort_values('FECHA CRUCE VIGENCIAS'),tabla_filtrada.sort_values('FECHA INICIO CONTRATO').drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'],axis=1),left_on=['FECHA CRUCE VIGENCIAS'],right_on='FECHA INICIO CONTRATO')],axis=0)
    if tipo_calculo == 'Siniestros de Reaseguro':
        contratos_reaseguro = df['CONTRATO REASEGURO'].unique()
        for contrato_reaseg in contratos_reaseguro:
            tabla_parametros_contrato=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato_reaseg]
            cobs_reaseguro=tabla_parametros_contrato[tabla_parametros_contrato['CONTRATO REASEGURO']==contrato_reaseg]['COBERTURA DEL CONTRATO'].unique()
            df_contrato = df[df['CONTRATO REASEGURO']==contrato_reaseg]
            # Ciclo que recorre cada uno de los elementos de la losta para ir filtrando y asignado vigencias
            for cobertura_reaseg in cobs_reaseguro:
                df_filtrado=df_contrato[(df_contrato['COBERTURA DEL CONTRATO']==cobertura_reaseg)]
                tabla_filtrada=tabla_parametros_contrato[tabla_parametros_contrato['COBERTURA DEL CONTRATO']==cobertura_reaseg]
                df_final=pd.concat([df_final,pd.merge_asof(df_filtrado.sort_values('FECHA CRUCE VIGENCIAS'),tabla_filtrada.sort_values('FECHA INICIO CONTRATO').drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'],axis=1),left_on=['FECHA CRUCE VIGENCIAS'],right_on='FECHA INICIO CONTRATO')],axis=0)
    
    # Eliminamos registros con fecha posterior o anterior a los contratos de vigencia establecidos
    cols_finales=list(df_final.columns)
    cols_extra=[x for x in cols_finales if not x in cols_iniciales]
    df_final_01=df_final[~df_final['VIGENCIA CONTRATO'].isnull()].copy()
    df_final_02=df_final_01[df_final_01['FECHA CRUCE VIGENCIAS']<=df_final_01['FECHA FIN CONTRATO']].copy()
    df_deleted=df_final[(df_final['VIGENCIA CONTRATO'].isnull())|(df_final['FECHA CRUCE VIGENCIAS']>df_final['FECHA FIN CONTRATO'])].copy()
    # Revisamos cuantos elementos se eliminaron por temas de fechas
    reg_elim_ant=sum(df_final['VIGENCIA CONTRATO'].isnull())
    reg_elim_post=sum(df_final_01['FECHA CRUCE VIGENCIAS']>df_final_01['FECHA FIN CONTRATO'])
    if reg_elim_ant>0:escribe_reporta(archivo_reporte,f'Se eliminaron {reg_elim_ant} registros cuya fecha es anterior al primer {tipo_calculo} de reaseguro establecido')
    if reg_elim_post>0:escribe_reporta(archivo_reporte,f'Se eliminaron {reg_elim_post} registros cuya fecha es posterior al ultimo {tipo_calculo} de reaseguro establecido')
    # return df_final_02,df_deleted
    if mantiene_na==1: return pd.concat([df_final_02,df_deleted.drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','INICIO DEL CONTRATO']+cols_extra,axis=1),df_nuls],axis=0),df_deleted
    else: return df_final_02,df_deleted


def cumulo_riesgo(df,contrato_reaseguro,riesgo_cumulo,lista_campos,limite_retencion,tipo_cumulo,columna_capital):
    """ Funcion de calculo de cumulo para un tipo_calculo y riesgo particular """
    # Filtro el df por el tipo_calculo y el riesgo de cumulo correspondiente
    df_filter=df[(df['CONTRATO REASEGURO']==contrato_reaseguro) & (df[tipo_cumulo]==riesgo_cumulo)]
    # Si el df filtrado es vacio (pensado en que la funcion cumulos recorre todos los registros de su tabla de cumulos), entonces crea en el df las columnas que se crearán en caso de no ser vacio
    if df_filter.empty:
        #print('dataframe vacio para tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato_reaseguro,riesgo_cumulo))
        return df_filter
    # Hacemos groupby por la lista de campos que entregamos. Tomamos como agregacion la columna de cumulos que indicamos en los parametros
    lista_campos=lista_campos.split(',')
    if sum([0 if x in df.columns else 1 for x in lista_campos])==0:
        df_grouped = df_filter.groupby(lista_campos)[columna_capital].sum().reset_index().rename(columns={columna_capital:'CUMULO'})
        if 'Siniestro' in tipo_calculo: df_grouped_pagados=df_filter[df_filter['ESTADO SINIESTRO']=='PAGADO'].groupby(lista_campos)[columna_capital].sum().reset_index().rename(columns={columna_capital:'CUMULO_PAGADOS'})
    else:
        for campo in lista_campos:
            if campo not in df.columns:
                escribe_reporta(archivo_reporte,'El campo {} para hacer cumulo no se encuentra dentro del dataframe, para tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(campo,contrato_reaseguro,riesgo_cumulo))
    # Calculo de retencion: 
    # Si la retencion no es igual para todos registros, se debe inrgesar el nombre de la tabla que parametriza aquello
    if isinstance(limite_retencion, str):
        # Busca la tabla del nombre que pusimos dentro de la tabla de cumulos
        try:
            tabla_cumulos=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name=limite_retencion)
        except:
            escribe_reporta(archivo_reporte,'la tabla de retenciones especificada no existe para el tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato_reaseguro,riesgo_cumulo))
        # Revisa el nombre de los campos que tiene, y quita el que contiene el limite
        campos=list(tabla_cumulos.columns)
        campos.remove('LIMITE O RETENCION')
        # Cruza con el df de acuerdo al resto de campos dentro de la tabla
        df_grouped=df_grouped.merge(tabla_cumulos,how='inner', left_on=campos, right_on=campos,suffixes=['','_x'])
    else:
    # Si la retencion es igual para todos los registros, rellena con ese valor dentro del df agrupado
        df_grouped['LIMITE O RETENCION']=limite_retencion
    # Crea la columna porcentaje
    if 'Siniestro' not in tipo_calculo:
        df_grouped['PORCENTAJE'] = np.where(df_grouped['CUMULO'] == 0, 0, np.minimum( 1, df_grouped['LIMITE O RETENCION'] / df_grouped['CUMULO']))
    else:
        df_grouped=df_grouped.merge(df_grouped_pagados,how='left',on=lista_campos)
        df_grouped['CUMULO_PAGADOS']=df_grouped['CUMULO_PAGADOS'].fillna(0)
        df_grouped['PORCENTAJE PAGADOS'] = np.where(df_grouped['CUMULO_PAGADOS'] == 0, 0, np.minimum( 1, df_grouped['LIMITE O RETENCION'] / df_grouped['CUMULO_PAGADOS']))
        df_grouped['PORCENTAJE PENDIENTES'] = np.where(df_grouped['CUMULO_PAGADOS'] == df_grouped['CUMULO'], 0, np.minimum( 1, ( df_grouped['LIMITE O RETENCION'] - np.minimum( df_grouped['LIMITE O RETENCION'], df_grouped['CUMULO_PAGADOS'])) / ( df_grouped['CUMULO'] - df_grouped['CUMULO_PAGADOS'])))
    # Finalmente, cruza el df original y filtrado con el df agrupado
    df_final=df_filter.merge(df_grouped,how='inner', left_on=lista_campos, right_on=lista_campos,suffixes=['','_x'])
    if df_final.shape[0] > df_filter.shape[0]:
        escribe_reporta(archivo_reporte,'la tabla agrupada con los limites y retenciones cruzó más registros que el df original, para el tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato_reaseguro,riesgo_cumulo))
    elif df_final.shape[0] < df_filter.shape[0]:
        escribe_reporta(archivo_reporte,'la tabla agrupada con los limites y retenciones cruzó menos registros que el df original, para el tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato_reaseguro,riesgo_cumulo))
    # Columna que calcula el capital posterior a establecer limite o retencion, segun corresponda
    if 'Siniestro' in tipo_calculo: df_final['PORCENTAJE']=np.where(df_final['ESTADO SINIESTRO']=='PAGADO',df_final['PORCENTAJE PAGADOS'],df_final['PORCENTAJE PENDIENTES'])
    df_final['CAPITAL POSTERIOR']=df_final['PORCENTAJE']*df_final[columna_capital]
    if 'Siniestro' in tipo_calculo: df_final.drop(columns=['PORCENTAJE PAGADOS', 'PORCENTAJE PENDIENTES'],inplace=True)
    return df_final


def cumulos(df,campo_cumulo):
    """ 
    Funcion de calculo de cumulo para todos los riesgos y contratos dentro del df
    En campo_cumulo tenemos las siguientes alternativas (ver diccionario de cumulos)
        # RIESGO LIMITE INDIVIDUAL
        # RIESGO LIMITE CONTRATO
        # RIESGO CONTRATOS EXCEDENTES
        # RIESGO LIMITE INDIVIDUAL SINIESTROS
        # RIESGO RETENCION EXCEDENTE SINIESTROS
    """
    # Va a buscar la tabla de cumulos, de acuerdo al tipo de cumulo que hayamos solicitado realizar
    escribe_reporta(archivo_reporte,'Comienza proceso de calculo de cumulos del tipo {}:\n{}'.format(campo_cumulo,time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    tabla_cumulos=diccionario_cumulos[campo_cumulo][3]
    # Check si los campo_cumulo y de monto_cumulo estan dentro del df
    for campo in [campo_cumulo,diccionario_cumulos[campo_cumulo][4]]:
        if campo not in df.columns:
            escribe_reporta(archivo_reporte,'El campo {} no se encuentra dentro del dataframe'.format(campo))
    # Se parte por los registros que no tengan que ser acumulados
    # Se llama df_inicial a este df
    df_inicial=df[df[campo_cumulo].isnull()].copy()
    # Se crean campos que posteriormente se crearán, con sus valores adecuados
    df_inicial['CUMULO']=df_inicial[diccionario_cumulos[campo_cumulo][4]]
    df_inicial['LIMITE O RETENCION']=np.nan
    if 'Siniestro' in tipo_calculo: df_inicial['CUMULO_PAGADOS']=np.where(df_inicial['ESTADO SINIESTRO']=='PAGADO',df_inicial[diccionario_cumulos[campo_cumulo][4]],0)
    df_inicial['PORCENTAJE']=1
    df_inicial['CAPITAL POSTERIOR']=df_inicial[diccionario_cumulos[campo_cumulo][4]]
    # Ahora, agregaremos al df_inicial la iteración de todos los cumulos que existan según la parametrizacion de la tabla de cumulos
    for index, row in tabla_cumulos.iterrows():
        # Aplicamos la funcion de cumulo_riesgo para cada tipo_calculo-cumulo que exista en la tabla de cumulos
        # print(row['CONTRATO REASEGURO']+'-'+row[campo_cumulo])
        df_agregar=cumulo_riesgo(df,row['CONTRATO REASEGURO'],row[campo_cumulo],row['CAMPOS A ACUMULAR'],row['LIMITE O RETENCION'],campo_cumulo,diccionario_cumulos[campo_cumulo][4])
        # Concatenamos con el df inicial
        df_inicial=pd.concat([df_inicial,df_agregar],axis=0)
    # Renombramos las variables, para que tengan un significado asociado al tipo de cumulo aplicado (ver diccionario de cumulos)
    df_inicial.rename(columns={'CUMULO': diccionario_cumulos[campo_cumulo][0],'PORCENTAJE': diccionario_cumulos[campo_cumulo][1],'LIMITE O RETENCION': diccionario_cumulos[campo_cumulo][2],'CAPITAL POSTERIOR': diccionario_cumulos[campo_cumulo][5],'CUMULO_PAGADOS': diccionario_cumulos[campo_cumulo][6]}, inplace=True)
    return df_inicial


def busca_tasas(df,nombre_tabla):
    """
    Funcion que dado un df y el nombre de una tabla, hace el cruce entre el df y la tabla, por los campos que contenga la tabla
    # Para esto, los campos que estén dentro de la tabla tambien deben estar, con el mismo nombre, en el df
    """
    # Busca la tabla de tasas/primas de reaseguro
    try:
        tabla=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name=nombre_tabla)    
    except:
        escribe_reporta(archivo_reporte,'la tabla {} de tasas/primas de reaseguro no existe'.format(nombre_tabla))
    # Extrae los campos que posee, quitando el nombre del campo que contiene la tasa/prima de reaseguro
    campos=list(tabla.columns)
    campos.remove('TASA O PRIMA DE REASEGURO')
    # Filtra el df por solo aquellos que debe aplicar aquella tabla de tasas/primas
    df_filtrado=df[df['TABLA']==nombre_tabla].copy()
    if df_filtrado.empty:
        escribe_reporta(archivo_reporte,'No hay registros con la tabla {} dentro del dataframe'.format(nombre_tabla))
    # Cruza el df filtrado vs la tabla encontrada, por todos los campos que la tabla posea
    if sum([1 if x not in df.columns else 0 for x in campos])==0:
        df_final=df_filtrado.merge(tabla,how='inner', left_on=campos, right_on=campos,suffixes=['','_x'])    
    else:
        for campo in campos:
            if campo not in df.columns:
                escribe_reporta(archivo_reporte,'El campo {} no se encuentra dentro del dataframe, por lo que no se puede cruzar la tabla {}'.format(campo,nombre_tabla))
    return df_final


def calculo_tasas_reaseguro(df):
    """ 
    Funcion para encontrar la tasa o prima de reaseguro asociada a cada registro dentro del df
    Para ello aplica la funcion "busca_tasas" para todos los valores de tablas que encuentre dentro del df
    """
    escribe_reporta(archivo_reporte,'COMIENZA LA ASIGNACION DE TASAS DE REASEGURO:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # Extrae todos los nombres de tablas de tasas/primas dentro del df
    lista=[x for x in list(df['TABLA'].unique()) if str(x)!='nan']
    # Genera primero el df de aquellos registros donde el nombre de la tabla no sea necesario, estos son los casos donde la tasa/prima sea solo una tasa anual x 1000
    df_inicial=df[df['TABLA'].isnull()].copy()
    df_inicial['TASA O PRIMA DE REASEGURO']=df_inicial['TASA']
    for name in lista:
        # Para cada nombre de tabla encontrada, aplica la funcion busca_tasas
        df_tabla=busca_tasas(df,name)
        # Luego concatena con el df_inicial creado anteriormente
        df_inicial=pd.concat([df_inicial,df_tabla],axis=0)
    return df_inicial


def calcula_tramos(df,diccionario,campo_cruce,campo_tramo,aux=1):
    if campo_cruce not in df.columns: return df
    lista_tramos=list(diccionario)
    df['APLICA TRAMOS '+campo_tramo]=np.where(df['CONTRATO REASEGURO'].isin(lista_tramos),1,0)
    df_inicial=df[df['APLICA TRAMOS '+campo_tramo]==0].copy()
    if not df_inicial.empty: df_inicial['TRAMO '+campo_tramo]='NA'
    for contrato_reaseguro in lista_tramos:
        df_filtrado=df[df['CONTRATO REASEGURO']==contrato_reaseguro].copy()
        bins=diccionario[contrato_reaseguro]
        labels=[str(bins[i])+'-'+str(bins[i+1]-aux) for i in range(len(bins)-1)]
        df_filtrado['TRAMO '+campo_tramo] = pd.cut(df_filtrado[campo_cruce], bins=bins, labels=labels, right=False)
        df_inicial=pd.concat([df_inicial,df_filtrado],axis=0) if not df_inicial.empty else df_filtrado
    df.drop('APLICA TRAMOS '+campo_tramo,axis=1,inplace=True)
    return df_inicial


def cruce_dfs(df_cierre,df_hist,campo_cruce,desc_llave='',campos_extra=''):
    # CALCULOS PREVIOS
    if desc_llave=='': llave=campo_cruce
    else: llave=campo_cruce+' - '+desc_llave
    if campos_extra=='':campos_cruzar_hist=[campo_cruce,'FECHA_ANULACION','CRUCE_HIST','SSEGURO']
    else: campos_cruzar_hist=[campo_cruce,'FECHA_ANULACION','CRUCE_HIST','SSEGURO']+campos_extra
    condicion=all(x in list(df_hist.columns) for x in campos_extra)
    # CREO COPIA DE LOS DF
    print('Corriendo cruce de df vs historico bdx para el campo {}'.format(llave))
    df_cierre_aux=df_cierre.copy()
    df_hist_aux=df_hist.copy()
    df_cierre_aux['CRUCE_HIST']=1
    df_hist_aux['CRUCE_CIERRE']=1
    # SETEO VARIABLES DE NUEVA VENTA Y NUEVO VENCIMIENTO EN CASO DE QUE SEA LA PRIMERA ITERACION
    if 'NUEVA VENTA' not in df_cierre_aux.columns: df_cierre_aux['NUEVA VENTA']=1
    if 'NUEVO VENCIMIENTO' not in df_hist_aux.columns: df_hist_aux['NUEVO VENCIMIENTO']=0
    # APERTURA DE DF HISTORICO ENTRE LOS QUE VENCIERON ANTES Y LOS QUE NO
    df_cierre_aux_nocruzar=df_cierre_aux[(df_cierre_aux['NUEVA VENTA']==0)|(df_cierre_aux[campo_cruce]=='')].copy()
    df_cierre_aux_cruzar=df_cierre_aux[(df_cierre_aux['NUEVA VENTA']==1)&(df_cierre_aux[campo_cruce]!='')].copy()
    df_hist_aux_nocruzar=df_hist_aux[(df_hist_aux['CRUCE CIERRE']==1)|(df_hist_aux[campo_cruce]=='')].copy()
    df_hist_aux_cruzar=df_hist_aux[(df_hist_aux['CRUCE CIERRE']==0)&(df_hist_aux[campo_cruce]!='')].copy()
    if (condicion)&(campos_extra!=''):df_hist_aux_cruzar.drop(columns=campos_extra+['SSEGURO_NUEVA'],axis=1,inplace=True)
    # PRINT DE CARACTERISTICAS DE LOS DF A CRUZAR
    print('El df de cierre a cruzar contiene {} registros con la {}'.format(df_cierre_aux_cruzar.shape[0],campo_cruce))
    print('El historico a cruzar contiene {} registros con la {}'.format(df_hist_aux_cruzar.shape[0],campo_cruce))
    # REVISO LOS DUPLICADOS
    duplicados_df_cierre=df_cierre_aux_cruzar.loc[df_cierre_aux_cruzar.duplicated(subset=[campo_cruce],keep=False)]
    duplicados_df_hist=df_hist_aux_cruzar.loc[df_hist_aux_cruzar.duplicated(subset=[campo_cruce],keep=False)]
    duplicados_df_cierre=duplicados_df_cierre[duplicados_df_cierre[campo_cruce].isin(list(df_hist_aux_cruzar[campo_cruce].unique()))]
    duplicados_df_hist=duplicados_df_hist[duplicados_df_hist[campo_cruce].isin(list(df_cierre_aux_cruzar[campo_cruce].unique()))]
    if not (duplicados_df_cierre.empty): duplicados_df_cierre.to_csv(ruta_historico_output+'\\0. Duplicados df '+campo_cruce+'.txt',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    if not (duplicados_df_hist.empty): duplicados_df_hist.to_csv(ruta_historico_output+'\\0. Duplicados Historico '+campo_cruce+'.txt',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # REALIZO LOS MERGE DE AMBOS LADOS
    df_cierre_aux_cruzado=df_cierre_aux_cruzar.merge(df_hist_aux_cruzar[[campo_cruce,'CRUCE_CIERRE']],how='left',on=campo_cruce)
    df_hist_aux_cruzado=df_hist_aux_cruzar.merge(df_cierre_aux_cruzar[campos_cruzar_hist],how='left',on=campo_cruce,suffixes=['','_NUEVA'])
    # RE-DEFINO LAS VARIABLES NUEVO VENCIMIENTO Y NUEVA VENTA CUANDO CORRESPONDA
    df_cierre_aux_cruzado['NUEVA VENTA']=np.where(~df_cierre_aux_cruzado['CRUCE_CIERRE'].isnull(),0,df_cierre_aux_cruzado['NUEVA VENTA'])
    df_hist_aux_cruzado['CRUCE CIERRE']=np.where(~df_hist_aux_cruzado['CRUCE_HIST'].isnull(),1,df_hist_aux_cruzado['CRUCE CIERRE'])
    df_hist_aux_cruzado['NUEVO VENCIMIENTO']=np.where((df_hist_aux_cruzado['FECHA_ANULACION'].isnull())&(~df_hist_aux_cruzado['FECHA_ANULACION_NUEVA'].isnull())&(df_hist_aux_cruzado['FECHA_ANULACION_NUEVA']<df_hist_aux_cruzado['FECHA_VENCIMIENTO']),1,df_hist_aux_cruzado['NUEVO VENCIMIENTO'])
    if uso_fecha_anulacion_historico==1: df_hist_aux_cruzado['FECHA_ANULACION']=df_hist_aux_cruzado['FECHA_ANULACION_NUEVA']
    else: df_hist_aux_cruzado['FECHA_ANULACION']=np.where(df_hist_aux_cruzado['NUEVO VENCIMIENTO']==1,df_hist_aux_cruzado['FECHA_ANULACION_NUEVA'],df_hist_aux_cruzado['FECHA_ANULACION'])
    # PRINT DE CUANTOS REGISTROS SE CRUZARON
    print('En el df de cierre se cruzaron {} registros'.format(sum(~df_cierre_aux_cruzado['CRUCE_CIERRE'].isnull())))
    print('En el df historico se cruzaron {} registros'.format(sum(~df_hist_aux_cruzado['CRUCE_HIST'].isnull())))
    # JUNTO LOS DF HISTORICOS
    df_hist_aux=pd.concat([df_hist_aux_cruzado,df_hist_aux_nocruzar],axis=0)
    df_cierre_aux=pd.concat([df_cierre_aux_cruzado,df_cierre_aux_nocruzar],axis=0)
    if df_hist_aux.shape[0]>df_hist.shape[0]: escribe_reporta(archivo_reporte,'Cruce de Dataframe Historico hizo un doble cruce {} veces para la llave {}'.format(df_hist_aux.shape[0]-df_hist.shape[0],campo_cruce))
    if df_cierre_aux.shape[0]>df_cierre.shape[0]: escribe_reporta(archivo_reporte,'Cruce de Dataframe de Cierre hizo un doble cruce {} veces para la llave {}'.format(df_cierre_aux.shape[0]-df_cierre.shape[0],campo_cruce))
    # DEFINIMOS LOS NUEVOS DF DE CIERRE E HISTORICO
    df_cierre_aux=df_cierre_aux[list(df_cierre.columns)].copy()
    if condicion: df_hist_aux=df_hist_aux[list(df_hist.columns)].copy()
    else: df_hist_aux=df_hist_aux[list(df_hist.columns)+campos_extra+['SSEGURO_NUEVA']].copy()
    return df_cierre_aux,df_hist_aux


def calcula_devoluciones(vencimientos,campo_devolver='PRIMA REASEGURO'):
    venc_aux=vencimientos.copy()
    venc_aux['PLAZO DIAS']=np.maximum((venc_aux['FECHA_VENCIMIENTO']-venc_aux['FECHA_EFECTO']).dt.days-venc_aux['CARENCIA'],1)
    venc_aux['DIAS CONSUMIDOS']=np.minimum(np.maximum((venc_aux['FECHA_ANULACION']-venc_aux['FECHA_EFECTO']).dt.days-venc_aux['CARENCIA'],0),venc_aux['PLAZO DIAS'])
    venc_aux['FACTOR DEVOLUCION']=np.minimum((venc_aux['PLAZO DIAS']-venc_aux['DIAS CONSUMIDOS'])/venc_aux['PLAZO DIAS'],1)
    venc_aux[campo_devolver+' A DEVOLVER']=venc_aux[campo_devolver]*venc_aux['FACTOR DEVOLUCION']
    # return venc_aux[list(vencimientos.columns)+[campo_devolver+' A DEVOLVER']]
    return venc_aux


def historico_quita_registros():
    cols_date = ['FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION']
    dic_types={'ICAPITAL':float}
    historico = pd.read_csv(ruta_historico_input+'1. Inputs Auxiliares\\Historicos\\'+'Historico '+contrato+' '+str(periodo_historico)+'.txt', sep=';',decimal='.', date_format='%d-%m-%Y', parse_dates=cols_date, low_memory=False,dtype=dic_types)
    cols_hist = list(historico.columns)
    # DEFINO LA LLAVE Y FILTRO
    historico['LLAVE']=historico['RUT'].astype('int64').astype('string')+'_'+historico['POLIZA'].astype('int64').astype('string')+'_'+historico['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+historico['FECHA_EFECTO'].astype('string')+'_'+historico['FECHA_VENCIMIENTO'].astype('string')
    historico_corregido=historico[~historico['LLAVE'].isin(['14147585_565_112_2021-03-10_2026-05-07',	'14147585_565_6_2021-03-10_2026-05-07',	'16832861_565_112_2020-11-26_2022-12-05',	'16832861_565_6_2020-11-26_2022-12-05',	'17393478_565_112_2021-08-06_2024-09-04',	'17393478_565_6_2021-08-06_2024-09-04',	'5972764_565_112_2021-02-17_2025-05-15',	'7359700_565_112_2021-09-21_2026-10-02'])]
    historico_eliminado=historico[historico['LLAVE'].isin(['14147585_565_112_2021-03-10_2026-05-07',	'14147585_565_6_2021-03-10_2026-05-07',	'16832861_565_112_2020-11-26_2022-12-05',	'16832861_565_6_2020-11-26_2022-12-05',	'17393478_565_112_2021-08-06_2024-09-04',	'17393478_565_6_2021-08-06_2024-09-04',	'5972764_565_112_2021-02-17_2025-05-15',	'7359700_565_112_2021-09-21_2026-10-02'])]
    # REALIZO EXPORTACION
    historico_corregido[cols_hist].to_csv(ruta_extensa+'Historico Corregido.txt',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
    historico_eliminado[cols_hist].to_csv(ruta_extensa+'Historico Eliminado.txt',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)


def base_cesantia_quita_registros():
    cols_date=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION']
    dateparse_forma2 = lambda x: pd.to_datetime(x, format='%d-%m-%Y',errors='coerce')
    df_iaxis_0_0=pd.read_csv(ruta_input+archivo_input,sep=separador_input,decimal=decimal_input,parse_dates=cols_date,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
    df_iaxis_0_0=df_iaxis_0_0[~df_iaxis_0_0['SSEGURO'].isin([1717494,1717789,1479342,1723646,1724421,1724423,1724539,1725023,1725150,1725791,1727021,1727363,1727862,1487680,1487693,1730782,1489987,1732639,1735083,1735458,1736032,1736052,1736705,1736906,1737967,1739388,1741399,1742163,1745146,1746182,1750354,1750475,1755755,1513231,1758352,1760507,1472115,1708711,1708717,1708799,1709180,1472836,1709763,1710676,1710713,1710987,2152297,1712503,1712758,1714049,1714181,1715214,1715623,1715954,1716560])]
    df_iaxis_0_0.to_csv(archivo_input,sep=separador_input,decimal=decimal_input,date_format='%d-%m-%Y',index=False)


def lista_campos_reaseguradores(cols_reporte_reaseguradores,lista_reaseguradores):
    lista=[]
    for col in cols_reporte_reaseguradores:
        for reasegurador in lista_reaseguradores:
            lista.append(col+'_'+reasegurador)
    return lista


def calculos_prima_unica_bdx(df_bdx,lista_reaseguradores):
    escribe_reporta(archivo_reporte,'{} Calculos de Contratos Prima Unica'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # LEO LA BASE DE HISTORICO
    historico = pd.read_csv(ruta_historico_input+'1. Inputs Auxiliares\\Historicos\\'+'Historico '+contrato+' '+str(periodo_historico)+'.txt', sep=';',decimal='.', date_format='%d-%m-%Y', parse_dates=['FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION'], low_memory=False,dtype={'ICAPITAL':float,'PRIMA REASEGURO':float})
    historico['NUEVO VENCIMIENTO']=0
    cols_reporte=list(set(cols_instancia('PU NV',lista_reaseguradores)+cols_instancia('PU Devoluciones BDX',lista_reaseguradores)+cols_instancia('PU Devoluciones Cierre',lista_reaseguradores)))
    # EXTRAIGO LAS COLUMNAS ORIGINALES DE CADA DF
    cols_hist = list(historico.columns)
    campos_extra=[x for x in list(set(cols_reporte)-set(cols_hist)) if x in df_bdx.columns]
    pd.options.mode.chained_assignment = None
    # HAGO COPIAS
    df_aux=df_bdx.copy()
    historico_aux=historico.copy()
    historico_aux['PERIODO_EFECTO']=historico_aux['FECHA_EFECTO'].dt.year*100+historico_aux['FECHA_EFECTO'].dt.month
    historico_aux['NUEVO VENCIMIENTO']=0
    historico_aux['CRUCE CIERRE']=0
    historico_aux['FUENTE']='HISTORICO'
    df_aux['NUEVO VENCIMIENTO']=1
    df_aux['PERIODO_EFECTO']=df_aux['FECHA_EFECTO'].dt.year*100+df_aux['FECHA_EFECTO'].dt.month
    df_aux['NUEVA VENTA']=1
    df_aux['FUENTE']='NV'
    df_aux['SECUENCIAL']=np.where(df_aux['BASE']=='GES',df_aux['CERTIFICADO'],0)
    # CREO FACTORES DE BDX PARA NV Y ANULACIONES DE ACUERDO A LOS PERIODOS DEL BDX INDICADOS
    if clasificacion_contrato=='Cesantia PU':
        df_aux['PRIMA NETA']=df_aux['PRIMA NETA ANUAL']
        if tipo_proceso=='BDX':
            df_aux['FECHA BDX']=pd.to_datetime(df_aux['PERIODO BDX'], format="%Y%m")+ MonthEnd(0)
            print(df_aux[['CONTRATO NICKNAME','FECHA BDX']].value_counts())
            df_aux['FACTOR BDX ANULACION']=np.where((~df_aux['FECHA_ANULACION'].isnull())&(df_aux['FECHA_ANULACION']>df_aux['FECHA BDX']),0,1)
            df_aux['FACTOR BDX NV']=np.where(df_aux['FECHA_EFECTO']>df_aux['FECHA BDX'],0,1)
        elif tipo_proceso=='Cierre':
            df_aux['FACTOR BDX ANULACION']=1
            df_aux['FACTOR BDX NV']=1
            df_aux['APLICA BDX']=1
    elif contrato=='K-Fijo':
        df_aux['COMISION']=0
        df_aux['RVA MES1']=0
        df_aux['PRIMA NETA']=df_aux['PRIMA NETA ANUAL']
        df_aux['FACTOR BDX ANULACION']=1
        df_aux['FACTOR BDX NV']=1
        df_aux['APLICA BDX']=1
    # Eliminamos fechas de anulacion que son posteriores al periodo del BDX (i.e con FACTOR BDX == 0), y la NV posterior a la fecha del BDX
    df_aux['FEC AUX NA']=0
    df_aux['FEC AUX NA']=pd.to_datetime(df_aux['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
    df_aux['FECHA_ANULACION']=np.where(df_aux['FACTOR BDX ANULACION']==1,df_aux['FECHA_ANULACION'],df_aux['FEC AUX NA'])
    df_aux=df_aux.drop(columns=['FEC AUX NA'],axis=1)
    df_aux=df_aux[df_aux['FACTOR BDX NV']==1].copy()
    df_aux=df_aux[df_aux['APLICA BDX']==1].copy()        
    # PRINTEAMOS CUANTOS REGISTROS TIENEN LOS DF ORIGINALES
    cruzar_hist=sum((historico_aux['FECHA_ANULACION'].isnull())&(historico_aux['FECHA_VENCIMIENTO']>fecha_cierre))
    tot_hist=historico_aux.shape[0]
    tot_df=df_aux.shape[0]
    print('df salida contiene un total de {} registros'.format(tot_df))
    print('Historico salida contiene un total de {} registros, de los cuales {} se encuentran vigentes'.format(tot_hist,cruzar_hist))
    # CREACION DE LLAVES PARA CRUCES, DIFERENCIADOS POR CONTRATO
    if contrato == 'Cesantia': 
        # DEFINO LLAVE 1 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_1']=np.where((df_aux['SSEGURO'].isnull())|(df_aux['SSEGURO']==0),'',df_aux['SSEGURO'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        historico_aux['LLAVE_1']=np.where((historico_aux['SSEGURO'].isnull())|(historico_aux['SSEGURO']==0),'',historico_aux['SSEGURO'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_1','SSEGURO+COB',campos_extra)
        # DEFINO LLAVE 2 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_2']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+df_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        historico_aux['LLAVE_2']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_2','POLIZA+RUT+SEC+NRO_OPE+COB',campos_extra)
        # DEFINO LLAVE 3 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_3']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string')+'_'+df_aux['FECHA_VENCIMIENTO'].astype('string'))
        historico_aux['LLAVE_3']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string')+'_'+historico_aux['FECHA_VENCIMIENTO'].astype('string') )
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_3','RUT+NRO_OPE+COB+FEC_EFECTO+FEC_VENCIMIENTO',campos_extra)
        # DEFINO LLAVE 4 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_4']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['ICAPITAL'].astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string'))
        historico_aux['LLAVE_4']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['ICAPITAL'].astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_4','ICAPITAL+RUT+COB+FEC_EFECTO',campos_extra)
        # DEFINO LLAVE 5 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_5']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['ICAPITAL'].astype('int64').astype('string')+'_'+df_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string'))
        historico_aux['LLAVE_5']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['ICAPITAL'].astype('int64').astype('string')+'_'+historico_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_5','ICAPITAL+NRO_OPERACION+COB+FEC_EFECTO',campos_extra)
    if contrato == 'Cesantia (POL 280)': 
        # DEFINO LLAVE 1 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_1']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+df_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        historico_aux['LLAVE_1']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_1','POLIZA+RUT+SEC+NRO_OPE+COB',campos_extra)
        # DEFINO LLAVE 2 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_2']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string')+'_'+df_aux['FECHA_VENCIMIENTO'].astype('string'))
        historico_aux['LLAVE_2']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string')+'_'+historico_aux['FECHA_VENCIMIENTO'].astype('string') )
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_2','RUT+NRO_OPE+COB+FEC_EFECTO+FEC_VENCIMIENTO',campos_extra)
    if contrato == 'Cesantia P10-P12': 
        # DEFINO LLAVE 1 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_1']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+df_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        historico_aux['LLAVE_1']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['NRO_OPERACION'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_1','POLIZA+RUT+SEC+NRO_OPE+COB',campos_extra)
        # DEFINO LLAVE 2 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_2']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string')+'_'+df_aux['FECHA_VENCIMIENTO'].astype('string'))
        historico_aux['LLAVE_2']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string')+'_'+historico_aux['FECHA_VENCIMIENTO'].astype('string') )
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_2','POLIZA+RUT+SEC+COB+FEC_EFECTO+FEC_VENCIMIENTO',campos_extra)
        # DEFINO LLAVE 2 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_3']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string')+'_'+df_aux['FECHA_VENCIMIENTO'].astype('string'))
        historico_aux['LLAVE_3']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string')+'_'+historico_aux['FECHA_VENCIMIENTO'].astype('string') )
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_3','POLIZA+RUT+FEC_EFECTO+FEC_VENCIMIENTO',campos_extra)
        # DEFINO LLAVE 31 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_4']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        historico_aux['LLAVE_4']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_4','POLIZA+RUT+SEC+COB',campos_extra)
    if contrato =='K-Fijo': 
        # DEFINO LLAVE 1 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_1']=np.where((df_aux['SSEGURO'].isnull())|(df_aux['SSEGURO']==0),'',df_aux['SSEGURO'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        historico_aux['LLAVE_1']=np.where((historico_aux['SSEGURO'].isnull())|(historico_aux['SSEGURO']==0),'',historico_aux['SSEGURO'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_1','SSEGURO+COB',campos_extra)
        # DEFINO LLAVE 2 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_2']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        historico_aux['LLAVE_2']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_2','POLIZA+RUT+SEC+COB',campos_extra)
        # DEFINO LLAVE 3 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_3']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['RUT'].astype('int64').astype('string')+'_'+df_aux['POLIZA'].astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string')+'_'+df_aux['FECHA_VENCIMIENTO'].astype('string'))
        historico_aux['LLAVE_3']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['RUT'].astype('int64').astype('string')+'_'+historico_aux['POLIZA'].astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string')+'_'+historico_aux['FECHA_VENCIMIENTO'].astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_3','RUT+POL+COB+FEC_EFECTO+FEC_VENCIMIENTO',campos_extra)
        # DEFINO LLAVE 4 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_4']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['RUT'].astype('int64').astype('string')+'_'+df_aux['POLIZA'].astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string')+'_'+df_aux['ICAPITAL'].round(1).astype('int64').astype('string'))
        historico_aux['LLAVE_4']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['RUT'].astype('int64').astype('string')+'_'+historico_aux['POLIZA'].astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string')+'_'+historico_aux['ICAPITAL'].round(1).astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_4','RUT+POL+COB+FEC_EFECTO+ICAPITAL',campos_extra)
        # DEFINO LLAVE 5 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_5']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['RUT'].astype('int64').astype('string')+'_'+df_aux['POLIZA'].astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+df_aux['FECHA_EFECTO'].astype('string'))
        historico_aux['LLAVE_5']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['RUT'].astype('int64').astype('string')+'_'+historico_aux['POLIZA'].astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+historico_aux['FECHA_EFECTO'].astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_5','RUT+POL+COB+FEC_EFECTO',campos_extra)
        # DEFINO LLAVE 6 PARA HACER PROCESO DE IDENTIFICAR NUEVA VENTA Y NUEVOS VENCIMIENTOS Y LUEGO APLICO FUNCION DE NV Y VENCIMIENTOS
        df_aux['LLAVE_6']=np.where(df_aux['NUEVA VENTA']==0,'',df_aux['RUT'].astype('int64').astype('string')+'_'+df_aux['POLIZA'].astype('int64').astype('string')+'_'+df_aux['CODIGO COBERTURA'].astype('int64').astype('string'))
        historico_aux['LLAVE_6']=np.where(historico_aux['CRUCE CIERRE']==1,'',historico_aux['RUT'].astype('int64').astype('string')+'_'+historico_aux['POLIZA'].astype('int64').astype('string')+'_'+historico_aux['CODIGO COBERTURA'].astype('int64').astype('string'))
        df_aux,historico_aux=cruce_dfs(df_aux,historico_aux,'LLAVE_6','RUT+POL+COB',campos_extra)
    # EXPORTACION PRA AMANTIUM (HISTORICO CON DATA SSEGURO CORREGIDA)
    historico_aux_amantium=historico_aux.copy()
    historico_aux_amantium['SSEGURO']=historico_aux_amantium['SSEGURO_NUEVA']
    cols_exportar_amantium=['POLIZA','RUT','SECUENCIAL','SSEGURO','CODIGO COBERTURA','PRODUCTO','FECHA_EFECTO','FECHA_VENCIMIENTO','PRIMA NETA','ICAPITAL','PRIMA REASEGURO','FECHA_ANULACION','BASE'] if contrato=='K-Fijo' else ['POLIZA','RUT','SECUENCIAL','NRO_OPERACION','SSEGURO','CODIGO COBERTURA','PRODUCTO','FECHA_EFECTO','FECHA_VENCIMIENTO','PRIMA NETA','ICAPITAL','PRIMA REASEGURO','FECHA_ANULACION','CARENCIA','CONTRATO SISTEMA','BASE']
    historico_aux_amantium[historico_aux_amantium['BASE']=='IAXIS'][cols_exportar_amantium].to_csv(f'{ruta_output}Historico {contrato} (Envio Amantium).txt',sep=';',decimal=',',date_format='%d-%m-%Y',index=False)
    # EXTRAIGO DATAFRAMES DE NUEVA VENTA Y NUEVOS VENCIMIENTOS Y ACTUALIZO EL HISTORICO
    cols_calculos=cols_hist+campos_extra
    historico_no_encontrados=historico_aux[(historico_aux['CRUCE CIERRE']==0)]
    nueva_venta=df_aux[df_aux['NUEVA VENTA']==1]
    devoluciones=pd.concat([historico_aux[historico_aux['NUEVO VENCIMIENTO']==1][cols_calculos],nueva_venta[~nueva_venta['FECHA_ANULACION'].isnull()][cols_calculos]],axis=0)
    historico_new=pd.concat([historico_aux[cols_hist],nueva_venta[cols_hist]],axis=0)
    nueva_venta['PERIODO_EFECTO']=nueva_venta['FECHA_EFECTO'].dt.year*100+nueva_venta['FECHA_EFECTO'].dt.month
    devoluciones['PERIODO_ANULACION']=devoluciones['FECHA_ANULACION'].dt.year*100+devoluciones['FECHA_ANULACION'].dt.month
    if contrato=='K-Fijo': nueva_venta['APLICA']=0
    # CALCULAMOS LA COMISION DE REASEGURO POR REASEGURADOR, TANTO PARA LA NUEVA VENTA COMO PARA EL HISTORICO
    # if clasificacion_contrato=='Cesantia PU':
        # multi_operations(nueva_venta,'PRIMA CEDIDA','PRIMA REASEGURO','COMISION REASEGURO',lista_reaseguradores,'RESTAR')
        # multi_operations(historico_aux,'PRIMA CEDIDA','PRIMA REASEGURO','COMISION REASEGURO',lista_reaseguradores,'RESTAR')
    # CALCULO DE DEVOLUCIONES DE LOS NUEVOS devoluciones
    if 'CARENCIA' not in list(devoluciones.columns): devoluciones['CARENCIA']=0
    # cols_venc_prev=devoluciones.columns
    if contrato in ['K-Fijo']:
        devoluciones=calcula_devoluciones(devoluciones)
        devoluciones=calcula_devoluciones(devoluciones,campo_devolver='PRIMA NETA ANUAL')
    if clasificacion_contrato=='Cesantia PU':
        devoluciones=calcula_devoluciones(devoluciones)
        devoluciones=calcula_devoluciones(devoluciones,campo_devolver='PRIMA NETA ANUAL CIERRE')
        devoluciones['PRIMA CEDIDA']=devoluciones['PRIMA NETA ANUAL CIERRE']*devoluciones['PORCENTAJE CEDIDO FINAL']
        devoluciones['COMISION REASEGURO']=devoluciones['PRIMA CEDIDA']-devoluciones['PRIMA REASEGURO']
        devoluciones=calcula_devoluciones(devoluciones,campo_devolver='PRIMA CEDIDA')
        devoluciones=calcula_devoluciones(devoluciones,campo_devolver='COMISION REASEGURO')
    historico_calculos=pd.concat([historico_aux[cols_calculos],nueva_venta[cols_calculos]],axis=0)
    historico_new.to_csv(ruta_output+'8. Nuevo Historico '+contrato+'.txt',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    historico_no_encontrados.to_csv(ruta_output+'8. Historico '+contrato+' No Encontrados Detalle.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # EXPORTO LOS RESUMENES DE LOS RESULTADOS DE INTERES
    reportes(nueva_venta, 'PU NV', lista_reaseguradores)
    reportes(devoluciones, 'PU Devoluciones BDX', lista_reaseguradores)
    # SALIDAS PARA REPORTES DE CIERRE Y COC
    if clasificacion_contrato=='Cesantia PU':
        devoluciones_cierre=devoluciones[devoluciones['PERIODO_CONTABILIZACION']==periodo].copy()
        devoluciones_cierre['FECHA CIERRE']=fecha_cierre
    if contrato=='K-Fijo':
        historico_calculos=recalculo_bdx_kfijo(historico_calculos,lista_reaseguradores)
        devoluciones_cierre=devoluciones[devoluciones['PERIODO_CONTABILIZACION']==periodo].copy()
        uf_cierre=tabla_uf[tabla_uf['FECHA_EFECTO']==fecha_cierre].iloc[0]['UF_EMISION']
        devoluciones_cierre['PRIMA REASEGURO A DEVOLVER CLP']=devoluciones_cierre['PRIMA REASEGURO A DEVOLVER']*uf_cierre
    reportes( devoluciones_cierre, 'PU Devoluciones Cierre', lista_reaseguradores)
    historico_calculos.to_csv(ruta_output+'8. Historico Calculos Cierre '+contrato+'.txt',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)        
    return historico_calculos
        

def recalculo_bdx_kfijo(df_historico,lista_reaseguradores):
    df_historico=df_historico[df_historico['APLICA']==1].copy()
    fecha_aumento_prima = datetime.datetime(2020,10,5)
    costo_reaseguro = 0.015
    factor_profit = 1 - 0.4
    df_historico['CARENCIA']=0
    df_historico['PERIODO']=periodo
    df_historico['PERIODO_EFECTO']=df_historico['FECHA_EFECTO'].dt.year*100+df_historico['FECHA_EFECTO'].dt.month
    df_historico['PORCENTAJE CESION'] = (np.minimum(df_historico['ICAPITAL'],3000)*0.75+np.maximum(df_historico['ICAPITAL']-3000,0))/df_historico['ICAPITAL']
    df_historico['TIPO RVA']=np.where(((df_historico['FECHA_VENCIMIENTO']-df_historico['FECHA_EFECTO']).dt.days>365)&(df_historico['CODIGO COBERTURA']==112),'RM','RRC')
    df_historico['FACTOR NP']=np.where(df_historico['FECHA_EFECTO']>=fecha_aumento_prima,1/1.1,1)
    df_historico = calcula_devoluciones(df_historico)
    df_historico['FACTOR DEVOLUCION'] = np.where(df_historico['FACTOR DEVOLUCION'].isnull(),0,df_historico['FACTOR DEVOLUCION'])
    df_historico['PRIMA REASEGURO A DEVOLVER'] = df_historico['PRIMA REASEGURO'] * df_historico['FACTOR DEVOLUCION']
    df_historico['PRIMA NETA ANUAL A DEVOLVER'] = df_historico['PRIMA NETA ANUAL'] * df_historico['FACTOR DEVOLUCION']
    df_historico['GGAA REASEGURADOR'] = df_historico['PRIMA REASEGURO'] * costo_reaseguro
    df_historico['COMISION CORREDOR CEDIDA'] = np.where(df_historico['CODIGO COBERTURA']==112,1.19,1)*0.3373*df_historico['PRIMA REASEGURO']
    df_historico['COMISION RECAUDACION'] = np.where(df_historico['CODIGO COBERTURA']==112,1.19,0)*0.06
    df_historico['COMISION CORREDOR CEDIDA A DEVOLVER'] = df_historico['COMISION CORREDOR CEDIDA'] * df_historico['FACTOR DEVOLUCION']
    df_historico['RVA MES1 CEDIDA'] = df_historico['RVA MES1'] * df_historico['PORCENTAJE CESION']
    df_historico['COMISION TODO EVENTO'] = df_historico['PRIMA REASEGURO']*df_historico['FACTOR NP']-df_historico['COMISION CORREDOR CEDIDA']*df_historico['FACTOR NP']-df_historico['COMISION RECAUDACION']-0.35*df_historico['RVA MES1 CEDIDA']*np.where(df_historico['TIPO RVA']=='RRC',df_historico['FACTOR NP'],1)
    df_historico['COMISION TODO EVENTO A DEVOLVER'] = df_historico['COMISION TODO EVENTO'] * df_historico['FACTOR DEVOLUCION']
    df_historico['PROFIT'] = np.where(df_historico['FECHA_EFECTO']>=fecha_aumento_prima,0.1/1.1,0) * (df_historico['PRIMA REASEGURO'] - df_historico['COMISION CORREDOR CEDIDA'])*factor_profit
    # VARIABLES DE SALIDA QUE NECESITO CONSTRUIR
    df_historico['COMISION CORREDOR'] = np.where(df_historico['CODIGO COBERTURA']==112,1.19,1)*0.3373*df_historico['PRIMA NETA ANUAL']
    df_historico['COMISION CORREDOR A DEVOLVER'] = df_historico['COMISION CORREDOR'] * df_historico['FACTOR DEVOLUCION']
    df_historico['TIPO COBERTURA']=np.where(df_historico['CODIGO COBERTURA']==6,'ITP','FALLECIMIENTO')
    df_historico['CAIDA']=np.where(df_historico['FECHA_ANULACION'].isnull(),'NO','SI')
    df_historico['TIPO RVA FALLECIMIENTO']=np.where(df_historico['CODIGO COBERTURA']==112,df_historico['TIPO RVA'],'')
    df_historico['ICAPITAL FALLECIMIENTO']=np.where(df_historico['CODIGO COBERTURA']==112,df_historico['ICAPITAL'],0)
    df_historico['PRIMA NETA ANUAL FALLECIMIENTO']=np.where(df_historico['CODIGO COBERTURA']==112,df_historico['PRIMA NETA ANUAL'],0)
    df_historico['RVA MES1 FALLECIMIENTO']=np.where(df_historico['CODIGO COBERTURA']==112,df_historico['RVA MES1'],0)
    df_historico['TIPO RVA ITP']=np.where(df_historico['CODIGO COBERTURA']==6,df_historico['TIPO RVA'],'')
    df_historico['ICAPITAL ITP']=np.where(df_historico['CODIGO COBERTURA']==6,df_historico['ICAPITAL'],0)
    df_historico['PRIMA NETA ANUAL ITP']=np.where(df_historico['CODIGO COBERTURA']==6,df_historico['PRIMA NETA ANUAL'],0)
    df_historico['RVA MES1 ITP']=np.where(df_historico['CODIGO COBERTURA']==6,df_historico['RVA MES1'],0)
    df_historico['PRIMA NETA BASE']=df_historico['PRIMA NETA ANUAL']*df_historico['FACTOR NP']
    df_historico['PRIMA NETA AUMENTO']=df_historico['PRIMA NETA ANUAL']-df_historico['PRIMA NETA BASE']
    df_historico['PRIMA NETA BASE A DEVOLVER']=df_historico['PRIMA NETA ANUAL A DEVOLVER']*df_historico['FACTOR NP']
    df_historico['PRIMA NETA AUMENTO A DEVOLVER']=df_historico['PRIMA NETA ANUAL A DEVOLVER']-df_historico['PRIMA NETA BASE A DEVOLVER']
    df_historico['RVA MES1 BASE']=np.where(df_historico['TIPO RVA']=='RM',df_historico['RVA MES1'],df_historico['RVA MES1']*df_historico['FACTOR NP'])
    df_historico['RVA MES1 AUMENTO']=df_historico['RVA MES1']-df_historico['RVA MES1 BASE']
    df_historico['PORCENTAJE COMISION']=(df_historico['COMISION CORREDOR CEDIDA']+df_historico['COMISION RECAUDACION'])/df_historico['PRIMA REASEGURO']
    df_historico['COMISION BASE (INCLUYE DEV)']=(df_historico['COMISION CORREDOR']-df_historico['COMISION CORREDOR A DEVOLVER'])*df_historico['FACTOR NP']+df_historico['COMISION RECAUDACION']
    df_historico['COMISION AUMENTO (INCLUYE DEV)']=(df_historico['COMISION CORREDOR']-df_historico['COMISION CORREDOR A DEVOLVER'])*(1-df_historico['FACTOR NP'])
    df_historico['PRIMA REASEGURO BASE (INCLUYE DEV)']=(df_historico['PRIMA REASEGURO']-df_historico['PRIMA REASEGURO A DEVOLVER'])*df_historico['FACTOR NP']
    df_historico['PRIMA REASEGURO AUMENTO (INCLUYE DEV)']=(df_historico['PRIMA REASEGURO']-df_historico['PRIMA REASEGURO A DEVOLVER'])*(1-df_historico['FACTOR NP'])
    df_historico['COMISION CEDIDA BASE (INCLUYE DEV)']=(df_historico['COMISION CORREDOR CEDIDA']-df_historico['COMISION CORREDOR CEDIDA A DEVOLVER'])*df_historico['FACTOR NP']+df_historico['COMISION RECAUDACION']
    df_historico['COMISION CEDIDA AUMENTO (INCLUYE DEV)']=(df_historico['COMISION CORREDOR CEDIDA']-df_historico['COMISION CORREDOR CEDIDA A DEVOLVER'])*(1-df_historico['FACTOR NP'])
    df_historico['GGAA REASEGURADOR BASE']=df_historico['GGAA REASEGURADOR']*df_historico['FACTOR NP']
    df_historico['GGAA REASEGURADOR AUMENTO']=df_historico['GGAA REASEGURADOR']-df_historico['GGAA REASEGURADOR BASE']
    reportes(df_historico,'Recalculo BDX Reservas', lista_reaseguradores)
    reportes(df_historico[~df_historico['FECHA_ANULACION'].isnull()],'Recalculo BDX Anulaciones', lista_reaseguradores)
    return df_historico
    
    
def calculo_fechas_renovacion(df,campo_inicio,campo_fin,campo_anulacion,campo_periodicidad,periodo_cierre,ajuste_pu=1):
    df_aux=df.copy()
    cierre_month=periodo_cierre%100
    cierre_year=int(periodo_cierre/100)
    # fecha_cierre=datetime.datetime(int(periodo_cierre/100),periodo_cierre%100,calendar.monthrange(int(periodo_cierre/100), periodo_cierre%100)[1])
    # Defino los campos de dia mes y año para la fecha de la ultima renovacion
    df_aux['year']=cierre_year-np.where((df_aux[campo_inicio].dt.month>cierre_month)|((df_aux[campo_anulacion].dt.month*100+df_aux[campo_anulacion].dt.day<df_aux[campo_inicio].dt.month*100+df_aux[campo_inicio].dt.day)&(~df_aux[campo_anulacion].isnull())),1,0)
    df_aux['month']=df_aux[campo_inicio].dt.month
    df_aux['day']=np.where((df_aux[campo_inicio].dt.day==29)&(df_aux['month']==2)&(df_aux['year']%4>0),28,df_aux[campo_inicio].dt.day)
    df_aux['INICIO RENOVACION']=np.maximum(pd.to_datetime(df_aux[['year','month','day']]),df_aux[campo_inicio])
    # Defino los campos de dia mes y año para la fecha de la proxima renovacion
    df_aux['year']=df_aux['INICIO RENOVACION'].dt.year+1
    df_aux['day']=np.where((df_aux[campo_inicio].dt.day==29)&(df_aux['month']==2)&(df_aux['year']%4>0),28,df_aux[campo_inicio].dt.day)
    df_aux['FIN RENOVACION']=np.where(df_aux[campo_fin].isnull(),pd.to_datetime(df_aux[['year','month','day']]),np.minimum(pd.to_datetime(df_aux[['year','month','day']]),df_aux[campo_fin]))
    # Ajusto en caso de primas unicas
    if ajuste_pu==1: 
        series_inicio=np.where(df_aux[campo_periodicidad]==0,df_aux[campo_inicio],df_aux['INICIO RENOVACION'])
        series_fin=np.where(df_aux[campo_periodicidad]==0,df_aux[campo_fin],df_aux['FIN RENOVACION'])
    else:
        series_inicio=df_aux['INICIO RENOVACION']
        series_fin=df_aux['FIN RENOVACION']
    return series_inicio,series_fin


def dist_meses(df,fec_ini,fec_fin,nombre_campo,lastday_include=0,firstday_include=1):
    # REVISAMOS SI VAMOS A UTILIZAR UNA FECHA FIJA O UN CAMPO DENTRO DEL DATAFRAME PARA LA FECHA DE INICIO Y DE FIN
    type_fini=type(fec_ini)
    type_ffin=type(fec_fin)
    # A PARTIR DE LO ANTERIOR, DEFINO UNAS FECHAS A UTILIZAR PARA LOS CALCULOS (QUE DEPENDEN SI ES UN DATO UNICO O UN PANDA SERIES)
    if type_fini==datetime.datetime: finmes_ini=fec_ini+MonthEnd(0)
    else: finmes_ini=pd.to_datetime(df[fec_ini], format="%Y-%m-%d")+ MonthEnd(0)
    if type_ffin==datetime.datetime: finmes_fin=fec_fin+MonthEnd(0)
    else: finmes_fin=pd.to_datetime(df[fec_fin], format="%Y-%m.%d")+ MonthEnd(0)
    # DEPENDIENDO DEL TIPO DE DATO ES QUE HACEMOS LOS CALCULOS
    if (type_fini==datetime.datetime)&(type_ffin==datetime.datetime):
        df[nombre_campo]=(fec_fin.year-fec_ini.year)*12+(fec_fin.month-fec_ini.month)+((finmes_ini.day-fec_ini.day+firstday_include)/finmes_ini.day)+((fec_fin.day-finmes_fin.day-1+lastday_include)/finmes_fin.day)
    if (type_fini==datetime.datetime)&(type_ffin==str):
        df[nombre_campo]=(df[fec_fin].dt.year-fec_ini.year)*12+(df[fec_fin].dt.month-fec_ini.month)+((finmes_ini.day-fec_ini.day+firstday_include)/finmes_ini.day)+((df[fec_fin].dt.day-finmes_fin.dt.day-1+lastday_include)/finmes_fin.dt.day)
    if (type_fini==str)&(type_ffin==datetime.datetime):
        df[nombre_campo]=(fec_fin.year-df[fec_ini].dt.year)*12+(fec_fin.month-df[fec_ini].dt.month)+((finmes_ini.dt.day-df[fec_ini].dt.day+firstday_include)/finmes_ini.dt.day)+((fec_fin.day-finmes_fin.day-1+lastday_include)/finmes_fin.day)
    if (type_fini==str)&(type_ffin==str):
        df[nombre_campo]=(df[fec_fin].dt.year-df[fec_ini].dt.year)*12+(df[fec_fin].dt.month-df[fec_ini].dt.month)+((finmes_ini.dt.day-df[fec_ini].dt.day+firstday_include)/finmes_ini.dt.day)+((df[fec_fin].dt.day-finmes_fin.dt.day-1+lastday_include)/finmes_fin.dt.day)


def calculos_cierre_pr(df_salida,lista_reaseguradores):
    escribe_reporta(archivo_reporte,'{} Calculos de Pasivos Reaseguro Generales'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # for reaseg in lista_reaseguradores
    campos=list(df_salida.columns)
    campos_eliminar=[]
    for reaseg in lista_reaseguradores:
        for campo in campos:
            if reaseg in campo: campos_eliminar.append(campo)
    df_aux=df_salida.drop(columns=campos_eliminar,axis=1)
    # DEFINO VARIABLES AUXILIARES DE INTERES
    periodo_anterior=periodo-1 if fecha_cierre.month>1 else periodo-89
    fecha_cierre_anterior=datetime.datetime(int(periodo_anterior/100),periodo_anterior%100,calendar.monthrange(int(periodo_anterior/100), periodo_anterior%100)[1])
    # AGREGO AL DF LAS COMISIONES ASOCIADAS A RESERVAS
    # df_aux=df_aux.merge(comisiones,how='left',on='POL_PROD')
    df_aux=cruce_comisiones(df_aux,comisiones)
    df_aux=df_aux.merge(iva_coberturas,how='left',on='CODIGO COBERTURA')
    df_aux=df_aux.rename(columns={'FINI_RENOV_ANUAL':'FINI_RENOV_ANUAL ORIG','FFIN_RENOV_ANUAL':'FFIN_RENOV_ANUAL ORIG'})
    # CREO VARIABLES AUXILIARES QUE AYUDARÁN AL CALCULO
    # df_aux['FECHA_VENCIMIENTO']=np.where(df_aux['FECHA_VENCIMIENTO'].isnull(),df_aux['FFIN_RENOV_ANUAL'],df_aux['FECHA_VENCIMIENTO'])
    df_aux['FEC AUX NA']=0
    df_aux['FEC AUX NA']=pd.to_datetime(df_aux['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
    df_aux['FINI_RENOV_ANUAL'],df_aux['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_aux, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo)
    df_aux['FINI_RENOV_ANUAL MES ANTERIOR'],df_aux['FFIN_RENOV_ANUAL MES ANTERIOR']=calculo_fechas_renovacion(df_aux, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo_anterior)
    df_aux['FINI_RENOV_ANUAL'],df_aux['FFIN_ANUALIDAD']=calculo_fechas_renovacion(df_aux, 'FECHA_EFECTO', 'FEC AUX NA', 'FEC AUX NA','FORMA_PAGO_CODIGO', periodo)
    df_aux['FINI_RENOV_ANUAL MES ANTERIOR'],df_aux['FFIN_ANUALIDAD MES ANTERIOR']=calculo_fechas_renovacion(df_aux, 'FECHA_EFECTO', 'FEC AUX NA', 'FEC AUX NA','FORMA_PAGO_CODIGO', periodo_anterior)
    df_aux['PERIODO INICIO VIGENCIA']=pd.DatetimeIndex(df_aux['FINI_RENOV_ANUAL']).year*100+pd.DatetimeIndex(df_aux['FINI_RENOV_ANUAL']).month
    df_aux['FECHA TERMINO']=np.where(df_aux['FECHA_ANULACION'].isnull(),df_aux['FFIN_RENOV_ANUAL'],df_aux['FECHA_ANULACION'])
    df_aux['VIGENTE']=np.where(df_aux['FECHA TERMINO']<=fecha_cierre,0,1)
    df_aux['VIGENTE MES ANTERIOR']=np.where((df_aux['FECHA TERMINO']>fecha_cierre_anterior)&(df_aux['FECHA_EFECTO'].dt.year*100+df_aux['FECHA_EFECTO'].dt.month<=periodo_anterior),1,0)
    df_aux['CIERRE']=fecha_cierre
    df_aux['CIERRE MES ANTERIOR']=fecha_cierre_anterior
    # CALCULOS RELACIONADOS A COMO CONTAR LA DISTANCIA ENTRE FECHAS (DIAS O MESES)
    # FORMA DE CALCULO A TRAVÉS DE MEDIR LA DISTANCIA EN DIAS (LA NORMA LOCAL LO SOLICITA ASI)
    df_aux['PLAZO DIAS']=(df_aux['FFIN_RENOV_ANUAL']-df_aux['FINI_RENOV_ANUAL']).dt.days
    df_aux['PLAZO DIAS MES ANTERIOR']=(df_aux['FFIN_RENOV_ANUAL MES ANTERIOR']-df_aux['FINI_RENOV_ANUAL MES ANTERIOR']).dt.days
    df_aux['DIAS ANUALIDAD']=(df_aux['FFIN_ANUALIDAD']-df_aux['FINI_RENOV_ANUAL']).dt.days
    df_aux['DIAS ANUALIDAD MES ANTERIOR']=(df_aux['FFIN_ANUALIDAD MES ANTERIOR']-df_aux['FINI_RENOV_ANUAL MES ANTERIOR']).dt.days
    df_aux['DIAS NO DEVENGADOS']=(df_aux['FFIN_RENOV_ANUAL']-df_aux['CIERRE']).dt.days-1
    df_aux['DIAS NO DEVENGADOS MES ANTERIOR']=(df_aux['FFIN_RENOV_ANUAL MES ANTERIOR']-df_aux['CIERRE MES ANTERIOR']).dt.days-1
    df_aux['DIAS A DEVOLVER']=(df_aux['FFIN_RENOV_ANUAL']-df_aux['FECHA TERMINO']).dt.days
    # FORMA DE CALCULO A TRAVÉS DE MEDIR LA DISTANCIA EN MESES
    # dist_meses(df_aux,'FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','PLAZO MESES')
    # dist_meses(df_aux,'FINI_RENOV_ANUAL MES ANTERIOR','FFIN_RENOV_ANUAL MES ANTERIOR','PLAZO MESES MES ANTERIOR')
    # dist_meses(df_aux,fecha_cierre,'FFIN_RENOV_ANUAL','MESES NO DEVENGADOS',firstday_include=0)
    # dist_meses(df_aux,fecha_cierre_anterior,'FFIN_RENOV_ANUAL MES ANTERIOR','MESES NO DEVENGADOS MES ANTERIOR',firstday_include=0)
    # dist_meses(df_aux,'FECHA TERMINO','FFIN_RENOV_ANUAL','MESES A DEVOLVER',firstday_include=1,lastday_include=0)
    # CALCULO DE OTRAS VARIABLES
    df_aux['PARTICIPACION DEL REASEGURADOR']=1
    df_aux['CAPITAL CEDIDO TOTAL']=df_aux['PORCENTAJE CEDIDO FINAL']*df_aux['MONTO ASEGURADO']
    df_aux['CAPITAL RETENIDO TOTAL']=df_aux['MONTO ASEGURADO']-df_aux['CAPITAL CEDIDO TOTAL']
    # CALCULO DE FACTORES DE NO DEVENGO, DEVOLUCION, ANUALIDAD Y COMISIONES
    df_aux['FACTOR NO DEVENGADO']=np.where(df_aux['VIGENTE']==0,0,df_aux['DIAS NO DEVENGADOS']/df_aux['PLAZO DIAS'])
    df_aux['FACTOR NO DEVENGADO MES ANTERIOR']=np.where(df_aux['VIGENTE MES ANTERIOR']==0,0,df_aux['DIAS NO DEVENGADOS MES ANTERIOR']/df_aux['PLAZO DIAS MES ANTERIOR'])
    df_aux['FACTOR DEVOLUCION']=np.where((df_aux['VIGENTE']==1)|(df_aux['PLAZO DIAS']==0),0,np.maximum(0,df_aux['DIAS A DEVOLVER']/df_aux['PLAZO DIAS']))
    df_aux['FACTOR ANUALIDAD']=np.minimum(df_aux['PLAZO DIAS']/df_aux['DIAS ANUALIDAD'],1)
    df_aux['FACTOR ANUALIDAD MES ANTERIOR']=np.minimum(df_aux['PLAZO DIAS MES ANTERIOR']/df_aux['DIAS ANUALIDAD MES ANTERIOR'],1)
    df_aux['FACTOR COMISION']=1-np.where(df_aux['COBERTURA CON IVA']==1,np.minimum(df_aux['COMISION'],0.3),np.minimum(1.19*df_aux['COMISION'],0.3))
    # CALCULO DE VALORES PREVIOS A LOS CALCULOS DE REASEGURO
    df_aux['MONTO ASEGURADO DIRECTO']=df_aux['MONTO ASEGURADO']*df_aux['PARTICIPACION DEL REASEGURADOR']
    df_aux['MONTO ASEGURADO CEDIDO']=df_aux['CAPITAL CEDIDO TOTAL']*df_aux['PARTICIPACION DEL REASEGURADOR']
    df_aux['MONTO ASEGURADO RETENIDO']=df_aux['CAPITAL RETENIDO TOTAL']*df_aux['PARTICIPACION DEL REASEGURADOR']
    df_aux['PRIMA DIRECTA ANUALIZADA']=df_aux['PRIMA NETA ANUAL']*df_aux['PARTICIPACION DEL REASEGURADOR']*df_aux['COASEGURO']*df_aux['FACTOR ANUALIDAD']
    df_aux['PRIMA CEDIDA ANUALIZADA']=df_aux['PRIMA NETA ANUAL']*df_aux['PARTICIPACION DEL REASEGURADOR']*df_aux['PORCENTAJE CEDIDO FINAL']*df_aux['FACTOR ANUALIDAD']
    df_aux['PRIMA REASEGURO ANUALIZADA']=df_aux['PRIMA REASEGURO']*df_aux['FACTOR ANUALIDAD']*12/df_aux['EXPOSICION MENSUAL']
    df_aux['COMISION DE REASEGURO ANUALIZADA']=df_aux['PRIMA CEDIDA ANUALIZADA']-df_aux['PRIMA REASEGURO ANUALIZADA']
    df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA']=df_aux['PRIMA CEDIDA ANUALIZADA']*df_aux['FACTOR COMISION']-df_aux['PRIMA REASEGURO ANUALIZADA']
    df_aux['PRIMA DIRECTA ANUALIZADA MES ANTERIOR']=df_aux['PRIMA NETA ANUAL']*df_aux['PARTICIPACION DEL REASEGURADOR']*df_aux['COASEGURO']*df_aux['FACTOR ANUALIDAD MES ANTERIOR']
    df_aux['PRIMA CEDIDA ANUALIZADA MES ANTERIOR']=df_aux['PRIMA NETA ANUAL']*df_aux['PARTICIPACION DEL REASEGURADOR']*df_aux['PORCENTAJE CEDIDO FINAL']*df_aux['FACTOR ANUALIDAD MES ANTERIOR']
    df_aux['PRIMA REASEGURO ANUALIZADA MES ANTERIOR']=df_aux['PRIMA REASEGURO']*df_aux['FACTOR ANUALIDAD MES ANTERIOR']*12/df_aux['EXPOSICION MENSUAL']
    df_aux['COMISION DE REASEGURO ANUALIZADA MES ANTERIOR']=df_aux['PRIMA CEDIDA ANUALIZADA MES ANTERIOR']-df_aux['PRIMA REASEGURO ANUALIZADA MES ANTERIOR']
    df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA MES ANTERIOR']=df_aux['PRIMA CEDIDA ANUALIZADA MES ANTERIOR']*df_aux['FACTOR COMISION']-df_aux['PRIMA REASEGURO ANUALIZADA MES ANTERIOR']
    # CONCEPTOS BDX
    df_aux['PRIMA CEDIDA BDX']=df_aux['PRIMA NETA ANUAL']*df_aux['PARTICIPACION DEL REASEGURADOR']*df_aux['PORCENTAJE CEDIDO FINAL']/12*df_aux['EXPOSICION MENSUAL']
    df_aux['PRIMA REASEGURO BDX']=np.where(pd.DatetimeIndex(df_aux['FINI_RENOV_ANUAL']).month!=fecha_cierre.month,df_aux['PRIMA REASEGURO']/df_aux['FACTOR MENSUALIZACION']*dias_exposicion/df_aux['DIAS ANUALIDAD'],df_aux['PRIMA REASEGURO']/df_aux['FACTOR MENSUALIZACION']*((pd.DatetimeIndex(df_aux['FINI_RENOV_ANUAL']).day-1)/df_aux['DIAS ANUALIDAD MES ANTERIOR'] + (dias_exposicion-pd.DatetimeIndex(df_aux['FINI_RENOV_ANUAL']).day+1)/df_aux['DIAS ANUALIDAD']))
    df_aux['COMISION DE REASEGURO BDX']=df_aux['PRIMA CEDIDA BDX']-df_aux['PRIMA REASEGURO BDX']
    # Reservas y Activos de reaseguro del mes
    df_aux['RRC DIRECTA']=df_aux['PRIMA DIRECTA ANUALIZADA']*df_aux['FACTOR NO DEVENGADO']*df_aux['FACTOR COMISION']
    df_aux['RRC CEDIDA']=df_aux['PRIMA CEDIDA ANUALIZADA']*df_aux['FACTOR NO DEVENGADO']*df_aux['FACTOR COMISION']
    df_aux['PRIMA DIRECTA NO GANADA']=df_aux['PRIMA DIRECTA ANUALIZADA']*df_aux['FACTOR NO DEVENGADO']
    df_aux['PRIMA REASEGURO NO GANADA']=df_aux['PRIMA REASEGURO ANUALIZADA']*df_aux['FACTOR NO DEVENGADO']
    df_aux['PRIMA CEDIDA NO GANADA']=df_aux['PRIMA CEDIDA ANUALIZADA']*df_aux['FACTOR NO DEVENGADO']
    df_aux['COMISION DE REASEGURO NO GANADA']=df_aux['COMISION DE REASEGURO ANUALIZADA']*df_aux['FACTOR NO DEVENGADO']
    df_aux['COMISION DE REASEGURO LOCAL NO GANADA']=df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA']*df_aux['FACTOR NO DEVENGADO']
    # Primas del mes
    df_aux['PRIMA DIRECTA ANUALIZADA DEL MES']=np.where(df_aux['PERIODO INICIO VIGENCIA']==periodo,df_aux['PRIMA DIRECTA ANUALIZADA'],0)
    df_aux['PRIMA REASEGURO ANUALIZADA DEL MES']=np.where(df_aux['PERIODO INICIO VIGENCIA']==periodo,df_aux['PRIMA REASEGURO ANUALIZADA'],0)
    df_aux['PRIMA CEDIDA ANUALIZADA DEL MES']=np.where(df_aux['PERIODO INICIO VIGENCIA']==periodo,df_aux['PRIMA CEDIDA ANUALIZADA'],0)
    df_aux['COMISION DE REASEGURO ANUALIZADA DEL MES']=np.where(df_aux['PERIODO INICIO VIGENCIA']==periodo,df_aux['COMISION DE REASEGURO ANUALIZADA'],0)
    df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA DEL MES']=np.where(df_aux['PERIODO INICIO VIGENCIA']==periodo,df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA'],0)
    # Devoluciones del mes
    df_aux['DEVOLUCION PRIMA DIRECTA']=-df_aux['PRIMA DIRECTA ANUALIZADA']*df_aux['FACTOR DEVOLUCION']
    df_aux['DEVOLUCION PRIMA REASEGURO']=-df_aux['PRIMA REASEGURO ANUALIZADA']*df_aux['FACTOR DEVOLUCION']
    df_aux['DEVOLUCION PRIMA CEDIDA']=-df_aux['PRIMA CEDIDA ANUALIZADA']*df_aux['FACTOR DEVOLUCION']
    df_aux['DEVOLUCION COMISION DE REASEGURO']=-df_aux['COMISION DE REASEGURO ANUALIZADA']*df_aux['FACTOR DEVOLUCION']
    df_aux['DEVOLUCION COMISION DE REASEGURO LOCAL']=-df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA']*df_aux['FACTOR DEVOLUCION']
    # Reservas y Activos de reaseguro del mes anterior
    df_aux['RRC DIRECTA MES ANTERIOR']=df_aux['PRIMA DIRECTA ANUALIZADA MES ANTERIOR']*df_aux['FACTOR NO DEVENGADO MES ANTERIOR']*df_aux['FACTOR COMISION']
    df_aux['RRC CEDIDA MES ANTERIOR']=df_aux['PRIMA CEDIDA ANUALIZADA MES ANTERIOR']*df_aux['FACTOR NO DEVENGADO MES ANTERIOR']*df_aux['FACTOR COMISION']
    df_aux['PRIMA DIRECTA NO GANADA MES ANTERIOR']=df_aux['PRIMA DIRECTA ANUALIZADA MES ANTERIOR']*df_aux['FACTOR NO DEVENGADO MES ANTERIOR']
    df_aux['PRIMA REASEGURO NO GANADA MES ANTERIOR']=df_aux['PRIMA REASEGURO ANUALIZADA MES ANTERIOR']*df_aux['FACTOR NO DEVENGADO MES ANTERIOR']
    df_aux['PRIMA CEDIDA NO GANADA MES ANTERIOR']=df_aux['PRIMA CEDIDA ANUALIZADA MES ANTERIOR']*df_aux['FACTOR NO DEVENGADO MES ANTERIOR']
    df_aux['COMISION DE REASEGURO NO GANADA MES ANTERIOR']=df_aux['COMISION DE REASEGURO ANUALIZADA MES ANTERIOR']*df_aux['FACTOR NO DEVENGADO MES ANTERIOR']
    df_aux['COMISION DE REASEGURO LOCAL NO GANADA MES ANTERIOR']=df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA MES ANTERIOR']*df_aux['FACTOR NO DEVENGADO MES ANTERIOR']
    # VALIDACION DE RESULTADOS
    df_aux['COSTO REASEGURO LOCAL']=((df_aux['PRIMA CEDIDA ANUALIZADA DEL MES']+df_aux['DEVOLUCION PRIMA CEDIDA'])*df_aux['FACTOR COMISION']-(df_aux['RRC CEDIDA']-df_aux['RRC CEDIDA MES ANTERIOR']))-(df_aux['COMISION DE REASEGURO LOCAL ANUALIZADA DEL MES']+df_aux['DEVOLUCION COMISION DE REASEGURO LOCAL']-(df_aux['COMISION DE REASEGURO LOCAL NO GANADA']-df_aux['COMISION DE REASEGURO LOCAL NO GANADA MES ANTERIOR']))
    df_aux['COSTO REASEGURO IFRS']=(df_aux['PRIMA CEDIDA ANUALIZADA DEL MES']+df_aux['DEVOLUCION PRIMA CEDIDA']-(df_aux['PRIMA CEDIDA NO GANADA']-df_aux['PRIMA CEDIDA NO GANADA MES ANTERIOR']))-(df_aux['COMISION DE REASEGURO ANUALIZADA DEL MES']+df_aux['DEVOLUCION COMISION DE REASEGURO']-(df_aux['COMISION DE REASEGURO NO GANADA']-df_aux['COMISION DE REASEGURO NO GANADA MES ANTERIOR']))
    df_aux['DIFF COSTO REASEGURO LOCAL']=(df_aux['COSTO REASEGURO LOCAL']-df_aux['PRIMA REASEGURO BDX']).round(6)
    df_aux['DIFF COSTO REASEGURO IFRS']=(df_aux['COSTO REASEGURO IFRS']-df_aux['PRIMA REASEGURO BDX']).round(6)
    df_errores_local=df_aux[df_aux['DIFF COSTO REASEGURO LOCAL']!=0]
    df_errores_ifrs=df_aux[df_aux['DIFF COSTO REASEGURO IFRS']!=0]
    if not df_errores_local.empty:
        escribe_reporta(archivo_reporte,'Se detectaron {} errores en el calculo del costo de reaseguro local. REVISAR!'.format(df_errores_local.shape[0]))
        # print('Se detectaron {} errores en el calculo del costo de reaseguro local. REVISAR!'.format(df_errores_local.shape[0]))
        df_errores_local.to_csv(ruta_output+'6. Descuadres Calculos Reaseguro Local Detalle.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    if not df_errores_ifrs.empty:
        escribe_reporta(archivo_reporte,'Se detectaron {} errores en el calculo del costo de reaseguro ifrs. REVISAR!'.format(df_errores_ifrs.shape[0]))
        # print('Se detectaron {} errores en el calculo del costo de reaseguro ifrs. REVISAR!'.format(df_errores_ifrs.shape[0]))
        df_errores_ifrs.to_csv(ruta_output+'6. Descuadres Calculos Reaseguro IFRS Detalle.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # REVISION GENERAL Y DETALLADA DE RESULTADOS
    reportes(df_aux[(df_aux['VIGENTE']==1)|(df_aux['VIGENTE MES ANTERIOR']==1)], 'Reservas', lista_reaseguradores)
    if periodo%100%3 == 0: reportes(df_aux, 'Auditoria Reservas', lista_reaseguradores)
    return df_aux
    

def filtra_df(df,lista_campos,lista_valores):
    df_filtrado=df.copy()
    # For que va filtrando 1 a 1 las caracteristicas del tipo_calculo de reaseguro en el dataframe    
    for col,valor in zip(lista_campos,lista_valores):
       if str(valor)!='nan': df_filtrado=df_filtrado[df_filtrado[col]==valor]
       if df_filtrado.empty: return df_filtrado,df
    df_a_filtrar=df.loc[df.index.difference(df_filtrado.index)]
    return df_filtrado,df_a_filtrar


def aplica_reaseguro(df):
    df_residual=df.copy()
    # FILTROS PARA LA APLICACION DE EDADES DE INGRESO Y PERMANENCIA
    df_residual['APLICA EDAD']=1
    if 'EDAD RENOVACION' in df_residual.columns: df_residual['APLICA EDAD']=np.where((df_residual['EDAD RENOVACION']<=df_residual['EDAD MAXIMA PERMANENCIA'])|(df_residual['EDAD MAXIMA PERMANENCIA'].isnull()),1,0)*df_residual['APLICA EDAD']
    if 'EDAD INGRESO' in df_residual.columns: df_residual['APLICA EDAD']=np.where((df_residual['EDAD INGRESO']<=df_residual['EDAD MAXIMA INGRESO'])|(df_residual['EDAD MAXIMA INGRESO'].isnull()),1,0)*df_residual['APLICA EDAD']
    # FILTROS DE REASEGURO SEGUN OTRAS CONDICIONES
    df_aux=pd.DataFrame()
    # Recorre el dataframe para cada registro de filtro que deba hacer, para ir concatenandolos en el dataframe que necesitamos
    for index, row in quitar_reaseguro.iterrows():
        df_filtrado,df_residual=filtra_df(df_residual,list(quitar_reaseguro.columns),list(row))
        df_aux=pd.concat([df_aux,df_filtrado],axis=0)
    df_aux['APLICA REASEGURO']=0
    df_residual['APLICA REASEGURO']=1
    df_final=pd.concat([df_aux,df_residual],axis=0)
    df_final['APLICA']=df_final['APLICA EDAD']*df_final['APLICA REASEGURO']
    return df_final


def calcula_exposicion(df,campo_inicio,campo_fin,exp_days,fec_bop,fec_eop):
    """ Funcion de calculo de exposicion """
    df_aux=df.copy()
    df_aux['INICIO MES']=pd.Timestamp(fec_bop.year, fec_bop.month, fec_bop.day)
    df_aux['FIN MES']=pd.Timestamp(fec_eop.year, fec_eop.month, fec_eop.day)+datetime.timedelta(days=1)
    serie_inicio=np.maximum(df_aux['INICIO MES'],df_aux[campo_inicio])
    serie_fin=np.minimum(df_aux['FIN MES'],df_aux[campo_fin])
    serie_exposicion=np.where((df_aux[campo_inicio]>fec_eop) | (df_aux[campo_fin]<fec_bop) |(df_aux[campo_inicio]>df_aux[campo_fin]),0,((serie_fin-serie_inicio).dt.days)/exp_days)
    return serie_exposicion


def calcula_edad(rut_series,fec_nac_series,fec_corte_series,edad_perdidos,edad_tope,reporta_issues=0):
    """ Funcion de calculo de edad """
    df_ruts=pd.DataFrame({'RUT':rut_series,'FEC_NAC':fec_nac_series})
    df_fechas_nac=pd.DataFrame({'RUT':rut_series,'FEC_NAC':fec_nac_series}).groupby(['RUT']).min().reset_index()
    df_ruts_final=df_ruts.merge(df_fechas_nac,how='left',on='RUT')
    fec_nac_series=df_ruts_final['FEC_NAC_y']
    edad_malas=np.where((fec_nac_series.isnull())|(fec_nac_series==datetime.datetime(1900,1,1)),1,0)
    serie_year=pd.DatetimeIndex(fec_nac_series).year
    serie_monthday=pd.DatetimeIndex(fec_nac_series).month*100+pd.DatetimeIndex(fec_nac_series).day
    if type(fec_corte_series)==pd.core.series.Series: 
        fec_corte_year=pd.DatetimeIndex(fec_corte_series).year
        fec_corte_monthday=pd.DatetimeIndex(fec_corte_series).month*100+pd.DatetimeIndex(fec_corte_series).day
    else:
        fec_corte_year=fec_corte_series.year
        fec_corte_monthday=fec_corte_series.month*100+fec_corte_series.day
    edad_series=np.where(edad_malas==1,edad_perdidos,fec_corte_year-serie_year+np.where(serie_monthday<=fec_corte_monthday,0,-1))
    registros_issues=np.where((edad_malas==1)|(np.where(edad_series>edad_tope,1,0)==1),1,0)
    cont_fecnac_malas=sum(edad_malas)
    cont_fecnac_tope=sum(edad_series>edad_tope)
    edad_series_tope=np.where(edad_series>edad_tope,edad_tope,edad_series)
    if cont_fecnac_malas>0: escribe_reporta(archivo_reporte,'La cantidad de registros con la fecha nula o mala es de {} registros'.format(cont_fecnac_malas))
    if cont_fecnac_tope>0: escribe_reporta(archivo_reporte,'Un total de {} registros tienen edad mayor a 108 año. Fueron topados en 108 para poder encontrar valores en las tablas de incidencia'.format(cont_fecnac_tope))
    if reporta_issues==0: return edad_series_tope
    else: return edad_series_tope,registros_issues
    

def cruce_comisiones(df_aux,comisiones):
    comisiones_unicas=comisiones[comisiones['NRO_CASOS']==1].copy()
    comisiones_con_fechas=comisiones[comisiones['NRO_CASOS']>1].copy()
    df_com_unicas=df_aux.merge(comisiones_unicas[['POL_PROD','COMISION']],how='inner',on=['POL_PROD'])
    df_com_con_fechas=pd.DataFrame()
    for pol_prod in comisiones_con_fechas['POL_PROD'].unique():
        tabla_filtrada=comisiones_con_fechas[comisiones_con_fechas['POL_PROD']==pol_prod].copy()
        df_filtrado=df_aux[df_aux['POL_PROD']==pol_prod].copy()
        df_com_con_fechas=pd.concat([df_com_con_fechas,pd.merge_asof(df_filtrado.sort_values('FECHA_EFECTO'),tabla_filtrada.sort_values('FECHA_EMISION_COMISION').drop(['POL_PROD','NRO_CASOS'],axis=1),left_on=['FECHA_EFECTO'],right_on='FECHA_EMISION_COMISION')],axis=0)
    df_final=pd.concat([df_com_unicas,df_com_con_fechas],axis=0)
    if df_final.shape[0]>df_aux.shape[0]: escribe_reporta(archivo_reporte, 'El cruce de comisiones hizo más cruces')
    elif df_final.shape[0]<df_aux.shape[0]: 
        escribe_reporta(archivo_reporte, 'El cruce de comisiones hizo menos cruces')
        df_cruce=df_aux.merge(comisiones_unicas[['POL_PROD','COMISION']],how='left',on=['POL_PROD'])
        df_cruce[df_cruce['COMISION'].isnull()].to_csv(ruta_output+'0. Registros sin Comisiones.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    return df_final


def calculos_cierre_pu(df_historico_cierre,lista_reaseguradores):
    escribe_reporta(archivo_reporte,'Relizando Calculos de Cierre para contratos de Prima Unica')
    if contrato=='K-Fijo':
        # DEFNIMOS VARIABLES DEL PROCESO Y HACEMOS UNA COPIA DEL HISTORICO
        historico_aux=df_historico_cierre.copy()
        historico_aux['VIGENTE'] = np.where(((historico_aux['FECHA_ANULACION']<=fecha_cierre)&(~historico_aux['FECHA_ANULACION'].isnull()))|(historico_aux['FECHA_VENCIMIENTO']<=fecha_cierre)|(historico_aux['FECHA_EFECTO']>fecha_cierre),0,1)
        historico_aux['FACTOR DEVENGO T']= np.where(historico_aux['VIGENTE']==0,0,((historico_aux['FECHA_VENCIMIENTO']-fecha_cierre).dt.days-1)/(historico_aux['FECHA_VENCIMIENTO']-historico_aux['FECHA_EFECTO']).dt.days)
        historico_aux['FACTOR CONTABILIZACION'] = np.where(historico_aux['PERIODO_CONTABILIZACION']==periodo,1,0)
        # CONCEPTOS DE DEVOLUCIONES
        historico_aux['GWP C'] = historico_aux['PRIMA REASEGURO A DEVOLVER'] * historico_aux['FACTOR CONTABILIZACION']
        historico_aux['COM C'] = -historico_aux['COMISION CORREDOR CEDIDA A DEVOLVER'] * historico_aux['FACTOR CONTABILIZACION']
        historico_aux['CTE REA'] = -historico_aux['COMISION TODO EVENTO A DEVOLVER'] * historico_aux['FACTOR CONTABILIZACION']
        # historico_aux['COSTO REA'] = historico_aux['GWP C'] * costo_reaseguro
        # CONCEPTOS DE ACTIVOS/PASIVOS
        historico_aux['DAC CTE T LOCAL'] = -1 * historico_aux['VIGENTE'] * historico_aux['FACTOR DEVENGO T'] * np.where(historico_aux['CODIGO COBERTURA']==6,historico_aux['COMISION TODO EVENTO'],0)
        historico_aux['DAC C IFRS'] = (historico_aux['COMISION CORREDOR CEDIDA']+historico_aux['COMISION RECAUDACION']) * historico_aux['VIGENTE'] * historico_aux['FACTOR DEVENGO T']
        historico_aux['DAC CTE T IFRS'] = historico_aux['VIGENTE'] * historico_aux['FACTOR DEVENGO T'] * historico_aux['COMISION TODO EVENTO']
        historico_aux['DAC PR T IFRS'] = historico_aux['VIGENTE'] * historico_aux['FACTOR DEVENGO T'] * historico_aux['GGAA REASEGURADOR']
        reportes(historico_aux, 'Reservas', lista_reaseguradores)
        if periodo%100%3 == 0: reportes(historico_aux, 'Auditoria Reservas', lista_reaseguradores)
        return historico_aux
    if clasificacion_contrato=='Cesantia PU' :
        historico_aux=df_historico_cierre.copy()
        # historico_aux=historico_aux.merge(comisiones,how='left',on='POL_PROD')
        historico_aux=cruce_comisiones(historico_aux,comisiones)
        historico_aux=historico_aux.merge(iva_coberturas,how='left',on='CODIGO COBERTURA')
        historico_aux=historico_aux.merge(tabla_uf,how='left',on='FECHA_EFECTO')
        uf_cierre=tabla_uf[tabla_uf['FECHA_EFECTO']==fecha_cierre].iloc[0]['UF_EMISION']
        # CREO VARIABLES AUXILIARES QUE AYUDARÁN AL CALCULO
        historico_aux['PERIODO INICIO VIGENCIA']=pd.DatetimeIndex(historico_aux['FECHA_EFECTO']).year*100+pd.DatetimeIndex(historico_aux['FECHA_EFECTO']).month
        historico_aux['FECHA TERMINO']=np.where(historico_aux['FECHA_ANULACION'].isnull(),historico_aux['FECHA_VENCIMIENTO'],historico_aux['FECHA_ANULACION'])
        historico_aux['VIGENTE']=np.where(historico_aux['FECHA TERMINO']<=fecha_cierre,0,1)
        historico_aux['PLAZO DIAS']=(historico_aux['FECHA_VENCIMIENTO']-historico_aux['FECHA_EFECTO']).dt.days
        historico_aux['DIAS NO DEVENGADOS']=(historico_aux['FECHA_VENCIMIENTO']-historico_aux['CIERRE']).dt.days-1
        historico_aux['DIAS A DEVOLVER']=(historico_aux['FECHA_VENCIMIENTO']-historico_aux['FECHA TERMINO']).dt.days
        historico_aux['PARTICIPACION DEL REASEGURADOR']=1
        historico_aux['CAPITAL CEDIDO TOTAL']=historico_aux['PORCENTAJE CEDIDO FINAL']*historico_aux['MONTO ASEGURADO']
        historico_aux['CAPITAL RETENIDO TOTAL']=historico_aux['MONTO ASEGURADO']-historico_aux['CAPITAL CEDIDO TOTAL']
        # CALCULO DE FACTORES DE NO DEVENGO, DEVOLUCION, ANUALIDAD Y COMISIONES
        historico_aux['FACTOR NO DEVENGADO']=np.where(historico_aux['VIGENTE']==0,0,historico_aux['DIAS NO DEVENGADOS']/historico_aux['PLAZO DIAS'])
        historico_aux['FACTOR DEVOLUCION']=np.where((historico_aux['VIGENTE']==1)|(historico_aux['PLAZO DIAS']==0),0,np.maximum(0,historico_aux['DIAS A DEVOLVER']/historico_aux['PLAZO DIAS']))*np.where(historico_aux['PERIODO_CONTABILIZACION']==periodo,1,0)
        historico_aux['FACTOR COMISION']=1-np.where(historico_aux['COBERTURA CON IVA']==1,np.minimum(historico_aux['COMISION'],0.3),np.minimum(1.19*historico_aux['COMISION'],0.3))
        historico_aux['FACTOR UF']=historico_aux['UF_EMISION']/uf_cierre*0+1
        historico_aux['PRIMA NETA ANUAL VIGENTE CIERRE AJUST UF']=historico_aux['PRIMA NETA ANUAL CIERRE']*historico_aux['UF_EMISION']/uf_cierre*historico_aux['VIGENTE']
        # CALCULO DE VALORES PREVIOS A LOS CALCULOS DE REASEGURO
        historico_aux['MONTO ASEGURADO DIRECTO']=historico_aux['MONTO ASEGURADO']*historico_aux['PARTICIPACION DEL REASEGURADOR']
        historico_aux['MONTO ASEGURADO CEDIDO']=historico_aux['CAPITAL CEDIDO TOTAL']*historico_aux['PARTICIPACION DEL REASEGURADOR']
        historico_aux['MONTO ASEGURADO RETENIDO']=historico_aux['CAPITAL RETENIDO TOTAL']*historico_aux['PARTICIPACION DEL REASEGURADOR']
        historico_aux['PRIMA DIRECTA ANUALIZADA']=historico_aux['PRIMA NETA ANUAL CIERRE']*historico_aux['PARTICIPACION DEL REASEGURADOR']*historico_aux['COASEGURO']
        historico_aux['PRIMA CEDIDA ANUALIZADA']=historico_aux['PRIMA NETA ANUAL CIERRE']*historico_aux['PARTICIPACION DEL REASEGURADOR']*historico_aux['PORCENTAJE CEDIDO FINAL']
        historico_aux['PRIMA REASEGURO ANUALIZADA']=historico_aux['PRIMA REASEGURO']
        historico_aux['COMISION DE REASEGURO ANUALIZADA']=historico_aux['PRIMA CEDIDA ANUALIZADA']-historico_aux['PRIMA REASEGURO ANUALIZADA']
        historico_aux['COMISION DE REASEGURO LOCAL ANUALIZADA']=historico_aux['PRIMA CEDIDA ANUALIZADA']*historico_aux['FACTOR COMISION']-historico_aux['PRIMA REASEGURO ANUALIZADA']
        # CONCEPTOS BDX
        historico_aux['PRIMA CEDIDA BDX']=historico_aux['PRIMA NETA ANUAL CIERRE']*historico_aux['PARTICIPACION DEL REASEGURADOR']*historico_aux['PORCENTAJE CEDIDO FINAL']
        historico_aux['COMISION DE REASEGURO BDX']=historico_aux['PRIMA CEDIDA BDX']-historico_aux['PRIMA REASEGURO']
        historico_aux['PRIMA REASEGURO BDX']=historico_aux['PRIMA REASEGURO']
        # Reservas y Activos de reaseguro del mes
        historico_aux['RRC DIRECTA']=historico_aux['PRIMA DIRECTA ANUALIZADA']*historico_aux['FACTOR NO DEVENGADO']*historico_aux['FACTOR COMISION']*historico_aux['FACTOR UF']
        historico_aux['RRC CEDIDA']=historico_aux['PRIMA CEDIDA ANUALIZADA']*historico_aux['FACTOR NO DEVENGADO']*historico_aux['FACTOR COMISION']*historico_aux['FACTOR UF']
        historico_aux['PRIMA DIRECTA NO GANADA']=historico_aux['PRIMA DIRECTA ANUALIZADA']*historico_aux['FACTOR NO DEVENGADO']*historico_aux['FACTOR UF']
        historico_aux['PRIMA REASEGURO NO GANADA']=historico_aux['PRIMA REASEGURO ANUALIZADA']*historico_aux['FACTOR NO DEVENGADO']*historico_aux['FACTOR UF']
        historico_aux['PRIMA CEDIDA NO GANADA']=historico_aux['PRIMA CEDIDA ANUALIZADA']*historico_aux['FACTOR NO DEVENGADO']*historico_aux['FACTOR UF']
        historico_aux['COMISION DE REASEGURO NO GANADA']=historico_aux['COMISION DE REASEGURO ANUALIZADA']*historico_aux['FACTOR NO DEVENGADO']*historico_aux['FACTOR UF']
        historico_aux['COMISION DE REASEGURO LOCAL NO GANADA']=historico_aux['COMISION DE REASEGURO LOCAL ANUALIZADA']*historico_aux['FACTOR NO DEVENGADO']*historico_aux['FACTOR UF']
        # Primas del mes
        historico_aux['PRIMA DIRECTA ANUALIZADA DEL MES']=np.where(historico_aux['PERIODO INICIO VIGENCIA']==periodo,historico_aux['PRIMA DIRECTA ANUALIZADA'],0)
        historico_aux['PRIMA REASEGURO ANUALIZADA DEL MES']=np.where(historico_aux['PERIODO INICIO VIGENCIA']==periodo,historico_aux['PRIMA REASEGURO ANUALIZADA'],0)
        historico_aux['PRIMA CEDIDA ANUALIZADA DEL MES']=np.where(historico_aux['PERIODO INICIO VIGENCIA']==periodo,historico_aux['PRIMA CEDIDA ANUALIZADA'],0)
        historico_aux['COMISION DE REASEGURO ANUALIZADA DEL MES']=np.where(historico_aux['PERIODO INICIO VIGENCIA']==periodo,historico_aux['COMISION DE REASEGURO ANUALIZADA'],0)
        historico_aux['COMISION DE REASEGURO LOCAL ANUALIZADA DEL MES']=np.where(historico_aux['PERIODO INICIO VIGENCIA']==periodo,historico_aux['COMISION DE REASEGURO LOCAL ANUALIZADA'],0)
        # Devoluciones del mes
        historico_aux['DEVOLUCION PRIMA DIRECTA']=-historico_aux['PRIMA DIRECTA ANUALIZADA']*historico_aux['FACTOR DEVOLUCION']
        historico_aux['DEVOLUCION PRIMA REASEGURO']=-historico_aux['PRIMA REASEGURO ANUALIZADA']*historico_aux['FACTOR DEVOLUCION']
        historico_aux['DEVOLUCION PRIMA CEDIDA']=-historico_aux['PRIMA CEDIDA ANUALIZADA']*historico_aux['FACTOR DEVOLUCION']
        historico_aux['DEVOLUCION COMISION DE REASEGURO']=-historico_aux['COMISION DE REASEGURO ANUALIZADA']*historico_aux['FACTOR DEVOLUCION']
        historico_aux['DEVOLUCION COMISION DE REASEGURO LOCAL']=-historico_aux['COMISION DE REASEGURO LOCAL ANUALIZADA']*historico_aux['FACTOR DEVOLUCION']
        # GUARDO COLUMNAS QUE QUIERO EN EL REPORTE
        reportes(historico_aux[historico_aux['VIGENTE']==1], 'Reservas', lista_reaseguradores)
        if periodo%100%3 == 0: reportes(historico_aux, 'Auditoria Reservas', lista_reaseguradores)
        return historico_aux
        

def revision_reserva_kfijo():
    # LEO LA BASE DE HISTORICO
    cols_date = ['FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION']
    cols_date_res=['RVAFINRVAH','RVAFINRVAH','RVAFFIRVAH','RVAFECCIEH','RVADETFINIANAH','RVADETFECNACH','RVADETFFINANAH']
    dic_types={'ICAPITAL':float}
    historico=pd.read_csv(ruta_output+'8. Nuevo Historico K-Fijo.txt',sep=separador_output,decimal=decimal_output, date_format='%d-%m-%Y', parse_dates=cols_date, low_memory=False,dtype=dic_types)
    reserva = pd.read_csv(ruta_reservas+'1. Inputs Auxiliares\\Reservas\\'+'Reserva '+contrato+' '+str(periodo)+'.txt', sep=';',decimal='.', date_format='%d-%m-%Y', parse_dates=cols_date_res, low_memory=False,dtype=dic_types)
    reserva.rename(columns={'POLNUM':'POLIZA',	'RVARUTRVAH':'RUT',	'RVASECRVAH':'SECUENCIAL',	'COBCOD':'CODIGO COBERTURA',	'RVAFINRVAH':'FECHA_EFECTO',	'RVAFFIRVAH':'FECHA_VENCIMIENTO','RVACAPRVAH':'ICAPITAL'},inplace=True)
    historico['PERIODO_EFECTO']=historico['FECHA_EFECTO'].dt.year*100+historico['FECHA_EFECTO'].dt.month
    historico['NUEVO VENCIMIENTO']=0
    historico['CRUCE CIERRE']=0
    historico['FUENTE']='HISTORICO'
    historico['VIGENTE'] = np.where(((historico['FECHA_ANULACION']<=fecha_cierre)&(~historico['FECHA_ANULACION'].isnull()))|(historico['FECHA_VENCIMIENTO']<=fecha_cierre)|(historico['FECHA_EFECTO']>fecha_cierre),0,1)
    historico=historico[historico['VIGENTE']==1].copy()
    reserva['NUEVO VENCIMIENTO']=1
    reserva['PERIODO_EFECTO']=reserva['FECHA_EFECTO'].dt.year*100+reserva['FECHA_EFECTO'].dt.month
    reserva['NUEVA VENTA']=1
    reserva['FUENTE']='NV'
    reserva['FEC AUX NA']=0
    reserva['FECHA_ANULACION']=pd.to_datetime(reserva['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
    reserva['LLAVE_RES_1']=np.where(reserva['NUEVA VENTA']==0,'',reserva['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+reserva['RUT'].fillna(0).astype('int64').astype('string')+'_'+reserva['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+reserva['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
    historico['LLAVE_RES_1']=np.where(historico['CRUCE CIERRE']==1,'',historico['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+historico['RUT'].fillna(0).astype('int64').astype('string')+'_'+historico['SECUENCIAL'].fillna(0).astype('int64').astype('string')+'_'+historico['CODIGO COBERTURA'].fillna(0).astype('int64').astype('string'))
    reserva,historico=cruce_dfs(reserva,historico,'LLAVE_RES_1','POLIZA+RUT+SEC+COB')
    reserva['LLAVE_RES_2']=np.where(reserva['NUEVA VENTA']==0,'',reserva['RUT'].astype('int64').astype('string')+'_'+reserva['POLIZA'].astype('int64').astype('string')+'_'+reserva['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+reserva['FECHA_EFECTO'].astype('string')+'_'+reserva['FECHA_VENCIMIENTO'].astype('string'))+'_'+reserva['ICAPITAL'].round(1).astype('int64').astype('string')
    historico['LLAVE_RES_2']=np.where(historico['CRUCE CIERRE']==1,'',historico['RUT'].astype('int64').astype('string')+'_'+historico['POLIZA'].astype('int64').astype('string')+'_'+historico['CODIGO COBERTURA'].astype('int64').astype('string')+'_'+historico['FECHA_EFECTO'].astype('string')+'_'+historico['FECHA_VENCIMIENTO'].astype('string'))+'_'+historico['ICAPITAL'].round(1).astype('int64').astype('string')
    reserva,historico=cruce_dfs(reserva,historico,'LLAVE_RES_2','RUT+POL+COB+FEC_EFECTO+FEC_VENCIMIENTO')
    historico.to_csv('Revision Historico vs Reserva.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)


def revision_reserva_cesantia(df_reservas):
    # LECTURA DE ARCHIVO DE RESERVAS
    cols_date = ['FECHA_EFECTO', 'FECHA_VENCIMIENTO']
    reservas = pd.read_csv(ruta_reservas+'1. Inputs Auxiliares\\Reservas\\'+'Reservas '+contrato+' '+str(periodo)+'.txt', sep=';',decimal='.', date_format='%d-%m-%Y', parse_dates=cols_date, low_memory=False)
    # COPIAS DE SEGURIDAD Y CREACION DE VARIABLES AUXILIARES
    df_aux=df_reservas.copy()
    reservas['CRUCE CIERRE']=0
    reservas['FECHA_ANULACION']=0
    reservas['FECHA_ANULACION']=pd.to_datetime(reservas['FECHA_ANULACION'],format = '%d-%m-%Y', errors='coerce')
    df_aux['NUEVA VENTA']=1
    df_aux['SECUENCIAL']=np.where(df_aux['BASE']=='GES',df_aux['CERTIFICADO'],0)
    # DEFINICION DE QUE CAMPOS SACARÉ DEL CIERRE DE PYTHON PARA AGREGARLOS A LA RESERVA Y LUEGO REVISAR
    campos_extra=['CERTIFICADO','NRO_OPERACION','SSEGURO','FECHA_EFECTO','FECHA_VENCIMIENTO','PRIMA NETA ANUAL CIERRE','UF_EMISION','VIGENTE','FACTOR NO DEVENGADO','FACTOR COMISION','FACTOR UF','RRC DIRECTA','RRC CEDIDA','PRIMA DIRECTA NO GANADA','PRIMA REASEGURO NO GANADA','PRIMA CEDIDA NO GANADA','COMISION DE REASEGURO NO GANADA','COMISION DE REASEGURO LOCAL NO GANADA']
    for campo in campos_extra:
        df_aux.rename(columns={campo:campo+' CIERRE'},inplace=True)
    campos_extra=[campo+' CIERRE' for campo in campos_extra]
    # SEPARO LOS DFS EN PARTE GES Y PARTE IAXIS
    df_ges=df_aux[df_aux['BASE']=='GES'].copy()
    df_iaxis=df_aux[df_aux['BASE']=='IAXIS'].copy()
    reservas_ges=reservas[reservas['BASE']=='GES'].copy()
    reservas_iaxis=reservas[reservas['BASE']=='IAXIS'].copy()
    
    # CRUCES PARA LOS DF DE GES
    df_ges['KEY_1']=np.where(df_ges['NUEVA VENTA']==0,'',df_ges['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_ges['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_ges['SECUENCIAL'].fillna(0).astype('int64').astype('string'))
    reservas_ges['KEY_1']=np.where(reservas_ges['CRUCE CIERRE']==1,'',reservas_ges['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+reservas_ges['RUT'].fillna(0).astype('int64').astype('string')+'_'+reservas_ges['SECUENCIAL'].fillna(0).astype('int64').astype('string'))
    df_ges,reservas_ges=cruce_dfs(df_ges,reservas_ges,'KEY_1','POLIZA+RUT+SEC',campos_extra)
    
    # CRUCES PARA LOS DF DE IAXIS
    # KEY 1
    df_iaxis['KEY_1']=np.where(df_iaxis['NUEVA VENTA']==0,'',df_iaxis['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+df_iaxis['RUT'].fillna(0).astype('int64').astype('string')+'_'+df_iaxis['FECHA_EFECTO CIERRE'].astype('string')+'_'+df_iaxis['FECHA_VENCIMIENTO CIERRE'].astype('string'))
    reservas_iaxis['KEY_1']=np.where(reservas_iaxis['CRUCE CIERRE']==1,'',reservas_iaxis['POLIZA'].fillna(0).astype('int64').astype('string')+'_'+reservas_iaxis['RUT'].fillna(0).astype('int64').astype('string')+'_'+reservas_iaxis['FECHA_EFECTO'].astype('string')+'_'+reservas_iaxis['FECHA_VENCIMIENTO'].astype('string'))
    df_iaxis,reservas_iaxis=cruce_dfs(df_iaxis,reservas_iaxis,'KEY_1','POLIZA+RUT+SEC',campos_extra)
    
    +'_'+df_aux['FECHA_EFECTO'].astype('string')+'_'+df_aux['FECHA_VENCIMIENTO'].astype('string')

    df_aux,reservas=cruce_dfs(df_aux,reservas,'LLAVE_1','SSEGURO+COB',campos_extra)
    

def completa_campo(df,campo_rellenar,campos_agrupar,campo_cero=False):
    # if campo_cero==True: df_sin_valores,df_con_valores=df[df[campo_rellenar].isnull()].copy(),df[~df[campo_rellenar].isnull()].copy()
    # elif campo_cero==False: df_sin_valores,df_con_valores=df[(df[campo_rellenar].isnull())|(df[campo_rellenar]==0)].copy(),df[(~df[campo_rellenar].isnull())&(df[campo_rellenar]>0)].copy()
    df_sin_valores,df_con_valores=df[df[campo_rellenar].isnull()].copy(),df[~df[campo_rellenar].isnull()].copy()
    df_sin_valores.drop(columns=[campo_rellenar],axis=1,inplace=True)
    df_agrupado=df_con_valores[[campo_rellenar]+campos_agrupar].groupby(campos_agrupar, dropna=False).agg('mean').reset_index()
    df_sin_valores=df_sin_valores.merge(df_agrupado,how='left',on=campos_agrupar)
    df_final=pd.concat([df_con_valores,df_sin_valores],axis=0)
    df_agrupado.to_csv(ruta_output+'0. Tabla Agrup '+campo_rellenar+' campos '+'_'.join(campos_agrupar)+'.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    return df_final
    

def completa_campo_total(df,campo_completar,listas_campos_agrupar,campo_cero=False):
    df_aux=df.copy()
    if campo_cero==True: 
        promedio_general=df_aux[~df_aux[campo_completar].isnull()][campo_completar].mean()
        df_aux[campo_completar+'_FINAL']=df_aux[campo_completar]
    elif campo_cero==False: 
        promedio_general=df_aux[(~df[campo_completar].isnull())&(df_aux[campo_completar]>0)][campo_completar].mean()
        df_aux[campo_completar+'_FINAL']=np.where(df_aux[campo_completar]>0,df_aux[campo_completar],np.nan)
    for lista in listas_campos_agrupar:
        df_aux=completa_campo(df_aux,campo_completar+'_FINAL',lista,campo_cero)
    df_aux[campo_completar+'_FINAL']=df_aux[campo_completar+'_FINAL'].fillna(promedio_general)
    return df_aux

        
def corrige_tasas_ges(df_ges_0_0):
    # Defino registros duplicados y no duplicados
    duplicados=df_ges_0_0.loc[df_ges_0_0.duplicated(subset=[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','COD_COB'],keep=False)].copy()
    no_duplicados_ges=df_ges_0_0[~df_ges_0_0.index.isin(duplicados.index)].copy()
    # Creo las tasas agrupadas de los registros duplicados (tomando la tasa promedio)
    tasas_promedio=duplicados[[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','TASA_CRED']].groupby([campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION']).agg('mean').reset_index()
    # Elimino la tasa y periodicidad de los duplicados, y luego elimino duplicados
    duplicados=duplicados.drop(columns=['PERIOD_TASA','TASA_CRED'],axis=1)
    duplicados=duplicados.drop_duplicates()
    # Cruzo ahora con las tasas promedio
    duplicados=duplicados.merge(tasas_promedio,how='left',on=[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION'])
    duplicados['PERIOD_CRED']='M'
    df_final=pd.concat([no_duplicados_ges,duplicados],axis=0)
    return df_final    


def recargos(df, calcula_recargos=1):
    df.rename(columns={'PRIMA REASEGURO':'PRIMA REASEGURO SIN RECARGO'},inplace=True)
    cols_df_final=list(df.columns)+['RECARGO']
    # CALCULOS PARA IAXIS
    cols_date_iaxis=['FECHA_INICIO_RECARGO']
    # recargos_iaxis=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos iAxis '+str(periodo)+'.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False,date_format='%d-%m-%Y',parse_dates=cols_date_iaxis)
    recargos_iaxis=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos iAxis.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False,date_format='%d-%m-%Y',parse_dates=cols_date_iaxis)
    # extraprima_iaxis=recargos_iaxis[(recargos_iaxis['FECHA_INICIO_RECARGO']<=fecha_cierre)&(recargos_iaxis['TIPO_RECARGO']=='Extraprima (tanto por mil)')&(recargos_iaxis['VALOR_RECARGO']>0)][['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS','VALOR_RECARGO']]
    # sobreprima_iaxis=recargos_iaxis[(recargos_iaxis['FECHA_INICIO_RECARGO']<=fecha_cierre)&(recargos_iaxis['TIPO_RECARGO']=='Sobreprima (%)')&(recargos_iaxis['VALOR_RECARGO']>0)][['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS','VALOR_RECARGO']]
    extraprima_iaxis=recargos_iaxis[(recargos_iaxis['TIPO_RECARGO']=='Extraprima (tanto por mil)')&(recargos_iaxis['VALOR_RECARGO']>0)][['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS','VALOR_RECARGO']]
    sobreprima_iaxis=recargos_iaxis[(recargos_iaxis['TIPO_RECARGO']=='Sobreprima (%)')&(recargos_iaxis['VALOR_RECARGO']>0)][['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS','VALOR_RECARGO']]
    extraprima_iaxis.rename(columns={'VALOR_RECARGO':'VALOR_RECARGO_EXTRAPRIMA'},inplace=True)
    sobreprima_iaxis.rename(columns={'VALOR_RECARGO':'VALOR_RECARGO_SOBREPRIMA'},inplace=True)
    df_iaxis=df[df['BASE']=='IAXIS'].copy()
    df_iaxis=df_iaxis.merge(extraprima_iaxis,how='left',on=['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS'])
    df_iaxis=df_iaxis.merge(sobreprima_iaxis,how='left',on=['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS'])
    if calcula_recargos==1:
        df_iaxis['RECARGO']=(df_iaxis['VALOR_RECARGO_SOBREPRIMA'].fillna(0)/100*df_iaxis['PRIMA REASEGURO SIN RECARGO']+df_iaxis['VALOR_RECARGO_EXTRAPRIMA'].fillna(0)/1000*df_iaxis['CAPITAL CEDIDO TOTAL']*1/12)*df_iaxis['PARTICIPACION DEL REASEGURADOR']
        df_iaxis[df_iaxis['RECARGO']>0].to_csv(ruta_output+'3. Recargos iAxis Detalle.csv',sep=';')
    else: df_iaxis['RECARGO']=np.where((df_iaxis['VALOR_RECARGO_SOBREPRIMA'].isnull())&(df_iaxis['VALOR_RECARGO_EXTRAPRIMA'].isnull()),0,1)
    # CALCULOS PARA GES
    # recargos_ges_cr=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos GES Credit '+str(periodo)+'.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
    # recargos_ges_ind=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos GES Individuales '+str(periodo)+'.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
    recargos_ges_cr=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos GES Credit.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
    recargos_ges_ind=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos GES Individuales.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
    df_ges=df[df['BASE']=='GES'].copy()
    df_ges=df_ges.merge(recargos_ges_cr,how='left',left_on=['POLIZA','RUT','CERTIFICADO','CODIGO COBERTURA'],right_on=['POLIZA_T0057','RUT_T0057','SECUENCIAL','CODIGO_COBERTURA'],suffixes=['', '_x'])
    df_ges=df_ges.merge(recargos_ges_ind,how='left',left_on=['POLIZA','RUT','CERTIFICADO','CODIGO COBERTURA'],right_on=['POLIZA','RUT','SECUENCIAL','CODIGO_COBERTURA'],suffixes=['', '_x'])
    if calcula_recargos==1:
        df_ges['RECARGO'] = (np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_ACTIVIDAD'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['SOBREPRIMA_ACTIVIDAD'].fillna(0)/100)+\
                        np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_MEDICO'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['SOBREPRIMA_MEDICO'].fillna(0)/100)+\
                        np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_DEPORTE'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['SOBREPRIMA_DEPORTE'].fillna(0)/100)+\
                        df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['PORCENTAJE_RECARGO'].fillna(0)/100+\
                        np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_EXTRAPRIMA'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['CAPITAL CEDIDO TOTAL']*df_ges['EXTRAPRIMA'].fillna(0)/1000)*1/12)*df_ges['PARTICIPACION DEL REASEGURADOR']
        df_ges[df_ges['RECARGO']>0].to_csv(ruta_output+'3. Recargos GES Detalle.csv',sep=';')
    else: df_ges['RECARGO']=np.where((df_ges['ORIGEN'].isnull())&(df_ges['PORCENTAJE_RECARGO'].isnull()),0,1)
    # CREACION DF_FINAL
    df_final=pd.concat([df_iaxis[cols_df_final],df_ges[cols_df_final]],axis=0)
    if calcula_recargos==1: df_final['PRIMA REASEGURO']=df_final['PRIMA REASEGURO SIN RECARGO']+df_final['RECARGO']
    return df_final
    
    
def licitado_desg_nl(df_5,ruta_salidas):
    cobs_old = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Cobs Reas Desg NL Licitacion')
    prods_ges = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Prod GES Licitacion')
    ramo_reas_final = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Ramo Reas Final Desg NL Licitac')
    nombre_prods = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Nombre Productos Licitacion')
    ramo_reas_otros = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Cobs Reas Otros Licitacion')
    if contrato=='Desgravamen No Licitado':
        df_5=df_5.merge(cobs_old,how='left',on=['CODIGO COBERTURA'])
    elif contrato in ['Digital Klare','K-Fijo','AP + Urgencias Medicas','Multisocios']:
        df_5=df_5.merge(ramo_reas_otros,how='left',on=['POL_PROD','CODIGO COBERTURA'])
    df_5=df_5.merge(nombre_prods,how='left',on=['PRODUCTO','BASE'])
    df_5=df_5.merge(prods_ges,how='left',on=['POLIZA','PRODUCTO'])
    df_5['PRODUCTO GES']=np.where(df_5['PRODUCTO GES'].isnull(),df_5['PRODUCTO'],df_5['PRODUCTO GES'])
    df_5['RAMO REAS CORREGIDO']=np.where(('DESG' not in df_5['NOMBRE PRODUCTO'])&(df_5['RAMO REAS']=='DESGRAVAMEN'),'VIDA',df_5['RAMO REAS'])
    df_5=df_5.merge(ramo_reas_final,how='left',on=['TIPO_POLIZA_LETRA','RAMO REAS CORREGIDO'])
    if contrato!='K-Fijo':campos=['RUT','SEXO','FEC_NAC','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO COBERTURA IAXIS','PLAN','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','ICAPITAL','PRIMA NETA ANUAL','FORMA_PAGO_CODIGO','BASE','TIPO_POLIZA_LETRA','CODIGO COBERTURA','EDAD INGRESO','EXPOSICION MENSUAL','TIPO ASEGURADO','EDAD RENOVACION','MESES RENTA','MONTO ASEGURADO','CONTRATO REASEGURO','COBERTURA DEL CONTRATO','CAPITAL RETENIDO TOTAL','CAPITAL CEDIDO TOTAL','PORCENTAJE CEDIDO FINAL','RAMO REAS','RAMO REAS CORREGIDO','COB REAS','PRODUCTO GES','RAMO REAS FINAL','NOMBRE PRODUCTO','RECARGO']
    else: campos=['RUT','SEXO','FEC_NAC','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO COBERTURA IAXIS','PLAN','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','ICAPITAL','PRIMA NETA ANUAL','FORMA_PAGO_CODIGO','BASE','TIPO_POLIZA_LETRA','CODIGO COBERTURA','EDAD INGRESO','EXPOSICION MENSUAL','EDAD RENOVACION','MONTO ASEGURADO','CONTRATO REASEGURO','COBERTURA DEL CONTRATO','CAPITAL RETENIDO TOTAL','CAPITAL CEDIDO TOTAL','PORCENTAJE CEDIDO FINAL','RAMO REAS','RAMO REAS CORREGIDO','COB REAS','PRODUCTO GES','RAMO REAS FINAL','NOMBRE PRODUCTO','RECARGO']
    df_5[campos].to_csv(ruta_salidas+'Detalle Licitacion '+contrato+'.txt',sep=';',decimal=',',date_format='%d-%m-%Y',index=False)
    
        
    sum(df_5['RAMO REAS'].isnull())
    sum(df_5['LOB'].isnull())
    sum(df_5['PRODUCTO GES'].isnull())
    sum(df_5['RAMO REAS FINAL'].isnull())
    
    
    
def encuentra_lob(df):
    df_iaxis=df[df['BASE']=='IAXIS'].copy()
    df_ges=df[df['BASE']=='GES'].copy()
    df_iaxis=df_iaxis.merge(nombre_prods[['PRODUCTO','BASE','LOB']],how='left',on=['PRODUCTO','BASE'])
    df_ges=df_ges.merge(lob_ges,how='left',on=['TIPO','POL_PROD'])
    df=pd.concat([df_iaxis,df_ges],axis=0)
    return df
        
    
def multi_operations(df,campo_1,campo_2,campo_salida,lista_reaseguradores,operation):
    for reasegurador in lista_reaseguradores:
        ejecuta_operacion=check_campo(df,campo_1+'_'+reasegurador)
        ejecuta_operacion=ejecuta_operacion*check_campo(df,campo_2+'_'+reasegurador)            
        if ejecuta_operacion==1:
            if operation=='SUMAR': df[campo_salida+'_'+reasegurador] = df[campo_1+'_'+reasegurador] + df[campo_2+'_'+reasegurador]
            elif operation=='RESTAR': df[campo_salida+'_'+reasegurador] = df[campo_1+'_'+reasegurador] - df[campo_2+'_'+reasegurador]
            elif operation=='MULTIPLICAR': df[campo_salida+'_'+reasegurador] = df[campo_1+'_'+reasegurador] * df[campo_2+'_'+reasegurador]
            elif operation=='DIVIDIR': df[campo_salida+'_'+reasegurador] = df[campo_1+'_'+reasegurador] / df[campo_2+'_'+reasegurador]
            elif operation=='POTENCIA': df[campo_salida+'_'+reasegurador] = df[campo_1+'_'+reasegurador] ** df[campo_2+'_'+reasegurador]
            else : escribe_reporta(archivo_reporte,'La operacion descrita no se encuentra')

def check_campo(df,campo):
    if campo not in df.columns:
        escribe_reporta(archivo_reporte,'El campo {} no se encuentra en el dataframe'.format(campo))
        return 0
    else: return 1            


def cruce_left(df_1, df_2, left_on, right_on, suffixes=('_df1', '_df2'), informa_no_cruces=1 ,name = '', ruta_output=ruta_output):

    # Realizar merge, especificando las columnas de fusión con left_on y right_on
    # Los sufijos _df1 y _df2 se agregan a los nombres de las columnas para diferenciar las columnas originales de cada DataFrame.
    merged_df = pd.merge(df_1, df_2, how='left', right_on=right_on, left_on=left_on, suffixes=('_df1', '_df2'), indicator='origen')

    # Encuentra los registros en df_1 que no cruzaron con df_2 
    no_cruces = merged_df[merged_df['origen'] == 'left_only']

    if no_cruces.empty:
        informa_no_cruces = 0
    else:
        informa_no_cruces = 1

    # Informar en archivo.txt si es necesario
    if informa_no_cruces == 1:
        # Guarda esos registros en un archivo no_cruces.txt en formato de texto tabular (separado por tabuladores).
        print(f'Una cantidad de {no_cruces.shape[0]} registros no cruzaron')
        print(no_cruces[left_on].drop_duplicates(keep='first'))
        no_cruces.to_csv(f'{ruta_output}{name} no cruces.txt', index=False,sep=separador_output,decimal=decimal_output)
    if len(merged_df) > len(df_1) :
        # Identificar duplicados en merged_df que no estaban en df_1
        duplicados_df_2 =df_2[df_2.duplicated(subset=right_on, keep=False)]
        df_1_sin_duplicados=df_1.drop_duplicates(keep='first')
        duplicados_comunes = duplicados_df_2.merge(df_1_sin_duplicados, how='inner', left_on=right_on, right_on=left_on)


        duplicados_comunes[df_2.columns].to_csv(f'{ruta_output}{name} duplicados.txt',sep=separador_output,decimal=decimal_output)
        print("Registros duplicados:")
        print(duplicados_comunes[df_2.columns])
    else:
        print(f"No hay duplicados adicionales. Tamaño original de df_1: {len(df_1)}")
    merged_df = merged_df.drop(['origen'], axis = 1)
    return merged_df


def cross_new_elements(df_anterior, df_actual, campos_comparar, ruta_output):
    # Verificar si los campos de comparación están presentes en ambos dataframes
    for campo in campos_comparar:
        if campo not in df_anterior.columns:
            raise ValueError(f'El campo "{campo}" no está presente en el dataframe anterior.')
        if campo not in df_actual.columns:
            raise ValueError(f'El campo "{campo}" no está presente en el dataframe actual.')
            
      # Eliminar elementos duplicados en cada dataframe y dejar solamente la primera ocurrencia de cada duplicado
    nuevos_productos_anterior = df_anterior.drop_duplicates(subset=campos_comparar, keep='first')
    nuevos_productos_actual = df_actual.drop_duplicates(subset=campos_comparar, keep='first')
    
    nuevos_productos_anterior['Origen df']='Previo'
    nuevos_productos_actual['Origen df']='Actual'
    # Concatenar y eliminar duplicados para identificar nuevos elementos
    nuevos_productos = pd.concat([nuevos_productos_anterior, nuevos_productos_actual]).drop_duplicates(subset=campos_comparar, keep=False)

    # Imprimir la cantidad de nuevas combinaciones
    cantidad_nuevas_combinaciones = len(nuevos_productos)
    if cantidad_nuevas_combinaciones > 0: escribe_reporta(archivo_reporte,'Hay'+str(cantidad_nuevas_combinaciones)+ ' nuevas combinaciones encontradas.')
    # else:
        # print('No hay nuevas combinaciones.')
        
    # Guardar resultados en un archivo de salida (formato txt)
    nuevos_productos_sin_duplicados = nuevos_productos.drop_duplicates(subset=campos_comparar)
    nuevos_productos_sin_duplicados[campos_comparar+['Origen df']].to_csv(ruta_output + '/nuevos_productos.txt', index=False, sep=separador_output, decimal=decimal_output)


def reportes(df,instancia,lista_reaseguradores):
    salidas=pd.read_excel(io=ruta_extensa+archivo_calculos,sheet_name='Salidas')
    filtro = (salidas['TIPO CALCULO'] == tipo_calculo) & (salidas['CONTRATO'] == contrato) & (salidas['INSTANCIA'] == instancia) & (salidas['APLICA'] == 1)
    salidas=salidas[filtro]
    salidas=salidas.replace(np.nan, '', regex=True)
    if salidas.empty:
        print('No se encontraron reportes para esta instancia y condiciones.')
    count=1
    nombres_vistos=set()
    for index,row in salidas.iterrows():
        nombre_salida=row['NOMBRE SALIDA']
        stack=row['STACK']
        cols_groupby,cols_agg=cols_reportes(row['COLS_GROUPBY'],row['COLS_GROUPBY_REASEG'],row['COLS_AGG'],row['COLS_AGG_REASEG'],lista_reaseguradores)
        cols_groupby= cols_groupby if cols_groupby else list(df.columns)
        if not nombre_salida:
            nombre_salida=f'Reporte {count}'
        if nombre_salida in nombres_vistos:
            print(f"Advertencia_el nombre de salida {nombre_salida} se repite.")
        nombres_vistos.add(nombre_salida)
        crea_reporte(df,cols_groupby,nombre_salida,stack,cols_agg)
        count+=1


def crea_reporte(df,cols_groupby,nombre_archivo,stack,cols_agg=''):
    #Verificar si las columnas existen en el dataframe
    cols_faltantes = [col for col in cols_groupby + list(cols_agg) if col not in df.columns]
    if cols_faltantes:
        # raise ValueError(f"Las siguientes columnas no se encuentran en el DataFrame: {', '.join(cols_faltantes)}")
        escribe_reporta(archivo_reporte,f"Las siguientes columnas no se encuentran en el DataFrame: {', '.join(cols_faltantes)}")
    #Determinar el tipo de reporte
    tipo_reporte='groupby' if cols_agg else 'extract'
    if tipo_reporte == 'groupby':
        if stack==0: reporte=df.groupby(cols_groupby,dropna=False)[cols_agg].agg('sum').reset_index()
        else: reporte=df.groupby(cols_groupby,dropna=False)[cols_agg].agg('sum').stack().reset_index()
    else:
        reporte=df[cols_groupby]
    #Determinar el nombre del archivo
    extension='csv' if tipo_reporte == 'groupby' else 'txt'
    if isinstance(nombre_archivo, int):
        nombre_archivo=f'Reporte Nro {nombre_archivo}.{extension}'
    else:
        nombre_archivo=f'{nombre_archivo}.{extension}'
    #Guardar el reporte
    reporte.to_csv(ruta_output+nombre_archivo,sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        
    print(f'Archivo {nombre_archivo} generado correctamente.')
    
    
def cols_reportes(columnas_groupby,columnas_groupby_reaseg,columnas_agg,columnas_agg_reaseg,lista_reaseguradores):
    cols_groupby=columnas_groupby.split(',') if columnas_groupby else []
    cols_agg=columnas_agg.split(',') if columnas_agg else []
    cols_groupby_reaseg=columnas_groupby_reaseg.split(',') if columnas_groupby_reaseg else []
    cols_agg_reaseg=columnas_agg_reaseg.split(',') if columnas_agg_reaseg else []
    if cols_groupby_reaseg: cols_groupby=cols_groupby+lista_campos_reaseguradores(cols_groupby_reaseg,lista_reaseguradores)
    if cols_agg_reaseg: cols_agg=cols_agg+lista_campos_reaseguradores(cols_agg_reaseg,lista_reaseguradores)
    return cols_groupby,cols_agg
    

def cols_instancia(instancia,lista_reaseguradores):
    salidas=pd.read_excel(io=ruta_extensa+archivo_calculos,sheet_name='Salidas')
    filtro = (salidas['TIPO CALCULO'] == tipo_calculo) & (salidas['CONTRATO'] == contrato) & (salidas['INSTANCIA'] == instancia) & (salidas['APLICA'] == 1)
    salidas=salidas[filtro]
    salidas=salidas.replace(np.nan, '', regex=True)
    cols_groupby=[]
    cols_agg=[]
    for index,row in salidas.iterrows():
        cols_groupby_aux,cols_agg_aux=cols_reportes(row['COLS_GROUPBY'],row['COLS_GROUPBY_REASEG'],row['COLS_AGG'],row['COLS_AGG_REASEG'],lista_reaseguradores)
        cols_groupby=cols_groupby+cols_groupby_aux
        cols_agg=cols_agg+cols_agg_aux
    return list(set(cols_groupby+cols_agg))     
    

def crea_siniestros_mes(df, crea_historico) -> pd.DataFrame():
    if contrato == 'Vida':
        date_cols=['FECHA CIERRE MES','FECHA DE NACIMIENTO','FECHA DE DEFUNCION','FECHA_SINIESTRO','FECHA_DENUNCIO','FECHA_PAGO_SANTANDER','INICIO_VIGENCIA','FECHA_TERMINO_VIGENCIA','FECHA_VENCIMIENTO','FECHA CRUCE VIGENCIAS','INICIO DEL CONTRATO','FECHA INICIO CONTRATO','FECHA FIN CONTRATO']
        historico_pagados = pd.read_csv(f'{ruta_historico_input}1. Inputs Auxiliares\\Historicos\\Historico Siniestros Pagados Vida {periodo_anterior}.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',parse_dates=date_cols,date_format='%d-%m-%Y',low_memory=False)
        historico_pendientes = pd.read_csv(f'{ruta_historico_input}1. Inputs Auxiliares\\Historicos\\Historico Siniestros Pendientes Vida {periodo_anterior}.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',parse_dates=date_cols,date_format='%d-%m-%Y',low_memory=False)
        # CODIGO PARA CORREGIR EL HISTORICO (CAMBIAR NOMBRES Y ELIMINAR ALGUNAS COLUMNAS INNECESARIAS)
        # historico_pagados=historico_pagados.drop(columns=['NOMBRE_PRODUCTO','ENTIDAD','TIPO_POLIZA'])
        # historico_pendientes=historico_pendientes.drop(columns=['NOMBRE_PRODUCTO','ENTIDAD','TIPO_POLIZA','PERIODICIDAD','POL_PROD'])
        # historico_pagados.columns=df.columns
        # historico_pendientes.columns=df.columns
        # historico_pagados.to_csv(f'{ruta_historico_input}1. Inputs Auxiliares\\Historicos\\Historico Siniestros Pagados Vida 202210.txt',sep=';',decimal='.',encoding='latin-1',date_format='%d-%m-%Y',index=False)
        # historico_pendientes.to_csv(f'{ruta_historico_input}1. Inputs Auxiliares\\Historicos\\Historico Siniestros Pendientes Vida 202210.txt',sep=';',decimal='.',encoding='latin-1',date_format='%d-%m-%Y',index=False)
        if crea_historico == 1:
            pd.concat([historico_pagados,df[df['ESTADO SINIESTRO']=='PAGADO']]).to_csv(f'{ruta_historico_input}1. Inputs Auxiliares\\Historicos\\Historico Siniestros Pagados Vida {periodo}.txt',sep=';',decimal='.',encoding='latin-1',date_format='%d-%m-%Y',index=False)
            pd.concat([historico_pendientes,df[df['ESTADO SINIESTRO']=='PENDIENTE']]).to_csv(f'{ruta_historico_input}1. Inputs Auxiliares\\Historicos\\Historico Siniestros Pendientes Vida {periodo}.txt',sep=';',decimal='.',encoding='latin-1',date_format='%d-%m-%Y',index=False)
        contrados_cumulos = set(list(cumulos_individuales['CONTRATO REASEGURO'])+list(cumulos_contrato['CONTRATO REASEGURO'])+list(cumulos_excedente['CONTRATO REASEGURO'])+list(cumulos_individuales_siniestros['CONTRATO REASEGURO'])+list(cumulos_excedente_siniestros['CONTRATO REASEGURO']))
        return pd.concat([historico_pagados[historico_pagados['CONTRATO REASEGURO'].isin(contrados_cumulos)],df[df['CONTRATO REASEGURO'].notnull()]])
    if contrato == 'Generales':
        return df
    
    
def aplica_pago_siniestros(df, recalcula_aplica=0):
    if contrato=='Vida':
        df['TIPO_PAGO'] = df['TIPO_PAGO'].str.upper()
        if recalcula_aplica ==0: df_aux = df[df['APLICA_PAGO'].isnull()].copy()
        elif recalcula_aplica ==1: df_aux = df.copy()
        df_aux = df_aux.drop(labels=['APLICA_PAGO'],axis=1)
        df_aux=df_aux.merge(tipos_pago,how='left',on=['TIPO_PAGO','CONTRATO REASEGURO'])
        if sum(df_aux['APLICA_PAGO'].isnull())>0:
            escribe_reporta(archivo_reporte,f'Cruce de APLICA_PAGO no encontró llave para todos los registros. Quedaron {sum(df_aux["APLICA_PAGO"].isnull())} registros afuera')
            df_aux[df_aux['APLICA_PAGO'].isnull()][['TIPO_PAGO','CONTRATO REASEGURO']].drop_duplicates().to_csv(ruta_output+'Tipos de Pago por Parametrizar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
            for index, row in df_aux[df_aux['APLICA_PAGO'].isnull()][['TIPO_PAGO','CONTRATO REASEGURO']].drop_duplicates().iterrows(): 
                print(f'{row["TIPO_PAGO"]}-{row["CONTRATO REASEGURO"]}')
            sys.exit()
        if recalcula_aplica ==0: return pd.concat([df_aux[df_aux['APLICA_PAGO']==1],df[df['APLICA_PAGO']==1]])
        elif recalcula_aplica ==1: return df_aux[df_aux['APLICA_PAGO']==1]
    if contrato=='Generales':
        df['APLICA_PAGO']=1
        return df
    
    
def exportar_casos(lista_filtrar,df_python,df_vs,campos_python,campo_vs=''):
    if campo_vs=='': campo_vs=campos_python
    for i in range(len(campos_python)):
        if i==0: df_aux_python=df_python
        if i==0: df_aux_vs=df_vs
        df_aux_python=df_aux_python[df_aux_python[campos_python[i]].isin(lista_filtrar[i])]
        df_aux_vs=df_aux_vs[df_aux_vs[campo_vs[i]].isin(lista_filtrar[i])]
    df_aux_python.to_csv(ruta_output+'df python revisar casos.csv',sep=separador_output,decimal=decimal_output,encoding='latin-1',date_format='%d-%m-%Y',index=False)
    df_aux_vs.to_csv(ruta_output+'df vs revisar casos.csv',sep=separador_output,decimal=decimal_output,encoding='latin-1',date_format='%d-%m-%Y',index=False)


def pivotear_campos(df,cols_iniciales,cols_agrupar):
        df_aux=df.pivot(index=cols_iniciales, columns='REASEGURADOR')[cols_agrupar].reset_index()
        cols=[]
        for i,j in df_aux.columns:
            if j=='':cols.append(i)
            else:cols.append(f'{i}_{j}')
        df_aux.columns=cols
        return df_aux

def suma_columnas_reaseguradores(df,columna_like,lista_reaseguradores,elimina_detalle=False):
    df_aux=df.copy()
    cols_sumar=[]
    for columna_df in df_aux.columns:
        for nombre_reasegurador in lista_reaseguradores:
            if (columna_like+'_'+nombre_reasegurador in columna_df):
                cols_sumar.append(columna_df)
    cols_sumar=list(set(cols_sumar))
    df_aux[columna_like]=df[cols_sumar].sum(axis=1)
    if elimina_detalle==True:
        cols_eliminar=[]
        for nombre_reasegurador in lista_reaseguradores:
            cols_eliminar.append(columna_like+'_'+nombre_reasegurador)
        df_aux.drop(cols_eliminar,axis=1,inplace=True)
    return df_aux


# df=pd.concat([df,df_sin_reaseguro])
def salida_resultados(df,tipo_calculo,cols_iniciales):
    # Crea directorio
    Path(ruta_output).mkdir(parents=True, exist_ok=True)
    # Extraemos todas las salidas que queramos para tirar en csv y para comparar con el df de actuariado y para pivotear el df
    pivotes=pd.read_excel(io=ruta_extensa+archivo_calculos,sheet_name='Pivotes')
    pivotes=pivotes[(pivotes['TIPO CALCULO']==tipo_calculo)&(pivotes['CONTRATO']==contrato)].reset_index(drop=True)
    matriz_comparaciones=pd.read_excel(io=ruta_extensa+archivo_calculos,sheet_name='Compara Resultados')
    matriz_comparaciones=matriz_comparaciones[(matriz_comparaciones['CONTRATO']==tipo_calculo)&(matriz_comparaciones['SUBCONTRATO']==contrato)].reset_index(drop=True)
    # Creacion de variables auxiliares
    lista_reaseguradores=list(df['REASEGURADOR'].unique())
    ejecuta_comparaciones=1
    # Comparacion de resultados
    escribe_reporta(archivo_reporte,'{} Comparacion de resultados'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    try: base_compara=pd.read_csv(ruta_input+subcarpeta_compara+'\\'+archivo_compara,sep=separador_compara,decimal = decimal_compara,encoding='latin-1',low_memory=False)
    except: ejecuta_comparaciones=0
    # Ciclo para hacer todas las comparaciones entre los calculos de Actuariado vs Python
    if (ejecuta_comparaciones==1)&~(matriz_comparaciones.empty):
        for index, row in matriz_comparaciones.iterrows():
            campos_agrupacion_df1=row['APERTURA DF_1'].split(',')
            campos_agrupacion_df2=row['APERTURA DF_2'].split(',')
            cumulo_df1=row['RESULTADO DF_1']
            cumulo_df2=row['RESULTADO DF_2']
            compara_resultados(df,base_compara,campos_agrupacion_df1,cumulo_df1,campos_agrupacion_df2,cumulo_df2)
    # Pivoteamos el df de resultados
    if (len(lista_reaseguradores)>1)&(pivotes.shape[0]==1)&(pivotea_df==1):
        print('{} Pivoteo del df'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
        cols_pivotear=pivotes.loc[0,'COLS PIVOTEAR'].split(',')
        try:
            ##### FORMA NUEVA DE PIVOTEAR
            df_salida=pd.DataFrame()
            for cob in list(df['CODIGO COBERTURA'].unique()):
                    print('corriendo cobertura {}'.format(cob))
                    df_aux_cob=df[df['CODIGO COBERTURA']==cob].copy()
                    df_salida_aux=pivotear_campos(df_aux_cob[cols_iniciales+['REASEGURADOR']+cols_pivotear], cols_iniciales, cols_pivotear)
                    df_salida=pd.concat([df_salida,df_salida_aux])
            for col_sum in cols_pivotear:
                    df_salida=suma_columnas_reaseguradores(df_salida,col_sum,lista_reaseguradores,False)
            
        except:
            escribe_reporta(archivo_reporte,'No se pudo pivotear df final de python. Contiene duplicados o problemas de memoria')
            df_salida=df.copy()
    else: df_salida=df.copy()
    # Calculos de BDX
    print('{} Calculos de BDX'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    df_bdx = calculos_reporteria(df_salida, tipo_calculo, contrato)
    if df_bdx.shape[0]!=df_salida.shape[0]: escribe_reporta(archivo_reporte, f'El archivo df_bdx y el df_salida no coinciden en sus filas. REVISAR!\n Existe una diferencia de {df_bdx.shape[0]-df_salida.shape[0]} entre BDX y Salida')
    # Reportes de BDX
    escribe_reporta(archivo_reporte, '{} Reportes'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    reportes(df_bdx,'Reaseguro',lista_reaseguradores)
    return df_bdx


def calculos_reporteria(df,tipo_calculo,contrato):
    # Calculo de df con detalle para el o los reaseguradores
    if ('Incendio y Sismo' in contrato):
        df['FECHA PROCESO']=fecha_inicio_mes
        df['FECHA TERMINO']=np.where(df['FECHA_ANULACION'].isnull(),df['FFIN_RENOV_ANUAL'],df['FECHA_ANULACION'])
        df['VIGENTE']=np.where(df['FECHA TERMINO']<=fecha_cierre,0,1)
        df=df.merge(cobs_ges[['COB_GES','NOMBRE COBERTURA']].drop_duplicates(),how='left',left_on=['CODIGO COBERTURA'],right_on=['COB_GES'])
        df['MONTO ASEGURADO CON COASEGURO']=df['MONTO ASEGURADO']*df['COASEGURO']
        df['PRIMA NETA ANUAL']=df['PRIMA NETA ANUAL'].fillna(0)
        df['PRIMA NETA MENSUAL']=df['PRIMA NETA ANUAL']/12
        df['PRIMA NETA MENSUAL EXPUESTA']=df['PRIMA NETA MENSUAL']*df['EXPOSICION MENSUAL']
        df['PRIMA CEDIDA']=df['PRIMA NETA MENSUAL']*df['PORCENTAJE CEDIDO FINAL']*df['EXPOSICION MENSUAL']
        df['PRIMA CEDIDA EXPUESTA']=df['PRIMA NETA MENSUAL']*df['PORCENTAJE CEDIDO FINAL']*df['EXPOSICION MENSUAL']
        df['COMISION DE REASEGURO']=df['PRIMA CEDIDA EXPUESTA']-df['PRIMA REASEGURO']
        df['PRIMA NETA MENSUAL CON COASEGURO']=df['PRIMA NETA MENSUAL']*df['COASEGURO']
        df['MONTO ASEGURADO EXPUESTO']=df['MONTO ASEGURADO']*df['EXPOSICION MENSUAL']*df['COASEGURO']
        df['MONTO ASEGURADO AL CIERRE']=df['MONTO ASEGURADO']*df['VIGENTE']
        df=df.merge(cumulos_is,how='left',on=['CODIGO COBERTURA'])
        df=df.merge(lob_primas,how='left',on=['POL_PROD'])
        df['NRO REGISTROS']=1
        return df
    elif clasificacion_contrato =='Cesantia PU':
        df['RANGO CAPITAL']='De '+df['CAPITAL BASE 50'].astype('string')+' a '+(df['CAPITAL BASE 50']+50).astype('string')
        df['CAPITAL MAX']=df['CAPITAL BASE 50']+50
        df['CEDIDO']='Cedido'
        df=df.merge(contratos_cesantia,how='left',on=['CONTRATO REASEGURO','VIGENCIA CONTRATO'])
        df=df.merge(periodos_bdx_cesantia,how='left',on=['CONTRATO SISTEMA','CONTRATO NICKNAME'])
        df['INI_VIG'] = df['FECHA_EFECTO']- np.where(df['FECHA_EFECTO'].dt.day==1,pd.offsets.MonthBegin(0),pd.offsets.MonthBegin(1))
        df['MARCA_NB']=np.where(df['INI_VIG']==fecha_inicio_mes,'NB','STOCK')
        df['CIERRE']=fecha_cierre
        df['PRIMA NETA ANUAL CIERRE']=np.where(df['BASE']=='IAXIS',df['PRIMA NETA ANUAL'],df['PRIMA NETA ANUAL']/1.19)
        return df
    elif tipo_contrato=='Vida':
        if contrato=='Desgravamen No Licitado':
            if pivotea_df==0:
                nro_reg_no_aplica=sum(df['APLICA']==0)
                if nro_reg_no_aplica>0: escribe_reporta(archivo_reporte,f'Se eliminaron {nro_reg_no_aplica} registros que no aplica el cálculo de prima de reaseguro')
                df=df[df['APLICA']==1].copy()
            fecha_cierre_reporte=fecha_cierre+pd.offsets.MonthEnd(2)
            df['AÑO']=fecha_cierre_reporte.year
            df['MES']=fecha_cierre_reporte.month
            uf_cierre=tabla_uf[tabla_uf['FECHA_EFECTO']==fecha_cierre_reporte].iloc[0]['UF_EMISION']
            df['PERIODO']=df['AÑO']*100+df['MES']
            df['CAPITAL_PREVIO_MESES_RENTA']=np.where(df['MESES RENTA']==0,df['MONTO ASEGURADO'],df['ICAPITAL'])
        else:
            df['AÑO']=int(periodo/100)
            df['MES']=periodo-fecha_cierre.year*100
            uf_cierre=tabla_uf[tabla_uf['FECHA_EFECTO']==fecha_cierre].iloc[0]['UF_EMISION']
            df['PERIODO']=periodo
        df=df.merge(cobs_ges[['COB_GES','NOMBRE COBERTURA GES']].drop_duplicates(),how='left',left_on=['CODIGO COBERTURA'],right_on=['COB_GES'])
        df=df.merge(nombre_prods[['PRODUCTO','BASE','NOMBRE PRODUCTO']],how='left',on=['PRODUCTO','BASE'])
        df=df.merge(coc_conceptos,how='left',on='TIPO_POLIZA_LETRA')
        df=df.merge(coc_institucion,how='left',on=['TIPO_POLIZA_LETRA','POL_PROD'])
        df=df.merge(negocio,how='left',on=['TIPO_POLIZA_LETRA'])
        if (pivotea_df==0):    
            df=df.merge(coc_reaseguradores,how='left',on=['REASEGURADOR'])
            df=df.merge(coc_contratos,how='left',on=['REASEGURADOR','CONTRATO REASEGURO'])
        df['TIPO']=np.where(df['TIPO_POLIZA_LETRA']=='I','IND','COL')
        # df['PROD_POL_BC']=np.where(df['TIPO_POLIZA_LETRA']=='C',df['PRODUCTO'].astype(int).astype(str)+'_'+df['POLIZA'].astype(int).astype(str),df['PRODUCTO'].astype(int).astype(str))
        df=df.merge(fecu_vida,how='left',on=['TIPO_POLIZA_LETRA','POL_PROD','CODIGO COBERTURA'])
        df['PRIMA REASEGURO CLP']=df['PRIMA REASEGURO']*uf_cierre
        if tipo_prima=='Prima Recurrente':df['PRIMA NETA MENSUAL']=df['PRIMA NETA ANUAL']/12
        if tipo_prima=='Prima Unica':df['PRIMA NETA']=df['PRIMA NETA ANUAL']
        df['FORMA PAGO LETRA']=np.where(df['FORMA_PAGO_CODIGO']==1,'A',np.where(df['FORMA_PAGO_CODIGO']==12,'M',np.where(df['FORMA_PAGO_CODIGO']==0,'U',np.where(df['FORMA_PAGO_CODIGO']==2,'S',np.where(df['FORMA_PAGO_CODIGO']==4,'P','REVISAR')))))
        df['NRO_REGISTROS']=1
        df['FECHA TERMINO']=np.where(df['FECHA_ANULACION'].isnull(),df['FFIN_RENOV_ANUAL'],df['FECHA_ANULACION'])
        if contrato=='Desgravamen No Licitado': df['CRUZA SI']=np.where(df['SALDO_INSOLUTO'].fillna(0)>0,1,0)*df['APLICA CALCULO SALDO INSOLUTO'].fillna(0)
        if contrato=='Desgravamen No Licitado': df['ENCUENTRA TASA_CRED']=np.where(df['TASA_CRED'].fillna(0)>0,1,0)*df['APLICA CALCULO SALDO INSOLUTO'].fillna(0)*(1-df['CRUZA SI'].fillna(0))
        if contrato =='Multisocios': df['ENCUENTRA TASA_CRED']=np.where(df['TASA_CRED'].fillna(0)>0,1,0)
        if contrato!='Desgravamen No Licitado':
            df['PRIMA RENOVACION']=np.where(df['FECHA_EFECTO']==df['FINI_RENOV_ANUAL'],'Prima de Primer Año','Prima de Renovacion')
        if contrato=='Oncologico UC':
            df['CONTRATO BDX']='Oncologico UC GES'
            df['TITULAR']=1
            df['ADICIONAL']=df['NRO_RIESGOS']-1
        if contrato=='Multisocios':
            df['POL GLOSA']='DESGRAMANE MULTISOCIO'
            df['CMDSC']='Desg. Consumo'
            df['POLASETFA']=1.05
            df['POLSCERFOL']=0            
        if contrato in ['Full Oncologico','Familia Protegida','Hospitalizacion Banefe']:
            df['TITULAR']=1
            df['ADICIONAL']=0
            if contrato=='Full Oncologico': df['LLAVE PLAN']=df['CODIGO COBERTURA'].astype(int).astype(str)+'-'+df['ICAPITAL'].astype(int).astype(str)+'-'+df['PLAN_DESC']+'-'+df['TRAMO EDAD']
            if contrato=='Familia Protegida': df['LLAVE PLAN']=df['CODIGO COBERTURA'].astype(int).astype(str)+'-'+df['PLAN_DESC']+'-'+df['ICAPITAL'].astype(int).astype(str)
            if contrato=='Hospitalizacion Banefe': df['LLAVE PLAN']=df['PLAN_DESC']
            df=df.merge(planes_cardiff,how='left',on=['CONTRATO REASEGURO','LLAVE PLAN'])
            if contrato=='Full Oncologico': df['LLAVE TARIFA']=df['TRAMO EDAD']+'-'+df['PLAN REASEGURO']+'-'+df['CODIGO COBERTURA'].astype(int).astype(str)+'-'+df['ICAPITAL'].astype(int).astype(str)+'-'+df['TIPO ASEGURADO']
            if contrato=='Familia Protegida': df['LLAVE TARIFA']=df['PLAN REASEGURO']+'-'+df['CODIGO COBERTURA'].astype(int).astype(str)+'-'+df['ICAPITAL'].astype(int).astype(str)+'-'+df['TRAMO EDAD']+'-'+df['TIPO ASEGURADO']
            if contrato=='Hospitalizacion Banefe': df['LLAVE TARIFA']=df['PLAN REASEGURO']+'-'+df['CODIGO COBERTURA'].astype(int).astype(str)+'-'+df['ICAPITAL'].astype(int).astype(str)+'-'+df['TIPO ASEGURADO']
            df['LLAVE ONCOLOGICO']=df['PLAN REASEGURO']+' '+np.where(df['TIPO ASEGURADO']=='Titular','Titulares','Adicionales')
            df=df.merge(nombres_cardiff,how='left',on='RUT')
            df=df.merge(lob_ges[lob_ges['TIPO']=='IND'][['POL_PROD','LOB']],how='left',left_on=['PRODUCTO'],right_on=['POL_PROD'],suffixes=['','_CROSS'])
            df['CEDIDO']='Cedido'
            df['PRODUCTO_COBERTURA']=df['PRODUCTO'].astype(int).astype(str)+'-'+df['CODIGO COBERTURA'].astype(int).astype(str)
            df=df.merge(fecu_desc,how='left',on='RAMO FECU')
        if contrato not in ['Full Oncologico','Familia Protegida','Hospitalizacion Banefe']: df=encuentra_lob(df)
        return df
    elif (tipo_calculo=='Siniestros de Reaseguro')&(contrato=='Generales'):
        lob_siniestros=pd.read_csv(f'{ruta_lob}1. Inputs Auxiliares\\LOB\\LOB {periodo}.TXT',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
        df['FECHA CIERRE ACUMULADO'] = fecha_cierre
        df['TIPO_REDUCIDO'] = np.where(df['TIPO1']=='Colectivo','C','I')
        df['SISTEMA'] = np.where(df['SSEGURO'].isnull(),'GES','IAXIS')
        df = df.merge(lob_siniestros,how='left',on=['TIPO_REDUCIDO','POLIZA'])
        df['APLICA CESION'] = np.where(df['CESION QS']>0,'S','N')
        df['CESION QS'] = df['CESION QS'].fillna(0)
        df['MES_SINIESTRO'] = np.where(df['FECHA_SINIESTRO'].dt.day==1,df['FECHA_SINIESTRO'],df['FECHA_SINIESTRO']+ pd.offsets.MonthBegin())
        df['MES_DENUNCIA'] = np.where(df['FECHA_DENUNCIA'].dt.day==1,df['FECHA_DENUNCIA'],df['FECHA_DENUNCIA']+ pd.offsets.MonthBegin())
        df['MES_PAGO'] = np.where(df['FECHA_PAGO'].dt.day==1,df['FECHA_PAGO'],df['FECHA_PAGO']+ pd.offsets.MonthBegin())
        df['FECHA_CIERRE_CONTABLE'] = fecha_cierre
        df['N_SINIESTRO_2'] = df['N_SINIESTRO']
        df['CATASTROFE'] = df['CATASTROFE_DETALLE']
        df['CODIGO COBERTURA 2'] = df['CODIGO COBERTURA']
        df['DIRECTO_ZS_UF'] = df['DIRECTO_TOTAL_UF'] * df['COASEGURO'].fillna(1)
        df['DIRECTO_TOTAL_CLP'] = df['DIRECTO_TOTAL_UF'] * df['TC']
        df['DIRECTO_ZS_CLP'] = df['DIRECTO_ZS_UF'] * df['TC']
        df['CEDIDO_ZS_CLP'] = df['SINIESTRO CEDIDO'] * df['TC']
        df['RETENIDO_ZS_CLP'] = df['SINIESTRO RETENIDO'] * df['TC']
        df['COM_NOMBRE'] = ''
        df['REG_NOMBRE'] = ''
        df['EML_NOMBRE'] = ''
        return df
    elif (tipo_calculo=='Siniestros de Reaseguro')&(contrato=='Vida'):
        df['FECHA CIERRE ACUMULADO'] = fecha_cierre
        return df
    else: return df


def compara_resultados(df_1,df_2,campos_agrupacion_df1,cumulo_df1,campos_agrupacion_df2,cumulo_df2,ruta_output=ruta_output):
    Path(ruta_output).mkdir(parents=True, exist_ok=True)
    # comprobamos que se cumplen todas las condiciones para correr bien la comparacion
    if 'VIGENTE' not in df_2:df_2['VIGENTE DF2']=1
    count=0
    if len(campos_agrupacion_df1)!=len(campos_agrupacion_df2):
        escribe_reporta(archivo_reporte,'listas de campos a evaluar no tienen el mismo largo. REVISAR')
        count=1
    for campo in campos_agrupacion_df1+cumulo_df1:
        if campo not in df_1.columns:
            escribe_reporta(archivo_reporte,'El campo {} no se encuentra dentro del primer dataframe'.format(campo))
            count=1
    for campo in campos_agrupacion_df2+cumulo_df2:
        if campo not in df_2.columns:
            escribe_reporta(archivo_reporte,'El campo {} no se encuentra dentro del segundo dataframe'.format(campo))
            count=1
    if count==1:
        return 
    name_campos_df1=','.join(campos_agrupacion_df1)
    name_cumulos_df1=','.join(cumulo_df1)
    print('Comienza analisis de resultados entre python y actuariado para los campos '+name_campos_df1+' sobre '+name_cumulos_df1)
    df_1['NRO REGISTROS DF1']=1
    df_2['NRO REGISTROS DF2']=1
    df1_grouped=df_1[campos_agrupacion_df1+cumulo_df1+['NRO REGISTROS DF1']].groupby(campos_agrupacion_df1, dropna=False).agg(sum).reset_index()
    df2_grouped=df_2[campos_agrupacion_df2+cumulo_df2+['NRO REGISTROS DF2']].groupby(campos_agrupacion_df2, dropna=False).agg(sum).reset_index()
    df_crossed=df1_grouped.merge(df2_grouped,how='outer',left_on=campos_agrupacion_df1,right_on=campos_agrupacion_df2)
    df_crossed_left=df_crossed[df_crossed['NRO REGISTROS DF2'].isnull()]
    df_crossed_right=df_crossed[df_crossed['NRO REGISTROS DF1'].isnull()]
    df_crossed_both=df_crossed[(~df_crossed['NRO REGISTROS DF1'].isnull())&(~df_crossed['NRO REGISTROS DF2'].isnull())]
    for campo_df1,campo_df2 in zip(cumulo_df1,cumulo_df2):
        print('Resultados sobre el campo {}'.format(campo_df1))
        print('Los registros que se encuentran en el primer dataframe y no en el segundo suman un total de {} de prima de reaseguro'.format(str(round(sum(df_crossed_left[campo_df1]),2))))
        print('Los registros que no se encuentran en el primer dataframe pero sí en el segundo suman un total de {} de prima de reaseguro'.format(str(round(sum(df_crossed_right[campo_df2]),2))))
        print('Los registros que se encuentran en el primer dataframe y también en el segundo suman un total de {} y {} de prima de reaseguro respectivamente'.format(str(round(sum(df_crossed_both[campo_df1]),2)),str(round(sum(df_crossed_both[campo_df2]),2))))
    df_crossed_both.to_csv(ruta_output+'2. Analisis Cross Campo '+name_campos_df1+'.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    df_1.merge(df_crossed_left,how='inner',on=campos_agrupacion_df1).to_csv(ruta_output+'2. Analisis Left Campo '+name_campos_df1+'.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    df_2.merge(df_crossed_right,how='inner',on=campos_agrupacion_df2).to_csv(ruta_output+'2. Analisis Right Campo '+name_campos_df1+'.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # return df_crossed


# Comparacion de resultados rapida
# df_salida=pd.read_csv(ruta_output+'3. Calculos Python '+contrato+' '+str(periodo)+'.csv',sep=';',decimal='.')
# df_iaxis=pd.read_csv(ruta_input+'Salida iAxis '+contrato+' '+str(periodo)+'.txt',sep=';',decimal='.')
# campos_agrupacion_df1=['SSEGURO','CODIGO COBERTURA IAXIS','POLIZA']
# campos_agrupacion_df2=['SSEGURO','COBERTURA','NPOLIZA']
# cumulo_df1='PRIMA REASEGURO'
# cumulo_df2='IPRIMAREA'
# compara_resultados(df_salida,df_iaxis,campos_agrupacion_df1,cumulo_df1,campos_agrupacion_df2,cumulo_df2)
# df_iaxis.to_csv(ruta_output+'3. Calculos Actuariado '+str(periodo)+'.csv',sep=separador_output,decimal=decimal_output)
    
    
def corrige_historicos_siniestros_vida():
    ruta = '1 Input\\Siniestros de Reaseguro\\Cierre\\1. Inputs Auxiliares\\Historicos\\'
    periodo=202210
    periodo_tope=202406
    while periodo<periodo_tope:
        date_cols=['FECHA CIERRE MES','FECHA DE NACIMIENTO','FECHA DE DEFUNCION','FECHA_SINIESTRO','FECHA_DENUNCIO','FECHA_PAGO_SANTANDER','INICIO_VIGENCIA','FECHA_TERMINO_VIGENCIA','FECHA_VENCIMIENTO','FECHA CRUCE VIGENCIAS','INICIO DEL CONTRATO','FECHA INICIO CONTRATO','FECHA FIN CONTRATO']
        historico_pagados = pd.read_csv(f'{ruta}Historico Siniestros Pagados Vida {periodo}.txt',sep=';',decimal='.',encoding='latin-1',parse_dates=date_cols,date_format='%d-%m-%Y',low_memory=False)
        historico_pagados.info()
        historico_pagados_202210 = historico_pagados[historico_pagados['FECHA CIERRE MES']==datetime.datetime(2022,10,31)].copy()
        historico_pagados_202210_nulls = historico_pagados_202210[historico_pagados_202210['FECHA DE NACIMIENTO'].isnull()].copy()
        historico_pagados_202210_notnulls = historico_pagados_202210[historico_pagados_202210['FECHA DE NACIMIENTO'].notnull()].copy()
        historico_pagados_resto = historico_pagados[historico_pagados['FECHA CIERRE MES']!=datetime.datetime(2022,10,31)].copy()
        historico_pagados_202210_nulls['EDAD SINIESTRO']=edad_casos_perdidos
        historico_pagados_202210_notnulls['EDAD SINIESTRO']=((historico_pagados_202210_notnulls['FECHA_SINIESTRO']-historico_pagados_202210_notnulls['FECHA DE NACIMIENTO']).dt.days/365.25).astype('int')
        historico_pagados_corregido = pd.concat([historico_pagados_202210_nulls,historico_pagados_202210_notnulls,historico_pagados_resto])
        historico_pagados_corregido.to_csv(f'{ruta}Historico Siniestros Pagados Vida {periodo}.txt',sep=';',decimal='.',encoding='latin-1',date_format='%d-%m-%Y',index=False)
        print(f'{periodo} - {sum(historico_pagados_corregido["EDAD SINIESTRO"].isnull())} registros con edad siniestro nulo')
        periodo = periodo + (89 if periodo%100==12 else 1)