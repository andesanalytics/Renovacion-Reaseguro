"""
Este script contiene todas las funciones externas que ayudan ya sea a preprocesar la data o a calcular el reaseguro
"""

# Importamos librerias que vamos a utilizar
import pandas as pd
import numpy as np
import time
import datetime
import cx_Oracle
from numpy._typing._array_like import NDArray
from pathlib import Path
from typing import Any, Hashable, TextIO
from itertools import chain, combinations
from S0_Loaders import Parameter_Loader


def escribe_reporta(reporte: TextIO,texto: str) -> None:
    """Escribe en un archivo txt, que ya debe venir abierto previamente el texto que le indiquemos

    Parameters
    ----------
    reporte : archivo txt
        es el archivo donde escribiremos la informacion a reportar
    texto : string
        contiene el texto a escribir
    """
    reporte.write(texto)
    reporte.write('\n')
    # Igualmente mostramos en pantalla lo que escribiremos en el reporte
    print(texto)


def get_all_subsets(s: list[str]) -> list[set[str]]:
    """
    Genera todos los subconjuntos de un conjunto 's'.
        Parameters:
        s (set): El conjunto de entrada para el cual se deben generar los subconjuntos.
         Returns:
        list: Una lista de conjuntos que representa todos los subconjuntos de 's'.
    """
    # Convert the input set into a list to ensure order of elements in subsets.
    s_list = list(s)
    # Generate all possible combinations of elements in the list.
    all_combinations = chain.from_iterable(combinations(s_list, r) for r in range(len(s_list) + 1))
    # Convert each combination into a set to obtain subsets.
    return [set(comb) for comb in all_combinations]


def filtra_una_combinacion(df: pd.DataFrame,lista_campos: list[str],tabla_parametros: pd.DataFrame,combinacion: set[str],cols_cruce: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Funcion que filtra un dataframe de acuerdo a las caracteristicas de 1 tipo_calculo de reaseguro especificado
        Ademas, tiene la funcion de poder hacer un merge_asof cuando el tipo_calculo asignado para un producto cambia en el tiempo
    Parameters
    ----------
        df : pd.DataFrame
        DataFrame
        El DataFrame que contiene los datos a filtrar.
        lista_campos : list[str]
        Lista de nombres de columnas en el DataFrame que se utilizarán para el filtrado.
        tabla_parametros : DataFrame
        DataFrame que contiene los parámetros de filtrado.
        combinacion : list[str]
        Lista de nombres de columnas que representan una combinación específica de valores para el filtrado.
        cols_cruce : list[str]
        Lista de nombres de columnas que se utilizarán para realizar el cruce entre el DataFrame y la tabla de parámetros.

        Returns
        -------
        tuple[DataFrame, DataFrame, DataFrame]
        Una tupla que contiene tres DataFrames:
        - El DataFrame filtrado que coincide con la combinación específica.
        - El DataFrame restante después de eliminar las filas filtradas.
        - La tabla de parámetros original.
        """
    # * Calculos iniciales
    # df_filtrado que será el que se irá cruzando con la tabla de las asignaciones
    df_filtrado: pd.DataFrame=df.copy()
    # Se guarda el indice para no perder el orden de los registros
    df_filtrado['INDICE']=df_filtrado.index
    # Tabla auxiliar que tendrá filtrado lo que vamos a cruzar
    tabla_parametros_filtrada: pd.DataFrame=tabla_parametros.copy()
    # Combinacion de campos que vamos a utilizar para cruzar
    combinacion_list = list(combinacion)
    # Campos que no vamos a cruzar
    combi_out=list(set(lista_campos).difference(combinacion_list))
    tabla_parametros_filtrada = tabla_parametros_filtrada.dropna(subset=combinacion_list) # type: ignore
    tabla_parametros_quitar: pd.DataFrame=tabla_parametros_filtrada.dropna(subset=combi_out,how='all')
    tabla_parametros_filtrada=tabla_parametros_filtrada.loc[tabla_parametros_filtrada.index.difference(tabla_parametros_quitar.index)].reset_index(drop=True)
    if tabla_parametros_filtrada.empty: return pd.DataFrame(),df,tabla_parametros
    else:
        tpf_sin_inicio: pd.DataFrame=tabla_parametros_filtrada[tabla_parametros_filtrada['INICIO DEL CONTRATO'].isnull()].copy()
        tpf_con_inicio: pd.DataFrame=tabla_parametros_filtrada[~tabla_parametros_filtrada['INICIO DEL CONTRATO'].isnull()].copy()
        # Pregunta si necesitamos hacer un merge o merge_asof, en caso de tener cambio de contratos en el tiempo
        if not tpf_sin_inicio.empty: df_filtrado_sin_inicio=df_filtrado.merge(tpf_sin_inicio[combinacion_list+cols_cruce],how='inner',on=combinacion_list)
        else: df_filtrado_sin_inicio=pd.DataFrame()
        if not tpf_con_inicio.empty:
            tpf_con_inicio_unicos=tpf_con_inicio[combinacion_list].drop_duplicates()
            df_filtrado_con_inicio=pd.DataFrame()
            # Recorre el dataframe para cada registro de filtro que deba hacer, para ir concatenandolos en el df que necesitamos
            for index, row in tpf_con_inicio_unicos.iterrows():
                lista_valores=list(row)
                df_filtrado_con_inicio_aux=df_filtrado.copy()
                tpf_con_inicio_filtrada=tpf_con_inicio.copy()
                for col,valor in zip(combinacion_list,lista_valores):
                    df_filtrado_con_inicio_aux=df_filtrado_con_inicio_aux[df_filtrado_con_inicio_aux[col]==valor]
                    tpf_con_inicio_filtrada=tpf_con_inicio_filtrada[tpf_con_inicio_filtrada[col]==valor]
                df_filtrado_con_inicio_aux=pd.merge_asof(df_filtrado_con_inicio_aux.sort_values('FECHA CRUCE VIGENCIAS'),tpf_con_inicio_filtrada[cols_cruce].sort_values('INICIO DEL CONTRATO'),left_on='FECHA CRUCE VIGENCIAS',right_on='INICIO DEL CONTRATO')
                df_filtrado_con_inicio_aux=df_filtrado_con_inicio_aux.dropna(subset=cols_cruce)
                df_filtrado_con_inicio=pd.concat([df_filtrado_con_inicio,df_filtrado_con_inicio_aux])
        else: df_filtrado_con_inicio=pd.DataFrame()
        df_filtrado=pd.concat([df_filtrado_con_inicio,df_filtrado_sin_inicio])
        df_a_filtrar: pd.DataFrame=df.loc[df.index.difference(df_filtrado['INDICE'])] # type: ignore
        df_filtrado=df_filtrado.drop(columns=['INDICE'])
        tabla_parametros_a_filtrar=tabla_parametros
        return df_filtrado,df_a_filtrar,tabla_parametros_a_filtrar


def asignacion_contratos(df: pd.DataFrame, parameters: Parameter_Loader, tables: Parameter_Loader,mantiene_na: int = 0) -> pd.DataFrame:
    """Asignacion de contratos de reaseguro, de acuerdo al contrato seleccionado
        Utiliza la funcion filtra_una_combinacion tantas veces como sea necesario hasta filtrar todo lo que el contrato tiene

    Parameters
    ----------
    df : pd.DataFrame
        dataframe con la data de asegurados
    parameters : Parameter_Loader
        Contiene los parametros del calculo
    tables : Parameter_Loader
        Contiene las tablas que ayudan a calcular los contratos de reaseguro
    mantiene_na : int, optional
        binario que permite mantener los registros que no fueron asignados al contrato de reaseguro, by default 0

    Returns
    -------
    pd.DataFrame
        dataframe original pero con mayor cantidad de campos que indican caracteristicas del contrato asignado
    """

        
    # Contiene las polizas que debo asignar por ocurrencia al reaseguro
    ocurrencias: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Ocurrencias')
    tabla_parametros: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Matriz Contrato-Cobertura')
    contrato: str = parameters.parameters['contrato']
    tipo_calculo: str = parameters.parameters['tipo_calculo']
    tipo_prima: str = parameters.parameters['tipo_prima']
    archivo_reporte: Any = parameters.parameters['archivo_reporte']
    # Definiciones preliminares del proceso
    original_rows=df.shape[0]
    # Solo tomo los registros asociados al proceso que estoy corriendo. Mayor info en el diccionario de contratos
    if tipo_calculo == 'Prima de Reaseguro': tabla_parametros=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato]
    cols_cruce=['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','INICIO DEL CONTRATO']
    lista_campos: list[str]=list(set(list(tabla_parametros.columns)).difference(cols_cruce))
    lista_combinaciones: list[set[str]] = get_all_subsets(lista_campos)
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


def asignacion_vigencias(df: pd.DataFrame, parameters: Parameter_Loader, tables: Parameter_Loader,mantiene_na: int = 0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Asigna la vigencia del reaseguro a la cual pertenece cada registro. Tambien puede quitar del reaseguro aquellos registros que por fechas de inicio y fin no deban pertencer al contrato de reaseguro

    Parameters
    ----------
    df : pd.DataFrame
        dataframe con la data de asegurados
    parameters : Parameter_Loader
        Contiene los parametros del calculo
    tables : Parameter_Loader
        Contiene las tablas que ayudan a calcular 
    mantiene_na : int, optional
        binario que permite mantener los registros que no fueron asignados al contrato de reaseguro, by default 0

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        dataframe original pero con mayor cantidad de campos que indican caracteristicas de la vigencia del contrato asignado
    """

    archivo_reporte: Any = parameters.parameters['archivo_reporte']
    contrato: str = parameters.parameters['contrato']
    tipo_calculo: str = parameters.parameters['tipo_calculo']
    tabla_parametros: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Matriz Vigencias')
    escribe_reporta(archivo_reporte,'COMIENZA LA ASIGNACION DE VIGENCIAS DE LOS CONTRATOS DE REASEGURO:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    cols_iniciales=list(df.columns)
    df_nuls=df[df['CONTRATO REASEGURO'].isnull()].copy()
    df_final=pd.DataFrame()
    
    if tipo_calculo == 'Prima de Reaseguro':
        tabla_parametros=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato]
        cobs_reaseguro=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato]['COBERTURA DEL CONTRATO'].unique()
        # Ciclo que recorre cada uno de los elementos de la losta para ir filtrando y asignado vigencias
        for cobertura_reaseg in cobs_reaseguro:
            df_filtrado: pd.DataFrame = df[df['COBERTURA DEL CONTRATO']==cobertura_reaseg]
            tabla_filtrada: pd.DataFrame = tabla_parametros[tabla_parametros['COBERTURA DEL CONTRATO']==cobertura_reaseg]
            df_final: pd.DataFrame = pd.concat(objs=[df_final,pd.merge_asof(df_filtrado.sort_values('FECHA CRUCE VIGENCIAS'),tabla_filtrada.sort_values('FECHA INICIO CONTRATO').drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'],axis=1),left_on=['FECHA CRUCE VIGENCIAS'],right_on='FECHA INICIO CONTRATO')],axis=0) # type: ignore
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
                df_final=pd.concat([df_final,pd.merge_asof(df_filtrado.sort_values('FECHA CRUCE VIGENCIAS'),tabla_filtrada.sort_values('FECHA INICIO CONTRATO').drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'],axis=1),left_on=['FECHA CRUCE VIGENCIAS'],right_on='FECHA INICIO CONTRATO')],axis=0) # type: ignore
    
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
    if mantiene_na==1: return pd.concat([df_final_02,df_deleted.drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','INICIO DEL CONTRATO']+cols_extra,axis=1),df_nuls],axis=0),df_deleted
    else: return df_final_02,df_deleted


def cumulo_riesgo(df: pd.DataFrame, parameters: Parameter_Loader, tables: Parameter_Loader, riesgo_cumulo: str, campos_str: str, limite_retencion:Any,tipo_cumulo: str,columna_capital: str) -> pd.DataFrame:
    """
    Calcula la suma de riesgos (cúmulos) y determina los límites o retenciones aplicables
    sobre el capital de un conjunto de registros. Dependiendo de la configuración, se
    cruzan tablas de referencia para determinar la retención y se calculan proporciones
    para ajustar el capital según correspondan los casos de siniestros pagados o pendientes.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame principal que contiene la información de pólizas, capital y otros campos
        necesarios para el cálculo.
    parameters : Parameter_Loader
        Objeto que contiene parámetros relevantes para la lógica del cálculo, tales como
        tipo de cálculo, contrato de reaseguro, archivo de reporte, etc.
    tables : Parameter_Loader
        Objeto utilizado para cargar tablas auxiliares (por ejemplo, archivos de Excel)
        que contienen información de retenciones o límites.
    riesgo_cumulo : str
        Identificador del riesgo de cúmulo que se desea procesar (por ejemplo, 'Riesgo1').
    campos_str : str
        String que contiene los nombres de los campos (separados por comas) que serán
        utilizados para agrupar los registros a fin de calcular los cúmulos.
    limite_retencion : Any
        Puede ser un valor numérico que aplica como retención o el nombre de una tabla
        (hoja de Excel) que contiene valores de retenciones variables según distintos
        criterios. Si es un string, se intentará buscarlo en `tables`.
    tipo_cumulo : str
        Nombre de la columna en `df` que se evalúa para filtrar los registros
        correspondientes al `riesgo_cumulo` especificado.
    columna_capital : str
        Nombre de la columna en `df` que contiene el valor de capital sobre el cual
        se aplicará la lógica de retenciones o límites.

    Returns
    -------
    pd.DataFrame
        DataFrame resultante con la información filtrada según el tipo de cúmulo y
        contrato, y con nuevas columnas que describen:
        - CUMULO: Suma del capital agrupado.
        - (Opcional) CUMULO_PAGADOS: Suma del capital para siniestros pagados.
        - PORCENTAJE: Porcentaje que se aplica para el cálculo de capital posterior,
          considerando la retención o el límite.
        - CAPITAL POSTERIOR: Capital ajustado tras aplicar la retención o límite.
        - (Opcional) PORCENTAJE PAGADOS y PORCENTAJE PENDIENTES: Se generan solamente
          en caso de que el `tipo_calculo` incluya siniestros. Luego se unifican en
          la columna final `PORCENTAJE`.

    Notes
    -----
    1. La función filtra `df` según las columnas `CONTRATO REASEGURO` y `tipo_cumulo`,
       y en caso de no encontrar registros, retorna el DataFrame filtrado vacío.
    2. Se agrupan registros por los campos indicados en `campos_str` para sumar
       el `columna_capital`, resultando en la columna `CUMULO`.
    3. El parámetro `limite_retencion` puede ser un valor único o el nombre de una tabla;
       si es el nombre de una tabla, se carga e incorpora la columna de retenciones.
    4. En cálculos de siniestros, se separan los siniestros pagados para aplicar
       porcentajes distintos respecto de los pendientes.
    5. Se generan reportes de advertencia (mediante la función `escribe_reporta`) en
       caso de que existan campos faltantes o discrepancias en la unión de los datos.
    """
    
    tipo_calculo: str = parameters.parameters['tipo_calculo']
    contrato: str = parameters.parameters['contrato']
    archivo_reporte: Any = parameters.parameters['archivo_reporte']
    ruta_extensa: str = parameters.ruta_extensa
    # * Funcion de calculo de cumulo para un tipo_calculo y riesgo particular
    # Filtro el df por el tipo_calculo y el riesgo de cumulo correspondiente
    df_filter=df[(df['CONTRATO REASEGURO']==contrato) & (df[tipo_cumulo]==riesgo_cumulo)]
    # Si el df filtrado es vacio (pensado en que la funcion cumulos recorre todos los registros de su tabla de cumulos), entonces crea en el df las columnas que se crearán en caso de no ser vacio
    if df_filter.empty:
        #print('dataframe vacio para tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato,riesgo_cumulo))
        return df_filter
    # Hacemos groupby por la lista de campos que entregamos. Tomamos como agregacion la columna de cumulos que indicamos en los parametros
    lista_campos: list[str] = campos_str.split(',')
    if sum([0 if x in df.columns else 1 for x in lista_campos])==0:
        df_grouped = df_filter.groupby(lista_campos)[columna_capital].sum().reset_index().rename(columns={columna_capital:'CUMULO'})
        if 'Siniestro' in tipo_calculo: df_grouped_pagados=df_filter[df_filter['ESTADO SINIESTRO']=='PAGADO'].groupby(lista_campos)[columna_capital].sum().reset_index().rename(columns={columna_capital:'CUMULO_PAGADOS'})
    else:
        for campo in lista_campos:
            if campo not in df.columns:
                escribe_reporta(archivo_reporte,'El campo {} para hacer cumulo no se encuentra dentro del dataframe, para tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(campo,contrato,riesgo_cumulo))
    # Calculo de retencion: 
    # Si la retencion no es igual para todos registros, se debe inrgesar el nombre de la tabla que parametriza aquello
    if isinstance(limite_retencion, str):
        # Busca la tabla del nombre que pusimos dentro de la tabla de cumulos
        try:
            tabla_cumulos: pd.DataFrame = tables.get_table_xlsx(sheet_name = limite_retencion)
        except:
            escribe_reporta(archivo_reporte,f'la tabla de retenciones especificada no existe para el tipo_calculo de reaseguro {contrato} y riesgo cumulo {riesgo_cumulo}')
        # Revisa el nombre de los campos que tiene, y quita el que contiene el limite
        campos = list(tabla_cumulos.columns)
        campos.remove('LIMITE O RETENCION')
        # Cruza con el df de acuerdo al resto de campos dentro de la tabla
        df_grouped=df_grouped.merge(tabla_cumulos,how='inner', left_on=campos, right_on=campos,suffixes=['','_x']) # type: ignore
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
    df_final=df_filter.merge(df_grouped,how='inner', left_on=lista_campos, right_on=lista_campos,suffixes=['','_x']) # type: ignore
    if df_final.shape[0] > df_filter.shape[0]:
        escribe_reporta(archivo_reporte,'la tabla agrupada con los limites y retenciones cruzó más registros que el df original, para el tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato,riesgo_cumulo))
    elif df_final.shape[0] < df_filter.shape[0]:
        escribe_reporta(archivo_reporte,'la tabla agrupada con los limites y retenciones cruzó menos registros que el df original, para el tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato,riesgo_cumulo))
    # Columna que calcula el capital posterior a establecer limite o retencion, segun corresponda
    if 'Siniestro' in tipo_calculo: df_final['PORCENTAJE']=np.where(df_final['ESTADO SINIESTRO']=='PAGADO',df_final['PORCENTAJE PAGADOS'],df_final['PORCENTAJE PENDIENTES'])
    df_final['CAPITAL POSTERIOR']=df_final['PORCENTAJE']*df_final[columna_capital]
    if 'Siniestro' in tipo_calculo: df_final.drop(columns=['PORCENTAJE PAGADOS', 'PORCENTAJE PENDIENTES'],inplace=True)
    return df_final


def cumulos(df: pd.DataFrame, parameters: Parameter_Loader, tables: Parameter_Loader,campo_cumulo: str) -> pd.DataFrame:
    """
    Calcula y asigna los límites o retenciones de cúmulo de reaseguros 
    según los parámetros y tablas de cúmulos definidos.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con la información base sobre la cual se aplican 
        los cálculos de cúmulos (pueden ser pólizas, siniestros, etc.).
    parameters : Parameter_Loader
        Objeto que contiene los parámetros de configuración necesarios 
        para la función (por ejemplo, 'archivo_reporte', 'tipo_calculo', etc.).
    tables : Parameter_Loader
        Objeto que carga las tablas en formato xlsx para definir los 
        diferentes tipos de cúmulos: 
        - 'Matriz Cumulo Individual'
        - 'Matriz Cumulo Contrato'
        - 'Matriz Cumulo Excedente'
    campo_cumulo : str
        Tipo de cúmulo a aplicar. Puede tomar valores como:
        - 'RIESGO LIMITE INDIVIDUAL'
        - 'RIESGO LIMITE CONTRATO'
        - 'RIESGO RETENCION EXCEDENTE'

    Returns
    -------
    pd.DataFrame
        DataFrame con la información actualizada según la aplicación
        de los cálculos de cúmulo. Se añaden (o modifican) columnas para 
        reflejar el cúmulo, la retención o límite asociado, los porcentajes 
        aplicados y el capital posterior al cálculo, entre otros campos.
    """

    # Se obtienen las tablas con configuraciones de cúmulos de acuerdo con cada tipo.
    cumulos_individuales = tables.get_table_xlsx(sheet_name='Matriz Cumulo Individual')
    cumulos_contrato = tables.get_table_xlsx(sheet_name='Matriz Cumulo Contrato')
    cumulos_excedente = tables.get_table_xlsx(sheet_name='Matriz Cumulo Excedente')

    # Se recuperan algunos parámetros relevantes.
    archivo_reporte: Any = parameters.parameters['archivo_reporte']
    tipo_calculo: str = parameters.parameters['tipo_calculo']

    # Diccionario que define los campos a utilizar dependiendo del tipo de cúmulo.
    # Índices:
    #   0: Nombre del campo de cúmulo total
    #   1: Nombre del campo de porcentaje
    #   2: Nombre del campo de retención o límite
    #   3: Tabla con los valores de retenciones o límites
    #   4: Campo base para el cálculo de montos asegurados
    #   5: Campo resultante después de aplicar límite o retención
    #   6: Campo de cúmulo de pagados (aplica para siniestros)
    diccionario_cumulos = {
        'RIESGO LIMITE INDIVIDUAL': [
            'CUMULO LIMITE INDIVIDUAL',
            'PORCENTAJE LIMITE INDIVIDUAL',
            'LIMITE INDIVIDUAL',
            cumulos_individuales,
            'MONTO ASEGURADO',
            'CAPITAL POST LIMITE INDIVIDUAL',
            'CUMULO PAGADOS LIMITE INDIVIDUAL'
        ],
        'RIESGO LIMITE CONTRATO': [
            'CUMULO LIMITE CONTRATO',
            'PORCENTAJE LIMITE CONTRATO',
            'LIMITE CONTRATO',
            cumulos_contrato,
            'CAPITAL POST LIMITE INDIVIDUAL',
            'CAPITAL POST LIMITE CONTRATO',
            'CUMULO PAGADOS LIMITE CONTRATO'
        ],
        'RIESGO RETENCION EXCEDENTE': [
            'CUMULO RETENCION EXCEDENTE',
            'PORCENTAJE RETENCION EXCEDENTE',
            'RETENCION EXCEDENTE',
            cumulos_excedente,
            'CAPITAL POST LIMITE CONTRATO',
            'CAPITAL RETENIDO POST EXCEDENTE',
            'CUMULO PAGADOS RETENCION EXCEDENTE'
        ],
    }

    # Se registra el inicio del proceso de cálculo de cúmulos.
    escribe_reporta(
        archivo_reporte,
        'Comienza proceso de calculo de cumulos del tipo {}:\n{}'.format(
            campo_cumulo,
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        )
    )

    # Se selecciona la tabla correspondiente al tipo de cúmulo especificado.
    tabla_cumulos = diccionario_cumulos[campo_cumulo][3]

    # Validación: se verifica que los campos requeridos existan en el DataFrame.
    for campo in [campo_cumulo, diccionario_cumulos[campo_cumulo][4]]:
        if campo not in df.columns:
            escribe_reporta(
                archivo_reporte,
                'El campo {} no se encuentra dentro del dataframe'.format(campo)
            )

    # Se filtran los registros que no requieren ser acumulados (campo_cumulo nulo).
    df_inicial = df[df[campo_cumulo].isnull()].copy()

    # Se inicializan campos para el DataFrame con valores por defecto.
    df_inicial['CUMULO'] = df_inicial[diccionario_cumulos[campo_cumulo][4]]
    df_inicial['LIMITE O RETENCION'] = np.nan

    # Si se trata de siniestros, se calcula el cúmulo de pagados solo para los registros con estado "PAGADO".
    if 'Siniestro' in tipo_calculo:
        df_inicial['CUMULO_PAGADOS'] = np.where(
            df_inicial['ESTADO SINIESTRO'] == 'PAGADO',
            df_inicial[diccionario_cumulos[campo_cumulo][4]],
            0
        )

    df_inicial['PORCENTAJE'] = 1
    df_inicial['CAPITAL POSTERIOR'] = df_inicial[diccionario_cumulos[campo_cumulo][4]]

    # Por cada configuración en la tabla de cúmulos, se aplica la función `cumulo_riesgo`
    # y se concatenan los resultados al DataFrame inicial.
    for index, row in tabla_cumulos.iterrows():
        df_agregar = cumulo_riesgo(
            df,
            parameters,
            tables,
            row[campo_cumulo],
            row['CAMPOS A ACUMULAR'],
            row['LIMITE O RETENCION'],
            campo_cumulo,
            diccionario_cumulos[campo_cumulo][4]
        )
        df_inicial = pd.concat([df_inicial, df_agregar], axis=0)

    # Se renombran las columnas para reflejar correctamente el tipo de cúmulo aplicado.
    df_inicial.rename(
        columns={
            'CUMULO': diccionario_cumulos[campo_cumulo][0],
            'PORCENTAJE': diccionario_cumulos[campo_cumulo][1],
            'LIMITE O RETENCION': diccionario_cumulos[campo_cumulo][2],
            'CAPITAL POSTERIOR': diccionario_cumulos[campo_cumulo][5],
            'CUMULO_PAGADOS': diccionario_cumulos[campo_cumulo][6]
        },
        inplace=True
    )

    return df_inicial



def calcula_edad(rut_series: pd.Series,fec_nac_series: pd.Series,fec_corte_series: Any,edad_perdidos: int,edad_tope: int,archivo_reporte: TextIO, reporta_issues: int = 0, edad_inf: int = 0, aplica_edad_prom_cartera: int = 0) -> NDArray[np.int_], Any: # type: ignore
    """
    Calcula la edad de cada registro en una serie, basado en la fecha de nacimiento (fec_nac_series)
    y la fecha de corte (fec_corte_series). Además, aplica reglas de corrección en casos de datos
    perdidos o inválidos, y puede reportar información adicional sobre los registros problemáticos.

    Parameters
    ----------
    rut_series : pd.Series
        Serie que contiene los RUT (identificador único) de cada registro.
    fec_nac_series : pd.Series
        Serie que contiene las fechas de nacimiento de cada registro.
    fec_corte_series : Any
        Fecha de corte o serie de fechas de corte para el cálculo de la edad.
        Puede ser un objeto datetime o un pd.Series de fechas.
    edad_perdidos : int
        Edad que se asigna a los registros con fecha de nacimiento nula o mala cuando
        NO se aplica la edad promedio de la cartera.
    edad_tope : int
        Edad máxima permitida. Si la edad calculada supera este valor y no se aplica
        la edad promedio de la cartera, se recorta a este tope.
    archivo_reporte : TextIO
        Objeto de archivo para escribir los reportes de problemas detectados.
    reporta_issues : int, optional
        Indica si se debe retornar también el arreglo con los registros que presentan
        problemas. Valor por defecto = 0 (no se reportan).
    edad_inf : int, optional
        Edad mínima permitida. Si la edad calculada es menor a este valor y no se aplica
        la edad promedio de la cartera, se ajusta a este valor. Valor por defecto = 0.
    aplica_edad_prom_cartera : int, optional
        Indica si se utiliza la edad promedio de la cartera para reemplazar las edades
        perdidas o fuera de rango en lugar de los valores por defecto (edad_perdidos,
        edad_inf, edad_tope). Valor por defecto = 0 (no se aplica).

    Returns
    -------
    NDArray[np.int_]
        Arreglo unidimensional con la edad final calculada para cada registro.
    Any, optional
        Si `reporta_issues` es 1, además de la edad se retorna un arreglo (np.array) que
        marca con 1 los registros problemáticos y con 0 los registros correctos.
    """
    # Se crea un DataFrame base con el RUT y la fecha de nacimiento (posiblemente repetidos).
    df_ruts = pd.DataFrame({'RUT': rut_series, 'FEC_NAC': fec_nac_series})
    
    # Se crea otro DataFrame para obtener la fecha de nacimiento mínima por RUT (manejo de duplicados).
    df_fechas_nac = (
        pd.DataFrame({'RUT': rut_series, 'FEC_NAC': fec_nac_series})
        .groupby(['RUT'])
        .min()
        .reset_index()
    )
    
    # Se hace un merge para que cada RUT tenga su fecha de nacimiento mínima.
    df_ruts_final = df_ruts.merge(df_fechas_nac, how='left', on='RUT')
    
    # Se actualiza la serie de fechas de nacimiento con el valor mínimo encontrado.
    fec_nac_series = df_ruts_final['FEC_NAC_y']
    
    # Se identifican fechas nulas o iguales a 1900-01-01 como "malas".
    edad_malas = np.where(
        (fec_nac_series.isnull()) | (fec_nac_series == datetime.datetime(1900, 1, 1)),
        1,
        0
    )
    
    # Se obtienen año y combinación de mes/día a partir de la fecha de nacimiento.
    serie_year = pd.DatetimeIndex(fec_nac_series).year  # type: ignore
    serie_monthday = (
        pd.DatetimeIndex(fec_nac_series).month * 100
        + pd.DatetimeIndex(fec_nac_series).day
    )  # type: ignore
    
    # Dependiendo del tipo de fec_corte_series (puede ser serie o fecha puntual),
    # se calculan el año y la combinación de mes/día.
    if isinstance(fec_corte_series, pd.core.series.Series):  # type: ignore
        fec_corte_year = pd.DatetimeIndex(fec_corte_series).year
        fec_corte_monthday = (
            pd.DatetimeIndex(fec_corte_series).month * 100
            + pd.DatetimeIndex(fec_corte_series).day
        )
    else:
        fec_corte_year = fec_corte_series.year
        fec_corte_monthday = fec_corte_series.month * 100 + fec_corte_series.day
    
    # Se calcula la edad promedio en la cartera, ignorando las fechas malas
    # (usando np.nan en lugar de esos valores).
    edad_promedio_cartera = np.nanmean(
        np.where(
            edad_malas == 1,
            np.nan,
            fec_corte_year
            - serie_year
            + np.where(serie_monthday <= fec_corte_monthday, 0, -1)
        )
    )
    
    # Si no se aplica la edad promedio de la cartera, se asigna edad_perdidos a fechas malas.
    if aplica_edad_prom_cartera == 0:
        edad_series = np.where(
            edad_malas == 1,
            edad_perdidos,
            fec_corte_year
            - serie_year
            + np.where(serie_monthday <= fec_corte_monthday, 0, -1)
        )
    # Si se aplica la edad promedio de la cartera, se reemplaza la edad de fechas malas por ese promedio.
    elif aplica_edad_prom_cartera == 1:
        edad_series = np.where(
            edad_malas == 1,
            edad_promedio_cartera,
            fec_corte_year
            - serie_year
            + np.where(serie_monthday <= fec_corte_monthday, 0, -1)
        )
    
    # Se marcan los registros que presentan algún problema:
    #    - Fecha mala
    #    - Edad mayor que el tope
    #    - Edad menor que el mínimo permitido
    registros_issues = np.where(
        (edad_malas == 1)
        | (np.where(edad_series > edad_tope, 1, 0) == 1)
        | (np.where(edad_series < edad_inf, 1, 0) == 1),
        1,
        0
    )
    
    # Se cuentan la cantidad de fechas malas y de fechas que exceden el tope.
    cont_fecnac_malas = sum(edad_malas)
    cont_fecnac_tope = sum(edad_series > edad_tope)
    
    # Ajuste final de edades según se aplique o no la edad promedio, y según los límites establecidos.
    if aplica_edad_prom_cartera == 0:
        edad_series_final = np.where(
            edad_series > edad_tope,
            edad_tope,
            np.where(edad_series < edad_inf, edad_inf, edad_series)
        )
    elif aplica_edad_prom_cartera == 1:
        edad_series_final = np.where(
            edad_series > edad_tope,
            edad_promedio_cartera,
            np.where(edad_series < edad_inf, edad_promedio_cartera, edad_series)
        )
    
    # Se escriben en el archivo de reporte los conteos de problemas detectados, si los hay.
    if cont_fecnac_malas > 0:
        escribe_reporta(
            archivo_reporte,
            'La cantidad de registros con la fecha nula o mala es de {} registros'.format(cont_fecnac_malas)
        )
    
    if cont_fecnac_tope > 0:
        escribe_reporta(
            archivo_reporte,
            'Un total de {} registros tienen edad mayor a 108 año. '
            'Fueron topados en 108 para poder encontrar valores en las tablas de incidencia'.format(cont_fecnac_tope)
        )
    
    # Retorna solamente la serie final con las edades o, si se especifica,
    # también retorna el array de issues.
    if reporta_issues == 0:
        return edad_series_final
    else:
        return edad_series_final, registros_issues
    
def calcula_exposicion(df: pd.DataFrame, campo_inicio: str,campo_fin: str, exp_days: int, fec_bop: datetime.datetime, fec_eop: datetime.datetime) -> NDArray[np.int_]: 
    """
    Calcula la exposición de cada registro en función de rangos de fechas de inicio y fin,
    ajustada a un período definido por `fec_bop` y `fec_eop`. El resultado se expresa
    como la proporción de días expuestos sobre `exp_days`.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame de entrada con la información de fechas (columnas de inicio y fin).
    campo_inicio : str
        Nombre de la columna en `df` que representa la fecha de inicio de validez.
    campo_fin : str
        Nombre de la columna en `df` que representa la fecha de fin de validez.
    exp_days : int
        Número de días que representan un período estándar de exposición (por ejemplo, 365).
    fec_bop : datetime.datetime
        Fecha de inicio (Beginning of Period) para el cálculo de la exposición.
    fec_eop : datetime.datetime
        Fecha de fin (End of Period) para el cálculo de la exposición.

    Returns
    -------
    np.typing.NDArray[np.int_]
        Arreglo de NumPy con los valores de exposición. Cada posición indica la proporción
        de exposición del registro correspondiente, calculada en relación a `exp_days`.
    """
    # Se realiza una copia del DataFrame original para no modificarlo directamente.
    df_aux = df.copy()

    # Se crean dos columnas que definen el período de cálculo de la exposición:
    # 'INICIO MES' (fec_bop) y 'FIN MES' (fec_eop + 1 día).
    df_aux['INICIO MES'] = pd.Timestamp(fec_bop.year, fec_bop.month, fec_bop.day)
    df_aux['FIN MES'] = pd.Timestamp(fec_eop.year, fec_eop.month, fec_eop.day) + datetime.timedelta(days=1)

    # Calcula la fecha de inicio real para cada registro como el máximo entre
    # la fecha de inicio del período y la fecha de inicio propia del registro.
    serie_inicio = np.maximum(df_aux['INICIO MES'], df_aux[campo_inicio])

    # Calcula la fecha de fin real para cada registro como el mínimo entre
    # la fecha de fin del período y la fecha de fin propia del registro.
    serie_fin = np.minimum(df_aux['FIN MES'], df_aux[campo_fin])

    # Calcula la exposición de cada registro:
    # - Si la fecha de inicio es posterior a 'fec_eop',
    #   o la fecha de fin es anterior a 'fec_bop',
    #   o la fecha de inicio supera a la fecha de fin del registro,
    #   entonces la exposición es 0.
    # - En caso contrario, se toma la diferencia de días entre serie_fin y serie_inicio,
    #   y se normaliza dividiendo por exp_days.
    serie_exposicion = np.where(
        (df_aux[campo_inicio] > fec_eop) |
        (df_aux[campo_fin] < fec_bop) |
        (df_aux[campo_inicio] > df_aux[campo_fin]),
        0,
        ((serie_fin - serie_inicio).dt.days) / exp_days
    )

    return serie_exposicion




def completa_campo(df: pd.DataFrame,campo_rellenar: str,campos_agrupar: list[str],parameters: Parameter_Loader,campo_cero=False) -> pd.DataFrame:
    """
    Completa valores faltantes en la columna especificada (campo_rellenar) de un DataFrame
    utilizando el promedio de esa columna agrupado por los campos proporcionados. Además,
    exporta la tabla de agregación a un archivo CSV.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame de entrada que contiene la columna a rellenar y las columnas para agrupar.
    campo_rellenar : str
        Nombre de la columna en el DataFrame `df` cuyos valores faltantes se desean completar.
    campos_agrupar : list of str
        Lista de columnas utilizadas para agrupar y calcular el promedio de `campo_rellenar`.
    parameters : Parameter_Loader
        Objeto que proporciona, entre otros, la ruta de salida (`ruta_output`),
        separador (`separador_output`) y formato decimal (`decimal_output`)
        para la exportación del archivo CSV.
    campo_cero : bool, optional
        Parámetro no implementado en la lógica actual de la función. Su valor no
        altera la forma en que se completan los datos. Por defecto es False.

    Returns
    -------
    pd.DataFrame
        DataFrame con los valores faltantes de `campo_rellenar` completados a partir
        del promedio calculado sobre los grupos definidos por `campos_agrupar`. 
        La columna resultante conserva el nombre original (`campo_rellenar`).

    Notes
    -----
    1. Para los registros con valor nulo en `campo_rellenar`, la función elimina temporalmente
       dicha columna, realiza el cruce con los promedios agrupados y luego rellena
       los valores nulos.
    2. El DataFrame agrupado (`df_agrupado`) se exporta a un archivo CSV nombrado con
       el prefijo `"0. Tabla Agrup"`, seguido del nombre de la columna y los campos
       que intervienen en el agrupamiento.
    3. La ruta y los parámetros de exportación se toman de `parameters.parameters`.
    4. Si no hay filas con valores no nulos en `campo_rellenar`, los valores faltantes
       no podrán ser rellenados con un promedio y permanecerán como nulos.
    """    
    # if campo_cero==True: df_sin_valores,df_con_valores=df[df[campo_rellenar].isnull()].copy(),df[~df[campo_rellenar].isnull()].copy()
    # elif campo_cero==False: df_sin_valores,df_con_valores=df[(df[campo_rellenar].isnull())|(df[campo_rellenar]==0)].copy(),df[(~df[campo_rellenar].isnull())&(df[campo_rellenar]>0)].copy()
    df_sin_valores,df_con_valores=df[df[campo_rellenar].isnull()].copy(),df[~df[campo_rellenar].isnull()].copy()
    df_sin_valores.drop(columns=[campo_rellenar],axis=1,inplace=True)
    df_agrupado=df_con_valores[[campo_rellenar]+campos_agrupar].groupby(campos_agrupar, dropna=False).agg('mean').reset_index()
    df_sin_valores=df_sin_valores.merge(df_agrupado,how='left',on=campos_agrupar)
    df_final=pd.concat([df_con_valores,df_sin_valores],axis=0)
    df_agrupado.to_csv(parameters.parameters['ruta_output']+'0. Tabla Agrup '+campo_rellenar+' campos '+'_'.join(campos_agrupar)+'.csv',sep=parameters.parameters['separador_output'],decimal=parameters.parameters['decimal_output'],date_format='%d-%m-%Y',index=False)
    return df_final
    

def completa_campo_total(
    df: pd.DataFrame,
    campo_completar: str,
    listas_campos_agrupar: List[str],
    parameters: Parameter_Loader,
    campo_cero: bool = False
) -> pd.DataFrame:
    """
    Completa valores nulos o no válidos en un campo específico de un DataFrame mediante
    agregaciones sucesivas. Finalmente, rellena los valores que siguen nulos con el
    promedio global calculado.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame base que contiene la información donde se desea completar el campo.
    campo_completar : str
        Nombre del campo a completar.
    listas_campos_agrupar : list of str
        Lista de campos o combinaciones de campos que se utilizarán para agrupar
        y completar el campo en cada paso.
    parameters : Parameter_Loader
        Objeto que contiene parámetros o configuraciones adicionales utilizadas
        dentro de la función `completa_campo`.
    campo_cero : bool, optional
        Indica si el criterio de asignación de nulos incluye el valor cero.
        - Si es True, se considera el 0 como un valor válido al calcular el promedio.
        - Si es False, los ceros se tratarán como valores a reemplazar.
        Por defecto es False.

    Returns
    -------
    pd.DataFrame
        DataFrame con una nueva columna `<campo_completar>_FINAL` que contiene los
        valores finales del campo completado.

    Notes
    -----
    1. La función crea internamente una copia del DataFrame original (`df_aux`) para
       no modificar los datos de entrada de manera directa.
    2. Dependiendo del valor de `campo_cero`, el cálculo del promedio se realiza
       filtrando o no los valores ceros.
    3. Después de cada agregación en `completa_campo`, los valores que continúan
       siendo nulos se completan con un promedio global calculado de manera inicial.
    4. La función `completa_campo` debe estar disponible en el entorno o importada
       de manera adecuada para su uso en esta función.
    """
    # Se crea una copia del DataFrame original para no modificarlo directamente.
    df_aux = df.copy()

    # Dependiendo del valor de 'campo_cero', se calcula el promedio general
    # y se definen los valores iniciales de la columna '_FINAL'.
    if campo_cero is True:
        # Cuando 'campo_cero' es True, se consideran todos los valores distintos de nulo.
        promedio_general = df_aux[~df_aux[campo_completar].isnull()][campo_completar].mean()
        df_aux[campo_completar + '_FINAL'] = df_aux[campo_completar]
    else:
        # Cuando 'campo_cero' es False, se excluyen los ceros y los nulos para el cálculo del promedio.
        promedio_general = df_aux[
            (~df_aux[campo_completar].isnull()) & (df_aux[campo_completar] > 0)
        ][campo_completar].mean()

        # Se asigna NaN a aquellos registros con valor <= 0, dejándolos listos para ser completados.
        df_aux[campo_completar + '_FINAL'] = np.where(
            df_aux[campo_completar] > 0,
            df_aux[campo_completar],
            np.nan
        )

    # Para cada campo (o combinación de campos) en la lista de agrupaciones,
    # se realiza la función 'completa_campo' que completa la columna '_FINAL'.
    for lista in listas_campos_agrupar:
        df_aux = df_aux=completa_campo(df_aux,campo_completar+'_FINAL',lista,parameters,campo_cero)

    # Luego de completar por grupos, se rellena con el promedio_general
    # los valores que aún estén nulos.
    df_aux[campo_completar + '_FINAL'] = df_aux[campo_completar + '_FINAL'].fillna(promedio_general)

    # Se retorna el DataFrame resultante con la nueva columna completada.
    return df_aux

        
def corrige_tasas_ges(df:pd.DataFrame, parameters: Parameter_Loader) -> pd.DataFrame:
    """
    Corrige las tasas de un DataFrame, manejando registros duplicados y calculando tasas promedio.

    Esta función identifica registros duplicados basados en un campo de RUT y otros identificadores.
    Luego, calcula la tasa promedio de los duplicados, elimina las tasas originales y asigna la nueva
    tasa promedio a los registros únicos, estableciendo además la periodicidad como 'M'.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame que contiene la información de tasas, incluyendo los identificadores
        relevantes para la detección de duplicados.
    parameters : Parameter_Loader
        Objeto que carga parámetros de configuración, incluyendo el campo que define los registros duplicados.

    Returns
    -------
    pd.DataFrame
        DataFrame corregido, donde los registros duplicados tienen su tasa ajustada
        y la periodicidad establecida a 'M'.
    """
    # Defino registros duplicados y no duplicados
    campo_rut_duplicados: str = parameters.parameters['campo_rut_duplicados']
    duplicados=df.loc[df.duplicated(subset=[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','COD_COB'],keep=False)].copy()
    no_duplicados_ges=df[~df.index.isin(duplicados.index)].copy()
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


def recargos(df:pd.DataFrame, parameters: Parameter_Loader,calcula_recargos=1) -> pd.DataFrame:
    ruta_recargos: str = parameters.parameters['ruta_recargos']
    separador_input: str = parameters.parameters['separador_input']
    decimal_input: str = parameters.parameters['decimal_input']
    ruta_output: str = parameters.parameters['ruta_output']
    fecha_inicio_mes: str = parameters.parameters['fecha_inicio_mes']
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
    else: 
        # df_iaxis['RECARGO']=np.where((df_iaxis['VALOR_RECARGO_SOBREPRIMA'].isnull())&(df_iaxis['VALOR_RECARGO_EXTRAPRIMA'].isnull()),0,1)
        df_iaxis['RECARGO']=df_iaxis['VALOR_RECARGO_SOBREPRIMA'].fillna(0)/100
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
    else: 
        # df_ges['RECARGO']=np.where((df_ges['ORIGEN'].isnull())&(df_ges['PORCENTAJE_RECARGO'].isnull()),0,1)
        df_ges['RECARGO'] = np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_ACTIVIDAD'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['SOBREPRIMA_ACTIVIDAD'].fillna(0)/100)+\
                        np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_MEDICO'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['SOBREPRIMA_MEDICO'].fillna(0)/100)+\
                        np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_DEPORTE'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['SOBREPRIMA_DEPORTE'].fillna(0)/100)+\
                        df_ges['PORCENTAJE_RECARGO'].fillna(0)/100
    # CREACION DF_FINAL
    df_final=pd.concat([df_iaxis[cols_df_final],df_ges[cols_df_final]],axis=0)
    if calcula_recargos==1: df_final['PRIMA REASEGURO']=df_final['PRIMA REASEGURO SIN RECARGO']+df_final['RECARGO']
    return df_final
    

def cruce_left(df_1:pd.DataFrame, df_2:pd.DataFrame, left_on: list[str], right_on: list[str], parameters: Parameter_Loader, suffixes: tuple[str,str]=('_df1', '_df2'), informa_no_cruces: int=1 , name: str = '') -> pd.DataFrame:
    ruta_output: str = parameters.parameters['ruta_output']
    separador_output: str = parameters.parameters['separador_output']
    decimal_output: str = parameters.parameters['decimal_output']
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


def identificador_anonimo(df:pd.DataFrame, campos: list[str]) -> pd.DataFrame:
    # Crear un DataFrame con combinaciones únicas de los identificadores
    identificadores_unicos = df[campos].drop_duplicates()
    nro_ruts = len(identificadores_unicos)
    # Generar números aleatorios para las combinaciones únicas
    np.random.seed(1000)  # Fijar semilla para reproducibilidad
    valores_aleatorios = np.random.choice(range(1000000, 9999999),size=nro_ruts,replace=False)
    identificadores_unicos['IDENTIFICADOR'] = valores_aleatorios
    # Hacer un merge para asignar los valores anonimizados al DataFrame original
    if len(identificadores_unicos['IDENTIFICADOR'].drop_duplicates()) ==nro_ruts:
        df = df.merge(identificadores_unicos, on=campos, how='left')
    else:
        print('Revisar los identificadores unicos. No fueron bien asignados')
    return df
    
    
def calculo_fechas_renovacion(df: pd.DataFrame,campo_inicio: str,campo_fin: str,campo_anulacion: str,campo_periodicidad: str,periodo_cierre: int,ajuste_pu: int=1) -> tuple[NDArray[np.int_],NDArray[np.datetime64]]:
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



def automatizacion_querys(files: Parameter_Loader) -> None:
    # Parametros de la consulta
    querys: Parameter_Loader = Parameter_Loader(excel_file=files.parameters['archivo_querys'], open_wb=True, ruta_extensa='')
    ruta_extensa: str = files.ruta_extensa
    periodo_inicio: int = querys.get_reference(reference='periodo_inicio')
    periodo_fin: int = querys.get_reference(reference='periodo_fin')
    querys.wb.close()
    parametros_split: pd.DataFrame = querys.get_table_xlsx(sheet_name = 'Split Querys').replace(np.nan, '', regex=True)
    parametros_querys: pd.DataFrame = querys.get_table_xlsx(sheet_name = 'Diccionario Querys').replace(np.nan, '', regex=True)
    diccionario_querys: dict[Hashable, Any]=parametros_querys.set_index('QUERY').to_dict()
    
    for consulta in parametros_querys['QUERY']:
        aplica=diccionario_querys['APLICA'][consulta]
        if aplica==1: 
            ejecuta_query(consulta,periodo_inicio,periodo_fin,diccionario_querys,parametros_split, ruta_extensa)
            
def ejecuta_query(consulta: str, periodo_inicio: int, periodo_fin: int, diccionario_querys: dict[str,Any], parametros_split: pd.DataFrame, ruta_extensa: str,name_file: str = None) -> None:
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


def split_querys(df: pd.DataFrame,parametros_split_filter: pd.DataFrame,ruta_exportar_query: str,terminacion_archivo: str,sistema: str) -> None:
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




