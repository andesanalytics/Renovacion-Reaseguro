"""
PRIMA DE REASEGURO
"""

# Ignorar warnings asociados a la lista desplegable del excel de parametros
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

# Importamos librerias que vamos a utilizar
import pandas as pd
import numpy as np
import time
from S0_Inputs import archivo_querys, archivo_calculos, archivo_parametros, ruta_extensa
from S1_Parametros_Calculo import tipo_calculo, ruta_input,ruta_output, periodo, tipo_contrato, contrato, separador_output, decimal_output, aplica_check_parametros, campo_rut_duplicados, archivo_reporte, tipo_prima, tipo_proceso, cap_expuestos, fecha_cierre
from S2_Funciones import cumulos, calculo_tasas_reaseguro, asignacion_contratos, asignacion_vigencias, aplica_reaseguro, diccionario_tramos_edades, diccionario_tramos_capital, diccionario_tramos_plazo, calcula_tramos, calculos_cierre_pr, calculos_prima_unica_bdx, escribe_reporta, calculos_cierre_pu, licitado_desg_nl, recargos, crea_siniestros_mes, aplica_pago_siniestros, calcula_tramos, diccionario_tramos_edades, salida_resultados, exportar_casos, compara_resultados
from S3_Pre_Procesamiento import pre_procesamiento, check_parametros_primas, check_parametros_siniestros


def prima_reaseguro():
    # Tiempo inicial
    start_time = time.time()
    print('Comienzo de cálculo de cierre de {} - {} periodo {}'.format(tipo_calculo,contrato, periodo))
    
    # Check de parametros
    if aplica_check_parametros==1: check_parametros_primas()   
    
    # Traemos tablas de parametrizaciones que vamos a usar
    contrato_cob = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
    parametros_contratos = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Vigencias')
    tasas_reaseguro = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Reaseguradores')
    
    # Cargamos la BBDD del mes con su funcion de pre-procesamiento
    df = pre_procesamiento(tipo_calculo)
    # Escribimos la prima neta del df procesado hasta el momento, para evidenciar integridad de la información.
    escribe_reporta(archivo_reporte,'El dataframe input luego de asignar contratos posee una prima neta de {}'.format(np.nansum(df['PRIMA NETA ANUAL'])))
    # Cruce con tablas de parametrizaciones
    df = asignacion_contratos(df,contrato_cob)
    df,df_deleted_vigencia = asignacion_vigencias(df,parametros_contratos,tipo_calculo)
    
    # Creamos algunas variables previas para el desarrollo
    cols_iniciales=list(df.columns)+['CAPITAL CEDIDO TOTAL','PORCENTAJE CEDIDO FINAL','FACTOR MENSUALIZACION','CUMULO RETENCION EXCEDENTE']
    
    # Aplicamos funcion "cumulos" sobre el df, para los 3 tipos de cumulos que debemos hacer
    # Recordar que la aplicacion de limites es secuencial, es decir, lo que resulte de capital post un limite/retención es usado como capital de cumulo para el siguiente limite/retencion
    # Revisar el diccionario de cumulos para mas detalles
    df = cumulos(df, 'RIESGO LIMITE INDIVIDUAL')
    df = cumulos(df, 'RIESGO LIMITE CONTRATO')
    df = cumulos(df, 'RIESGO RETENCION EXCEDENTE')
    
    # Calculamos la el capital retenido y cedido post aplicacion del tipo_calculo QS, para obtener el capital cedido final
    escribe_reporta(archivo_reporte,'COMIENZA EL CALCULO DE CAPITALES RETENIDOS Y CEDIDOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    df['CAPITAL CEDIDO POST EXCEDENTE'] = df['CAPITAL POST LIMITE CONTRATO'] * (1-df['PORCENTAJE RETENCION EXCEDENTE'])
    df['CAPITAL RETENIDO POST QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] * (1-np.where(df['CESION QS'].isnull(),0,df['CESION QS']))
    df['CAPITAL CEDIDO QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] - df['CAPITAL RETENIDO POST QS']
    df['CAPITAL RETENIDO TOTAL'] = df['MONTO ASEGURADO']-(df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS'])
    df['CAPITAL CEDIDO TOTAL'] = df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS']
    df['PORCENTAJE CEDIDO FINAL']=np.where(df['MONTO ASEGURADO']>0,df['CAPITAL CEDIDO TOTAL']/df['MONTO ASEGURADO'],df['CESION QS']*df['PORCENTAJE LIMITE INDIVIDUAL']*df['PORCENTAJE LIMITE CONTRATO']*df['PORCENTAJE RETENCION EXCEDENTE'])
    
    # Filtra los capitales cedidos mayores a 0, o mayores o iguales a 0. Depende del tipo_calculo
    if cap_expuestos==1: df=df[(df['CAPITAL CEDIDO TOTAL']>0)].copy()
    else: df=df[(df['CAPITAL CEDIDO TOTAL']>=0)].copy()
    
    # Calculamos campos de tramos (edad, plazo, capital) segun el tipo_calculo que tenga el asegurado
    df=calcula_tramos(df,diccionario_tramos_edades,'EDAD RENOVACION','EDAD')
    df=calcula_tramos(df,diccionario_tramos_plazo,'PLAZO MESES','PLAZO')
    df=calcula_tramos(df,diccionario_tramos_capital,'ICAPITAL','ICAPITAL',0)
    # Escribimos la prima neta del df procesado hasta el momento, para evidenciar integridad de la información.
    escribe_reporta(archivo_reporte,'El dataframe input luego de calcular capitales retenidos y cedidos posee una prima neta de {}'.format(np.nansum(df['PRIMA NETA ANUAL'])))
    

    
    # Calculamos las parametrizaciones de tasas de reaseguro por reasegurador
    df = df.merge(tasas_reaseguro, how='inner', on=['CONTRATO REASEGURO', 'COBERTURA DEL CONTRATO', 'VIGENCIA CONTRATO'], suffixes=['', '_x']).reset_index(drop=True)
    # Luego calcula las tasas de reaseguro para todos los registros
    df = calculo_tasas_reaseguro(df).reset_index(drop=True)
    # aplica reaseguro sobre el registro (edades y otras caracteristicas particulares de cada tipo_calculo)
    df=aplica_reaseguro(df)

    # CALCULO DE PRIMA DE REASEGURO Y OTROS CONCEPTOS DEL CIERRE
    df['FACTOR MENSUALIZACION']=np.where(df['TIPO DE PRIMA']=='Tabla Primas',1,np.where(df['TIPO DE PRIMA'].isin(['Tasa Anual x 1000','Porcentaje Tabla']),1/12,1))
    # La prima de reaseguro depende de si tu calculo es una tasa anual x 1000 o una tabla tipo M95 la cual se multiplica por el monto asegruado, o si tu tarifa es una prima ya establecida que depende de otros parametros
    df['PRIMA REASEGURO'] = np.where(df['TIPO DE PRIMA']=='Tasa Anual x 1000',df['CAPITAL CEDIDO TOTAL']*df['TASA O PRIMA DE REASEGURO']/1000,\
                        np.where(df['TIPO DE PRIMA']=='Porcentaje Tabla',df['CAPITAL CEDIDO TOTAL']*df['TASA O PRIMA DE REASEGURO']*df['TASA']/1000,\
                        np.where(df['TIPO DE PRIMA']=='Tabla Primas', df['TASA O PRIMA DE REASEGURO'],-10000000)))\
                        *df['EXPOSICION MENSUAL']*df['PARTICIPACION DEL REASEGURADOR']*df['APLICA']*df['FACTOR MENSUALIZACION']
    # Calculo de conceptos de reaseguro generales
    if (tipo_contrato=='Generales'):
        df['PRIMA CEDIDA']=df['PRIMA NETA ANUAL']*np.where(df['FORMA_PAGO_CODIGO']==0,1,1/12)*df['EXPOSICION MENSUAL']*df['PORCENTAJE CEDIDO FINAL']*df['PARTICIPACION DEL REASEGURADOR']
        if (tipo_prima=='Prima Recurrente'):df['COMISION REASEGURO']=df['PRIMA CEDIDA']-df['PRIMA REASEGURO']
    # Calculo de recargos por sobreprima y extraprima
    if tipo_contrato=='Vida': df=recargos(df)
    
    
    # CHECK DE CONSISTENCIA
    def check_cruces(df_ini, df_fin):
        """ # Funcion de Check de BBDD en cruces con tablas de parametrizaciones """
        name_ini = [x for x in globals() if globals()[x] is df_ini][0]
        name_fin = [x for x in globals() if globals()[x] is df_fin][0]
        if df_ini.shape[0] == df_fin.shape[0]:
            print('Check OK entre {} y {}'.format(name_ini, name_fin))
        elif df_ini.shape[0] > df_fin.shape[0]:
            escribe_reporta(archivo_reporte,'Revisar Check entre {} y {}. El dataframe {} tiene menos registros'.format(name_ini, name_fin, name_fin))
        else:
            escribe_reporta(archivo_reporte,'Revisar Check entre {} y {}. El dataframe {} tiene mas registros'.format(name_ini, name_fin, name_fin))
    
    def check_cruces_total(lista_df):
        count=0
        for dataframe in lista_df:
            if count==0:
                df_ant=dataframe
                count=1
            else:
                df_post=dataframe
                check_cruces(df_ant,df_post)
                df_ant=dataframe
    
    # escribe_reporta(archivo_reporte,'COMIENZA EL CHECK DE LOS DATAFRAME RESULTANTES:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # check_cruces_total([df_1,df_2,df_3,df_4,df_5,df_6])
    # check_cruces_total([df_8,df_9,df_10])
    
    
    # RESULTADOS
    escribe_reporta(archivo_reporte,'COMIENZA LA SALIDA DE RESULTADOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    lista_reaseguradores=list(df['REASEGURADOR'].unique())
    # Reporte de los registros fuera de vigencia por fechas
    if not df_deleted_vigencia.empty:df_deleted_vigencia.to_csv(ruta_output+'1. Reporte Registros Fuera Vigencia '+str(periodo)+'.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # Reporte de asegurados que superan limite individual
    if not df[df['PORCENTAJE LIMITE INDIVIDUAL']<1].empty:df[df['PORCENTAJE LIMITE INDIVIDUAL']<1].to_csv(ruta_output+'1. Reporte Casos Exceden Lim Individual '+str(periodo)+'.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # Reporte de casos que quedaron fuera del reaseguro por alguna condición de los aplica
    if not df[df['APLICA']==0].empty:df[df['APLICA']==0].to_csv(ruta_output+'1. Reporte Casos sin Reaseguro '+str(periodo)+'.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # Duplicados en el df final
    cols_dup = [campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','CODIGO COBERTURA','CODIGO COBERTURA IAXIS','SSEGURO','REASEGURADOR','NRIESGO'] if tipo_contrato=='Vida' else [campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','CODIGO COBERTURA','CODIGO COBERTURA IAXIS','SSEGURO','REASEGURADOR']
    duplicados=df.loc[df.duplicated(subset=cols_dup,keep=False)]
    if not (duplicados.empty): 
        escribe_reporta(archivo_reporte,'El dataframe final de salida contiene {} registros duplicados. Revisar!'.format(duplicados.shape[0]))
        duplicados.to_csv(ruta_output+'0.2.1 Duplicados df Final.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        df=df.drop_duplicates()
        duplicados_drop=df.loc[df.duplicated(subset=cols_dup,keep=False)]
        if not (duplicados_drop.empty): 
            escribe_reporta(archivo_reporte,'Luego de eliminar duplicados, el dataframe final de salida contiene {} registros duplicados. Revisar!'.format(duplicados_drop.shape[0]))
            duplicados_drop.to_csv(ruta_output+'0.2.2 Duplicados df Final Drop.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
    # Funciones de salida de resultados, generales como para los BDX
    df_salida=salida_resultados(df,tipo_calculo,cols_iniciales)
    # Calculos asociados a productos con prima unica
    if (tipo_prima=='Prima Unica'): df_historico_cierre=calculos_prima_unica_bdx(df_salida,lista_reaseguradores)
    # Calculos de Pasivos/Activos del Cierre
    if (tipo_prima=='Prima Recurrente')&(tipo_contrato=='Generales')&(tipo_proceso=='Cierre'):df_reservas=calculos_cierre_pr(df_salida,lista_reaseguradores)
    if (tipo_prima=='Prima Unica')&(tipo_proceso=='Cierre'):df_reservas=calculos_cierre_pu(df_historico_cierre,lista_reaseguradores)
    
    # Mido tiempo de ejecucion
    total_time = round((time.time()-start_time)/60, 2)
    escribe_reporta(archivo_reporte,'El tiempo total de ejecución fue de %s minutos' % total_time)
    # Cierro archivo de errores
    archivo_reporte.close()
    return df_salida


def siniestros_reaseguro():
    # Tiempo inicial
    start_time = time.time()
    print(f'Comienzo de cálculo de cierre de {tipo_calculo} - {contrato} periodo {periodo}')
    # Check de parametros
    if aplica_check_parametros==1: check_parametros_siniestros()
    
    # COMIENZO DE PROGRAMACION DE SINIESTROS CEDIDOS
    
    # Tablas de parametros a utilizar
    contrato_cob = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
    parametros_contratos = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Vigencias')
    tasas_reaseguro = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Reaseguradores')
    
    # Cargamos la BBDD del mes con su funcion de pre-procesamiento
    df = pre_procesamiento(tipo_calculo)
    
    # Cruce con tablas de parametrizaciones
    df = asignacion_contratos(df,contrato_cob,mantiene_na = 1)
    df, df_deleted_vigencia = asignacion_vigencias(df, parametros_contratos, tipo_calculo, mantiene_na = 1)
    
    # Junto df del mes con el historico para aquellos contratos que requieran de cumulos historicos (que reaseguren) + Crea el historico de siniestros del mes y lo guardo en los inputs
    df = crea_siniestros_mes(df, crea_historico=1)
    
    # Revisa que tipos de pagos deben ser incluidos en los cumulos
    df = aplica_pago_siniestros(df, recalcula_aplica=1)
    
    cols_iniciales=list(df.columns)+['MONTO SINIESTRO RETENIDO TOTAL','MONTO SINIESTRO CEDIDO TOTAL','PORCENTAJE CEDIDO FINAL','CUMULO RETENCION EXCEDENTE']
    # Multiplica por el coaseguro el monto de los siniestros, para calcular correctamente los cumulos
    # Nuestro porcentajes de cesion ya incluyen el coaseguro
    # df['MONTO SINIESTRO UF'] = df['MONTO SINIESTRO UF'] * df['COASEGURO']

    df = cumulos(df, 'RIESGO LIMITE INDIVIDUAL SINIESTROS')
    df = cumulos(df, 'RIESGO RETENCION EXCEDENTE SINIESTROS')
    
    # Calculo de capitales cedidos y retenidos
    df['MONTO SINIESTRO CEDIDO POST EXCEDENTE'] = df['MONTO SINIESTRO RETENIDO POST LIM INDIVIDUAL'] * (1-df['PORCENTAJE RETENCION EXCEDENTE'])
    df['MONTO SINIESTRO RETENIDO POST QS'] = df['MONTO SINIESTRO RETENIDO POST EXCEDENTE'] * (1-np.where(df['CESION QS'].isnull(),0,df['CESION QS']))
    df['MONTO SINIESTRO CEDIDO QS'] = df['MONTO SINIESTRO RETENIDO POST EXCEDENTE'] - df['MONTO SINIESTRO RETENIDO POST QS']
    df['MONTO SINIESTRO RETENIDO TOTAL'] = df['MONTO SINIESTRO RETENIDO POST QS']
    df['MONTO SINIESTRO CEDIDO TOTAL'] = df['MONTO SINIESTRO CEDIDO POST EXCEDENTE'] + df['MONTO SINIESTRO CEDIDO QS']
    df['PORCENTAJE CEDIDO FINAL']=df['MONTO SINIESTRO CEDIDO TOTAL']/df['MONTO SINIESTRO UF']
    
    # CREO DATA DE REGISTROS SIN REASEGURO
    df_sin_reaseguro = df[df['MONTO SINIESTRO CEDIDO TOTAL']==0].copy()
    df_sin_reaseguro['REASEGURADOR'] = 'SIN REASEGURADOR'
    df_sin_reaseguro['PARTICIPACION DEL REASEGURADOR'] = 0
    df_sin_reaseguro['SINIESTRO RETENIDO'] = df_sin_reaseguro['DIRECTO_TOTAL_UF']  
    df_sin_reaseguro['SINIESTRO CEDIDO'] = 0

    
    # FILTRO FATOS A REASEGURAR
    df = df[df['MONTO SINIESTRO CEDIDO TOTAL']!=0]
    
    if contrato == 'Vida': df=calcula_tramos(df,diccionario_tramos_edades,'EDAD SINIESTRO','EDAD')
    
    df=df.merge(tasas_reaseguro,how='left',on=['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO'])
    
    df['APLICA_EDAD']=np.where((df['EDAD SINIESTRO']<=df['EDAD MAXIMA PERMANENCIA'])|(df['EDAD MAXIMA PERMANENCIA'].isnull()),1,0)
    df['APLICA_CARENCIA']=np.where((df['DIAS DESDE INICIO VIGENCIA']>=df['CARENCIA'])|(df['CARENCIA'].isnull()),1,0)
    df['APLICA']=df['APLICA_EDAD']*df['APLICA_CARENCIA']*df['APLICA_PAGO']
    df['SINIESTRO CEDIDO']=df['MONTO SINIESTRO CEDIDO TOTAL']*df['PARTICIPACION DEL REASEGURADOR'].fillna(1)*df['APLICA'].fillna(1)
    df['SINIESTRO RETENIDO']=df['DIRECTO_TOTAL_UF']*df['PARTICIPACION DEL REASEGURADOR']* df['COASEGURO']-df['SINIESTRO CEDIDO']
    df_deleted_edad=df[df['APLICA_EDAD']==0].copy()
    df_deleted_carencia=df[df['APLICA_CARENCIA']==0].copy()
    
    
    df_deleted_vigencia.to_csv(ruta_output+'1. Siniestros Excluidos Vigencia '+str(periodo)+'.csv',sep=';')
    df_deleted_edad.to_csv(ruta_output+'1. Siniestros Excluidos Edad '+str(periodo)+'.csv',sep=';')
    df_deleted_carencia.to_csv(ruta_output+'1. Siniestros Excluidos Carencia '+str(periodo)+'.csv',sep=';')
    
    # RESULTADOS
    escribe_reporta(archivo_reporte,'COMIENZA LA SALIDA DE RESULTADOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # Funciones de salida de resultados, generales como para los BDX
    df_salida=salida_resultados(pd.concat([df,df_sin_reaseguro]),tipo_calculo,cols_iniciales)
    
    total_time = round((time.time()-start_time)/60, 2)
    escribe_reporta(archivo_reporte,'El tiempo total de ejecución fue de %s minutos' % total_time)
    archivo_reporte.close()

