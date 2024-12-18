"""
PRE-PROCESAMIENTO DE LAS BASES DE DATOS
"""


# Importamos librerias que vamos a utilizar
import pandas as pd
import numpy as np
import datetime
import time
from unidecode import unidecode
import unicodedata
import sys
from pandas.tseries.offsets import MonthEnd
from S0_Inputs import archivo_querys, archivo_calculos, archivo_parametros, ruta_extensa
from S1_Parametros_Calculo import fecha_inicio_mes, fecha_cierre, dias_exposicion, periodo, ruta_input, ruta_output, tdm_mensual, edad_casos_perdidos, archivo_input, archivo_input_ges, contrato, tipo_contrato, separador_input, decimal_input, separador_fechas_input, separador_output, decimal_output, campo_rut_duplicados, archivo_reporte, ruta_pyme, ruta_regiones, ruta_si, ruta_otros, base_iaxis, base_ges, clasificacion_contrato, base_input_siniestros_generales, tipo_calculo, ruta_uso_seguro
from S2_Funciones import calcula_exposicion, calcula_edad, calculo_fechas_renovacion, comisiones, iva_coberturas, escribe_reporta, completa_campo_total, corrige_tasas_ges, tabla_uf, coc_conceptos, coc_institucion, coc_reaseguradores

# Prueba de Ejecucion del codigo
print(f'El script {__name__} se está ejecutando')

# Importamos tablas de parametrizaciones
# Tablas de Primas y Siniestros
contrato_cob = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
parametros_contratos = pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Vigencias')
reaseguradores = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Reaseguradores')
# Tablas de Primas
cobs_ges=pd.read_excel(io=archivo_parametros,sheet_name='Coberturas GES')
planes_ges=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Planes GES')
parametros_cesantia=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Parametros Cesantia')
zonas=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Zonas')
meses_renta=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Meses Renta')
saldo_insoluto=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Saldo Insoluto')
estados_ges=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Estados GES')
estados_iaxis=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Estados IAXIS')
cumulos_is=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Cumulos I&S')
lob_primas=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='LOB Generales')
contratos_cesantia=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Contratos Cesantia')
canales_venta=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Canal Venta')
forma_pago=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Forma Pago')
periodos_bdx_cesantia=pd.read_excel(io=ruta_extensa+archivo_calculos,sheet_name='BDXs Cesantia')

# Tablas de Cumulos
cumulos_individuales=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Individual')
cumulos_contrato=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Contrato')
cumulos_excedente=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Excedente')
cumulos_individuales_siniestros=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Individual Sinies')
cumulos_excedente_siniestros=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Excedente Sinies')
# Tablas de Siniestros
ocurrencias=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Ocurrencias')
lob_siniestros_generales=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='LOB_Siniestros_Generales')
catastrofes=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Catastrofes')



diccionario_tablas_primas={ 'contrato_cob':[contrato_cob,['POLIZA','PRODUCTO','CODIGO COBERTURA','INICIO DEL CONTRATO']],\
                            'parametros_contratos':[parametros_contratos,['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO']],\
                            'reaseguradores':[reaseguradores,['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO','REASEGURADOR']],\
                            'cumulos_individuales':[cumulos_individuales,['CONTRATO REASEGURO','RIESGO LIMITE INDIVIDUAL']],\
                            'cumulos_contrato':[cumulos_contrato,['CONTRATO REASEGURO','RIESGO LIMITE CONTRATO']],\
                            'cumulos_excedente':[cumulos_excedente,['CONTRATO REASEGURO','RIESGO RETENCION EXCEDENTE']],\
                            'parametros_cesantia':[parametros_cesantia,['POLIZA']],\
                            'zonas':[zonas,['REGION']],\
                            'meses_renta':[meses_renta,['CODIGO COBERTURA']],\
                            'saldo_insoluto':[saldo_insoluto,['PRODUCTO','CODIGO COBERTURA']],\
                            'cobs_ges':[cobs_ges,['COD_COB']],\
                            'estados_ges':[estados_ges,['ESTADO']],\
                            'estados_iaxis':[estados_iaxis,['ESTADO']],\
                            'planes_ges':[planes_ges,['PRODUCTO','COD_PLAN']],\
                            'cumulos_is':[cumulos_is,['CODIGO COBERTURA']],\
                            'lob_primas':[lob_primas,['POLIZA']],\
                            'comisiones':[comisiones,['POL_PROD']],\
                            'iva_coberturas':[iva_coberturas,['CODIGO COBERTURA']],\
                            'contratos_cesantia':[contratos_cesantia,['CONTRATO REASEGURO','VIGENCIA CONTRATO']],\
                            'tabla_uf':[tabla_uf,['FECHA_EMISION']],\
                            # 'polizas_pyme':[polizas_pyme,['POLIZA']],\
                            'coc_conceptos':[coc_conceptos,['TIPO_POLIZA_LETRA']],\
                            'coc_institucion':[coc_institucion,['TIPO_POLIZA_LETRA','POL_PROD_COC']],\
                            'coc_reaseguradores':[coc_reaseguradores,['REASEGURADOR']],\
    }

diccionario_tablas_siniestros={'contrato_cob':[contrato_cob,['POLIZA','PRODUCTO','CODIGO COBERTURA','INICIO DEL CONTRATO']],\
                               'parametros_contratos':[parametros_contratos,['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO']],\
                               'reaseguradores':[reaseguradores,['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO','REASEGURADOR']],\
                               'cumulos_individuales_siniestros':[cumulos_individuales_siniestros,['CONTRATO REASEGURO','RIESGO LIMITE INDIVIDUAL SINIESTROS']],\
                               'cumulos_excedente_siniestros':[cumulos_excedente_siniestros,['CONTRATO REASEGURO','RIESGO RETENCION EXCEDENTE SINIESTROS']],\
                               'catastrofes':[catastrofes,['CATASTROFE_DETALLE']],\
                               'lob_siniestros_generales':[lob_siniestros_generales,['POL_PROD']],\
                               'ocurrencias':[ocurrencias,['POL_PROD']],\
      }  


# Formas de parsear las fechas que vienen en los archivos txt    
dateparse_forma1 = lambda x: pd.to_datetime(x, format='%d/%m/%Y',errors='coerce')
dateparse_forma2 = lambda x: pd.to_datetime(x, format='%d-%m-%Y',errors='coerce')
dateparse_forma3 = lambda x: pd.to_datetime(x, format='%d-%m-%Y %X',errors='coerce')
dateparse_forma4 = lambda x: pd.to_datetime(x, format='%Y-%m-%d',errors='coerce')

def revisa_duplicados(df,lista_campos,lista_valores):
    """ Funcion que revisa que no hayan elementos duplicados dentro de una tabla, dado un set de valores"""
    df_filtrado=df.copy()
    # For que va filtrando 1 a 1 los campos y valores establecidos, dentro del dataframe
    for col,valor in zip(lista_campos,lista_valores):
        if str(valor) not in ['nan','NaT','NaN']:
            df_filtrado=df_filtrado[df_filtrado[col]==valor]
    # Ahora preguntamos si existen duplicados
    if df_filtrado.shape[0]>1:
        # print('Existen duplicados en los siguientes parámetros. REVISAR!\n{}\n{}'.format(lista_campos,lista_valores))
        escribe_reporta(archivo_reporte,'Existen duplicados en los siguientes parámetros. REVISAR!\n{}'.format(df_filtrado))
        return 1
    else:
        return 0


def revisa_duplicados_all(df,lista_campos,nombre_tabla=''):
    if nombre_tabla!='': print('Comienza la revision de duplicados de la tabla {}'.format(nombre_tabla))
    contador_errores=0
    df_revisar=df[lista_campos].copy()
    # Recorremos todos los elementos del df y llamamos a la funcion que revisa duplicados
    for index, row in df_revisar.iterrows():
        contador_errores+=revisa_duplicados(df_revisar,lista_campos,list(row))
    # Mostramos en pantalla la cantidad total de errores
    if contador_errores>0: escribe_reporta(archivo_reporte,'La revision de duplicados arrojó la siguiente cantidad de errores: {}'.format(contador_errores))


def revisa_completitud(nombre_tabla_ant,nombre_tabla_post,campos,tabla_ant=pd.DataFrame(),tabla_post=pd.DataFrame()):
    print('Comienza la revision de completitud de la tabla {}'.format(nombre_tabla_post))
    # Extraemos todas las combinaciones que deben estar, a partir de la tabla_ant
    if tabla_ant.empty: 
        try: tabla_ant=diccionario_tablas_primas[nombre_tabla_ant][0] 
        except: tabla_ant=diccionario_tablas_siniestros[nombre_tabla_ant][0]
    if tabla_post.empty: 
        try: tabla_post=diccionario_tablas_primas[nombre_tabla_post][0]
        except: tabla_post=diccionario_tablas_siniestros[nombre_tabla_post][0]
    tabla_ant_unique=tabla_ant[campos].drop_duplicates().reset_index(drop=True)
    tabla_post_unique=tabla_post[campos].drop_duplicates().reset_index(drop=True)
    tabla_cruce=tabla_ant_unique.merge(tabla_post_unique,how='left',on=campos,indicator=True)
    tabla_diferencias=tabla_cruce[tabla_cruce['_merge']=='left_only']
    if tabla_diferencias.shape[0]>1: escribe_reporta(archivo_reporte,'Existen registros no parametrizados. REVISAR!\n{}'.format(tabla_diferencias[campos]))


def check_parametros_primas():
    print('COMIENZA EL CHEQUEO DE LAS TABLAS DE PARAMETROS PARA EL CALCULO DE PRIMAS DE REASEGURO:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # Revisamos duplicados para cada una de las tablas de parametrizaciones
    for tabla in list(diccionario_tablas_primas):
        revisa_duplicados_all(diccionario_tablas_primas[tabla][0],diccionario_tablas_primas[tabla][1],tabla)
    # Extraemos de la base de tasas de reaseguro todas las tablas que debemos chequear
    for nombre_tabla in reaseguradores['TABLA'].unique():
        if str(nombre_tabla) not in ['nan','NaT','NaN']:
            tabla=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name=nombre_tabla)
            campos=list(tabla.columns)
            campos.remove('TASA O PRIMA DE REASEGURO')
            revisa_duplicados_all(tabla,campos,nombre_tabla)
    # Extraemos de las tablas de cumulos las tablas de parametrizaciones que debemos revisar
    for tabla in [cumulos_individuales,cumulos_contrato,cumulos_excedente]:
        for nombre_tabla in tabla['LIMITE O RETENCION'].unique():
            if (str(nombre_tabla) not in ['nan','NaT','NaN'])&(isinstance(nombre_tabla, str)) :
                tabla=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name=nombre_tabla)
                campos=list(tabla.columns)
                campos.remove('LIMITE O RETENCION')
                revisa_duplicados_all(tabla,campos,nombre_tabla)    
    # Revisamos completitud de las tablas de parametros
    revisa_completitud('contrato_cob','parametros_contratos',['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'])
    revisa_completitud('parametros_contratos','reaseguradores',['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO'])
    cesiones_cum_ind=parametros_contratos[~parametros_contratos['RIESGO LIMITE INDIVIDUAL'].isnull()].reset_index(drop=True)
    revisa_completitud('','cumulos_individuales',['CONTRATO REASEGURO','RIESGO LIMITE INDIVIDUAL'],cesiones_cum_ind)
    cesiones_cum_cont=parametros_contratos[~parametros_contratos['RIESGO LIMITE CONTRATO'].isnull()].reset_index(drop=True)
    revisa_completitud('','cumulos_contrato',['CONTRATO REASEGURO','RIESGO LIMITE CONTRATO'],cesiones_cum_cont)
    cesiones_cum_exced=parametros_contratos[~parametros_contratos['RIESGO RETENCION EXCEDENTE'].isnull()].reset_index(drop=True)
    revisa_completitud('','cumulos_excedente',['CONTRATO REASEGURO','RIESGO RETENCION EXCEDENTE'],cesiones_cum_exced)
    tasas_reaseguro_grouped=reaseguradores.groupby(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO']).agg(SUMA_PARTICIPACIONES=pd.NamedAgg('PARTICIPACION DEL REASEGURADOR', aggfunc=sum)).reset_index()
    tasas_reaseguro_grouped['SUMA_PARTICIPACIONES']=tasas_reaseguro_grouped['SUMA_PARTICIPACIONES'].round(5)
    tasas_reaseguro_grouped_less_1=tasas_reaseguro_grouped[tasas_reaseguro_grouped['SUMA_PARTICIPACIONES']<1][['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','SUMA_PARTICIPACIONES']]
    escribe_reporta(archivo_reporte,'Revisar las siguientes participaciones de reaseguro:\n{}'.format(tasas_reaseguro_grouped_less_1)) if tasas_reaseguro_grouped_less_1.shape[0]>0 else print('Participaciones de reaseguro OK')


def check_parametros_siniestros():
    print('COMIENZA EL CHEQUEO DE LAS TABLAS DE PARAMETROS PARA EL CALCULO DE SINIESTROS CEDIDOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    # Revisamos duplicados para cada una de las tablas de parametrizaciones
    for tabla in list(diccionario_tablas_siniestros):
        revisa_duplicados_all(diccionario_tablas_siniestros[tabla][0],diccionario_tablas_siniestros[tabla][1],tabla)
    # Extraemos de las tablas de cumulos las tablas de parametrizaciones que debemos revisar
    for tabla in [cumulos_individuales_siniestros,cumulos_excedente_siniestros]:
        for nombre_tabla in tabla['LIMITE O RETENCION'].unique():
            if (str(nombre_tabla) not in ['nan','NaT','NaN'])&(isinstance(nombre_tabla, str)) :
                tabla=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name=nombre_tabla)
                campos=list(tabla.columns)
                campos.remove('LIMITE O RETENCION')
                revisa_duplicados_all(tabla,campos,nombre_tabla)    
    # Revisamos completitud de las tablas de parametros
    revisa_completitud('contrato_cob','parametros_contratos',['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'])
    revisa_completitud('parametros_contratos','reaseguradores',['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO'])
    cesiones_cum_ind=parametros_contratos[~parametros_contratos['RIESGO LIMITE INDIVIDUAL SINIESTROS'].isnull()].reset_index(drop=True)
    revisa_completitud('','cumulos_individuales_siniestros',['CONTRATO REASEGURO','RIESGO LIMITE INDIVIDUAL SINIESTROS'],cesiones_cum_ind)
    cesiones_cum_exced=parametros_contratos[~parametros_contratos['RIESGO RETENCION EXCEDENTE SINIESTROS'].isnull()].reset_index(drop=True)
    revisa_completitud('','cumulos_excedente_siniestros',['CONTRATO REASEGURO','RIESGO RETENCION EXCEDENTE SINIESTROS'],cesiones_cum_exced)
    cesion_reaseguradores_grouped=reaseguradores.groupby(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','VIGENCIA CONTRATO']).agg(SUMA_PARTICIPACIONES=pd.NamedAgg('PARTICIPACION DEL REASEGURADOR', aggfunc=sum)).reset_index()
    cesion_reaseguradores_grouped['SUMA_PARTICIPACIONES']=cesion_reaseguradores_grouped['SUMA_PARTICIPACIONES'].round(5)
    cesion_reaseguradores_grouped_less_1=cesion_reaseguradores_grouped[cesion_reaseguradores_grouped['SUMA_PARTICIPACIONES']<1][['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','SUMA_PARTICIPACIONES']]
    escribe_reporta(archivo_reporte,'Revisar las siguientes participaciones de reaseguro:\n{}'.format(cesion_reaseguradores_grouped_less_1)) if cesion_reaseguradores_grouped_less_1.shape[0]>0 else print('Participaciones de reaseguro OK')


def pre_procesamiento(tipo_calculo=tipo_calculo):
    """
    # Funcion de pre-procesamiento de la data
    # Corresponden a modificaciones iniciales a las bbdd antes de hacer el calculo generico
    """
    escribe_reporta(archivo_reporte,'COMIENZA LA LECTURA DE LAS BASES DE DATOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    if tipo_calculo=='Prima de Reaseguro':
        # Inputs de otras fuentes
        # polizas_pyme=pd.read_csv(ruta_pyme+'1. Inputs Auxiliares\\Polizas Pyme\\'+'Polizas Pyme '+str(periodo)+'.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
        polizas_pyme=pd.read_csv(ruta_pyme+'1. Inputs Auxiliares\\Polizas Pyme\\'+'Polizas Pyme.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
        if clasificacion_contrato=='I&S':regiones=pd.read_csv(ruta_regiones+'1. Inputs Auxiliares\\Regiones I&S\\'+'Regiones '+str(periodo)+'.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
        # regiones=pd.read_csv(ruta_regiones+'1. Inputs Auxiliares\\Regiones I&S\\'+'Regiones.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
        cti=pd.read_csv(ruta_otros+'1. Inputs Auxiliares\\Otros\\'+'CTI.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
        if contrato=='Complementario UC': uso_seguro_com_uc = pd.read_csv(f'{ruta_uso_seguro}1. Inputs Auxiliares\\Com UC\\COM UC Uso del Seguro Hist {periodo}.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
        if (tipo_contrato=='Vida')&(contrato not in ['K-Fijo','Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO']
        elif (tipo_contrato=='Vida')&(contrato in ['Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_INICIO_CRED','FECHA_FIN_CRED']
        elif (tipo_contrato=='Vida')&(contrato=='K-Fijo'): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
        elif clasificacion_contrato =='Cesantia PU': cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
        elif (tipo_contrato=='Generales')&('Incendio y Sismo' in contrato): cols_date,cols_date_ges=['FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FECHA_EFECTO','FECHA_VENCIMIENTO']
        elif (tipo_contrato=='Generales')&(contrato=='Cesantia PR'): cols_date=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION']
        # LECTURA DE BASES DE DATOS IAXIS
        if base_iaxis==1: 
            df_iaxis=pd.read_csv(ruta_input+archivo_input,sep=separador_input,decimal=decimal_input,parse_dates=cols_date,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
            for col in cols_date:
                if df_iaxis[col].dtype!='datetime64[ns]': df_iaxis[col]=pd.to_datetime(df_iaxis[col],format = '%d-%m-%Y', errors='coerce')   
            df_iaxis['IPRIANU']=round(df_iaxis['IPRIANU'],4)
            df_iaxis['ICAPITAL']=round(df_iaxis['ICAPITAL'],4)
            df_iaxis['BASE']='IAXIS'
            if 'CANAL_VENTA' in df_iaxis.columns: df_iaxis=df_iaxis.merge(canales_venta,how='left',on=['CANAL_VENTA'])
            if contrato=='Incendio y Sismo Licitado': df_iaxis['FECHA_VENCIMIENTO']=np.where(df_iaxis['FECHA_VENCIMIENTO']==datetime.datetime(2023,11,30),df_iaxis['FECHA_VENCIMIENTO']+datetime.timedelta(days=1),df_iaxis['FECHA_VENCIMIENTO'])
            if 'NRO_OPERACION' not in df_iaxis.columns:df_iaxis['NRO_OPERACION']=0
            else: df_iaxis['NRO_OPERACION']=df_iaxis['NRO_OPERACION'].fillna(0)
            if contrato=='Complementario UC': df_iaxis['USO SEGURO']= np.where((df_iaxis['SSEGURO'].isin(uso_seguro_com_uc['SSEGURO']))&(df_iaxis['MOTIVO_BAJA']==306),1,0)
            if 'PERIOD_TASA' in df_iaxis.columns:df_iaxis['TASA_CRED']=np.where(df_iaxis['PERIOD_TASA']==12,df_iaxis['TASA_CRED']/100,np.where(df_iaxis['PERIOD_TASA']==1,(1+df_iaxis['TASA_CRED']/100)**(1/12)-1,df_iaxis['TASA_CRED']/100))
            if 'FECHA_CONTABILIZACION_ANULACION' in df_iaxis.columns:df_iaxis['PERIODO_CONTABILIZACION']=df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.year*100+df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.month
            df_iaxis['CTI']=np.where(df_iaxis['PRODUCTO'].isin(list(cti['PRODUCTO'])),1,0)
            # VALIDADOR DE DATA 1: UNICIDAD DENTRO DE LA BASE IAXIS
            cols_dup_iaxis = ['SSEGURO','COD_COB','NRIESGO'] if tipo_contrato=='Vida' else ['SSEGURO','COD_COB']
            duplicados_iaxis=df_iaxis.loc[df_iaxis.duplicated(subset=cols_dup_iaxis,keep=False)]
            if not (duplicados_iaxis.empty): 
                escribe_reporta(archivo_reporte,'El dataframe iAxis de entrada contiene {} registros duplicados. Revisar!'.format(duplicados_iaxis.shape[0]))
                duplicados_iaxis.to_csv(ruta_output+'0.1.1 Duplicados iAxis.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
                df_iaxis=df_iaxis.drop_duplicates()
                duplicados_drop_iaxis=df_iaxis.loc[df_iaxis.duplicated(subset=cols_dup_iaxis,keep=False)]
                if not (duplicados_drop_iaxis.empty): 
                    escribe_reporta(archivo_reporte,'Luego de eliminar duplicados, la base input de iAxis contiene {} registros duplicados. Revisar!'.format(duplicados_drop_iaxis.shape[0]))
                    duplicados_drop_iaxis.to_csv(ruta_output+'0.1.2 Duplicados iAxis Drop.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
            df_iaxis=df_iaxis.merge(estados_iaxis[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
            df_iaxis=df_iaxis[df_iaxis['APLICA ESTADO']==1].copy()
            df_iaxis=df_iaxis.merge(polizas_pyme,how='left',on=['POLIZA'])
            df_iaxis['TIPO_POLIZA_LETRA']=np.where(df_iaxis['TIPO_POLIZA_LETRA'].isnull(),np.where(df_iaxis['TIPO_POLIZA']==1,'I','C'),df_iaxis['TIPO_POLIZA_LETRA'])
            if contrato =='Incendio y Sismo Licitado':
                regs_excluir=list(pd.read_csv(ruta_otros+'1. Inputs Auxiliares\\Otros\\'+'Anulaciones I&S Licitada.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)['SSEGURO'])
                df_iaxis=df_iaxis[~df_iaxis['SSEGURO'].isin(regs_excluir)]
            if contrato =='Desgravamen No Licitado':
                saldos_insolutos_detalle=pd.read_csv(ruta_si+'1. Inputs Auxiliares\\Saldos Insolutos\\'+'Saldos Insolutos '+str(periodo)+'.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
                saldos_insolutos_detalle['NRO_OPERACION']=saldos_insolutos_detalle['NRO_OPERACION'].astype(str).str.replace('K','').astype(float)
                df_iaxis=df_iaxis.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
        # LECTURA DE BASES DE DATOS GES
        if base_ges==1: 
            df_ges=pd.read_csv(ruta_input+archivo_input_ges,sep=separador_input,decimal=decimal_input,parse_dates=cols_date_ges,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
            for col in cols_date_ges:
                if df_ges[col].dtype!='datetime64[ns]': df_ges[col]=pd.to_datetime(df_ges[col],format = '%d-%m-%Y', errors='coerce')            
            df_ges['CTI']=0
            if contrato=='Incendio y Sismo Licitado': df_ges['FECHA_VENCIMIENTO']=np.where(df_ges['FECHA_VENCIMIENTO']==datetime.datetime(2023,11,30),df_ges['FECHA_VENCIMIENTO']+datetime.timedelta(days=1),df_ges['FECHA_VENCIMIENTO'])
            if 'PERIOD_TASA' in df_ges.columns:df_ges['TASA_CRED']=np.where(df_ges['PERIOD_TASA']=='M',df_ges['TASA_CRED']/100,np.where(df_ges['PERIOD_TASA']=='A',(1+df_ges['TASA_CRED']/100)**(1/12)-1,df_ges['TASA_CRED']/100))
            if clasificacion_contrato=='Cesantia PU': df_ges=corrige_tasas_ges(df_ges)
            # VALIDADOR DE DATA 1: UNICIDAD DENTRO DE LA BASE GES
            duplicados_ges=df_ges.loc[df_ges.duplicated(subset=[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','COD_COB'],keep=False)]
            if not (duplicados_ges.empty): 
                escribe_reporta(archivo_reporte,'El dataframe GES de entrada contiene {} registros duplicados. Revisar!'.format(duplicados_ges.shape[0]))
                duplicados_ges.to_csv(ruta_output+'0. Duplicados GES.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
                df_ges=df_ges.drop_duplicates()
                duplicados_drop_ges=df_ges.loc[df_ges.duplicated(subset=['POLIZA','CERTIFICADO','NRO_OPERACION','COD_COB'],keep=False)]
                if not (duplicados_drop_ges.empty): 
                    escribe_reporta(archivo_reporte,'Luego de eliminar duplicados, la base input de GES contiene {} registros duplicados. Revisar!'.format(duplicados_drop_ges.shape[0]))
                    duplicados_drop_ges.to_csv(ruta_output+'0. Duplicados GES Drop.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
            if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'): df_ges['FECHA_ANULACION']=pd.to_datetime(np.where((df_ges['FECHA_VENCIMIENTO']>=fecha_inicio_mes)&(df_ges['FECHA_VENCIMIENTO']<=fecha_cierre),df_ges['FECHA_VENCIMIENTO'].astype(str),''), format = '%Y-%m-%d', errors='coerce')
            elif clasificacion_contrato =='Cesantia PU': 
                ############ CRITERIO LEY JACKSON: CREDITOS PREPAGADOS CUYA FEC_INICIO<2021-11-01 NO DEVUELVEN PRIMA AL ASEGURADO
                df_ges['FEC AUX NA']=0
                df_ges['FEC AUX NA']=pd.to_datetime(df_ges['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
                df_ges['FECHA_ANULACION']=np.where(~df_ges['FECHA_RENUNCIA'].isnull(),df_ges['FECHA_RENUNCIA'],np.where((df_ges['FECHA_EFECTO']<datetime.datetime(2021,11,1))&(~df_ges['FECHA_PREPAGO'].isnull()),df_ges['FEC AUX NA'],df_ges['FECHA_PREPAGO']))
                df_ges=df_ges.drop(columns=['FEC AUX NA'],axis=1)
                df_ges['PERIODO_CONTABILIZACION']=np.where(df_ges['FECHA_ANULACION'].isnull(),np.nan,np.maximum(df_ges['PERIODO_CONTABILIZACION'],df_ges['FECHA_ANULACION'].dt.year*100+df_ges['FECHA_ANULACION'].dt.month))
                df_ges['FECHA_CONTABILIZACION_ANULACION']=pd.to_datetime(df_ges['PERIODO_CONTABILIZACION'],format='%Y%m', errors='coerce')+ MonthEnd(0)
            elif contrato=='K-Fijo':
                df_ges['FEC AUX NA']=0
                df_ges['FEC AUX NA']=pd.to_datetime(df_ges['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
                df_ges['FECHA_ANULACION']=np.where(~df_ges['FECHA_RENUNCIA'].isnull(),df_ges['FECHA_RENUNCIA'],np.where(~df_ges['FECHA_PREPAGO'].isnull(),df_ges['FECHA_PREPAGO'],np.where(df_ges['FECHA_FIN_VIGENCIA']==df_ges['FECHA_VENCIMIENTO'],df_ges['FEC AUX NA'],df_ges['FECHA_FIN_VIGENCIA'])))
                df_ges=df_ges.drop(columns=['FEC AUX NA'],axis=1)
                df_ges['PERIODO_CONTABILIZACION']=np.where(df_ges['FECHA_ANULACION'].isnull(),np.nan,np.maximum(df_ges['PERIODO_CONTABILIZACION'],df_ges['FECHA_ANULACION'].dt.year*100+df_ges['FECHA_ANULACION'].dt.month))
                df_ges['FECHA_CONTABILIZACION_ANULACION']=pd.to_datetime(df_ges['PERIODO_CONTABILIZACION'],format='%Y%m', errors='coerce')+ MonthEnd(0)
            df_ges=df_ges.merge(forma_pago,how='left',on='FORMA_PAGO')
            df_ges['TIPO_POLIZA_LETRA']=df_ges['TIPO_POLIZA']
            df_ges['TIPO_POLIZA']=np.where(df_ges['TIPO_POLIZA_LETRA']=='C',2,1)
            df_ges['FINI_RENOV_ANUAL'],df_ges['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_ges, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo)
            df_ges['BASE']='GES'
            ########## REVISAR QUE PASA CON I&S
            if tipo_contrato=='Vida':df_ges['IPRIANU']=df_ges['IPRIANU']*df_ges['FACTOR ANUALIZACION']
            df_ges=df_ges.merge(planes_ges,how='left',on=['PRODUCTO','PLAN_DESC'])
            df_ges['COD_PLAN']=df_ges['COD_PLAN'].fillna(0)
            df_ges=df_ges.merge(estados_ges[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
            df_ges=df_ges[df_ges['APLICA ESTADO']==1].copy()
            if clasificacion_contrato =='Cesantia PU': 
                df_ges=df_ges[(df_ges['DESGPRPAG']>0)|(df_ges['FECHA_ANULACION'].isnull())]
                df_ges=df_ges.drop(columns=['DESGPRPAG'],axis=1)
                df_ges['TIPO_BAJA']=np.where(~df_ges['FECHA_RENUNCIA'].isnull(),'Renuncia',np.where(~df_ges['FECHA_PREPAGO'].isnull(),'Prepago',''))
            if 'POLVIGENTE' in df_ges.columns: df_ges=df_ges[~df_ges['POLVIGENTE'].isin([9])]
            # ACA VEMOS QUE CONSIDERAR COMO ICAPITAL EN CASO DEL CONTRATO DESGRAVAMEN NO LICITADO
            if contrato=='Desgravamen No Licitado':
                # df_ges['ICAPITAL']=np.where(df_ges['POLCFIORI'].isnull(),df_ges['POLASECFI'],df_ges['POLCFIORI'])
                df_ges['ICAPITAL']=df_ges['POLASECFI']
                df_ges.drop(columns=['POLCFIORI','POLASECFI'],axis=1,inplace=True)
                df_ges['NRO_OPERACION']=pd.to_numeric(df_ges['NRO_OPERACION'],errors = 'coerce')
                df_ges=df_ges.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
            if contrato=='Incendio y Sismo No Licitado':
                riesgos_industriales=pd.read_csv(ruta_otros+'1. Inputs Auxiliares\\Otros\\'+'Riesgos Industriales.txt',sep=';',decimal='.')
                df_ges=df_ges.merge(riesgos_industriales,how='left',on=['POLIZA','RUT_CONTRATANTE','CERTIFICADO'])
                df_ges=df_ges[df_ges['RI'].isnull()]
            if contrato=='Incendio y Sismo No Licitado RI':
                riesgos_industriales=pd.read_csv(ruta_otros+'1. Inputs Auxiliares\\Otros\\'+'Riesgos Industriales.txt',sep=';',decimal='.')
                df_ges=df_ges.merge(riesgos_industriales,how='inner',on=['POLIZA','RUT_CONTRATANTE','CERTIFICADO'])
        # JUNTAMOS LAS BASES DEPENDIENDO DE CUALES EXISTEN
        if (base_iaxis==1)&(base_ges==1):
            df_0_0=pd.concat([df_iaxis,df_ges],axis=0)
        elif base_iaxis==1:
            df_0_0=df_iaxis
        elif base_ges==1:
            df_0_0=df_ges
        # CALCULOS DE VARIABLES EXTRAS Y CAMBIOS DE NOMBRE DE ALGUNAS VARIABLES
        escribe_reporta(archivo_reporte,'El dataframe input posee una prima neta de {}'.format(np.nansum(df_0_0['IPRIANU'])))
        df_0_0['NRO_OPERACION']=df_0_0['NRO_OPERACION'].fillna(0)
        if 'CANAL_DESC' in df_0_0.columns: df_0_0['CANAL_DESC']=df_0_0['CANAL_DESC'].str.strip()
        df_0_1=df_0_0.merge(cobs_ges[['COD_COB','COB_GES']],how='left',on=['COD_COB'],suffixes=['','_x'])
        df_0_1['COB_GES']=np.where(df_0_1['COB_GES'].isnull(),df_0_1['COD_COB'],df_0_1['COB_GES'])
        df_0_1.rename(columns={'COD_PLAN':'PLAN','IPRIANU':'PRIMA NETA ANUAL','COB_GES':'CODIGO COBERTURA','COD_COB':'CODIGO COBERTURA IAXIS'},inplace=True)
        df_0_1['POL_PROD']=np.where((df_0_1['TIPO_POLIZA_LETRA']=='I')|(df_0_1['CTI']==1),df_0_1['PRODUCTO'],df_0_1['POLIZA'])
        df_0_1['FECHA CIERRE']=fecha_cierre
        df_0_1['FECHA CIERRE']=df_0_1['FECHA CIERRE'].astype(df_0_1['FECHA_EFECTO'].dtype)
        if 'FEC_NAC' in df_0_1.columns: 
            escribe_reporta(archivo_reporte,'Calculando edad de ingreso')
            df_0_1['EDAD INGRESO'],df_0_1['ISSUE EDAD INGR']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],df_0_1['FECHA_EFECTO'],edad_casos_perdidos,108,reporta_issues=1)
        # CALCULOS ESPECIFICOS POR CADA CONTRATO
        # CALCULOS DE FECHAS DE INICIO/FIN DE EXPOSICION: SE DIFERENCIAN ENTRE PRIMA UNICA (CESANTIA Y K-FIJO) DEL RESTO
        if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'):
            df_0_1['FINI_RENOV_ANUAL'],df_0_1['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_0_1, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo,0)
            # df_0_1=df_0_1.drop(columns=['FEC AUX NA'],axis=1)
            if contrato=='Complementario UC': 
                df_0_1['FECHA_ANULACION_EXPOSICION'] = np.where((df_0_1['FECHA_ANULACION'].isnull())|(df_0_1['MOTIVO_BAJA']==242)|(df_0_1['USO SEGURO']==1)|((df_0_1['MOTIVO_BAJA']==324)&(df_0_1['SUBMOTIVO_BAJA']!=321)),df_0_1['FECHA_ANULACION'],np.where((df_0_1['MOTIVO_BAJA']==324)&(df_0_1['SUBMOTIVO_BAJA']==321),df_0_1['FECHA_ANULACION']-datetime.timedelta(days=30),df_0_1['FECHA_EFECTO']))
                df_0_1['FECHA_ANULACION_EXPOSICION'] = np.where(df_0_1['FECHA_ANULACION_EXPOSICION']<df_0_1['FECHA_EFECTO'],df_0_1['FECHA_EFECTO'],df_0_1['FECHA_ANULACION_EXPOSICION'])
                df_0_1['FECHA FIN EXP']=np.where(~df_0_1['FECHA_ANULACION_EXPOSICION'].isnull(),df_0_1['FECHA_ANULACION_EXPOSICION'],np.where(df_0_1['FECHA_VENCIMIENTO'].isnull(),df_0_1['FFIN_RENOV_ANUAL'],df_0_1['FECHA_VENCIMIENTO']))
            else: 
                df_0_1['FECHA FIN EXP']=np.where(~df_0_1['FECHA_ANULACION'].isnull(),df_0_1['FECHA_ANULACION'],np.where(df_0_1['FECHA_VENCIMIENTO'].isnull(),df_0_1['FFIN_RENOV_ANUAL'],df_0_1['FECHA_VENCIMIENTO']))
        else:
            df_0_1['FEC AUX NA']=0
            df_0_1['FEC AUX NA']=pd.to_datetime(df_0_1['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
            df_0_1['FECHA_ANULACION']=np.where(df_0_1['FECHA_ANULACION']<=fecha_cierre,df_0_1['FECHA_ANULACION'],df_0_1['FEC AUX NA'])
        # CALCULOS GENERICOS PARA BASES DE VIDA PRIMA RECURRENTE
        if (tipo_contrato=='Vida')&(contrato!='K-Fijo'): 
            df_0_1['EXPOSICION MENSUAL']=calcula_exposicion(df_0_1,'FECHA_EFECTO','FECHA FIN EXP',dias_exposicion,fecha_inicio_mes,fecha_cierre)
            df_0_1['TIPO ASEGURADO']=np.where((df_0_1['RUT'].isnull())|(df_0_1['RUT']==df_0_1['RUT_CONTRATANTE']),'Titular','Adicional')
            escribe_reporta(archivo_reporte,'Calculando edad de renovacion')
            if contrato == 'Desgravamen No Licitado': df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_inicio_mes,edad_casos_perdidos,108,reporta_issues=1)
            else: df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],df_0_1['FINI_RENOV_ANUAL'],edad_casos_perdidos,108,reporta_issues=1)
            df_0_2=df_0_1.merge(meses_renta,how='left',on=['CODIGO COBERTURA'],suffixes=['','_x'])
            if contrato=='Desgravamen No Licitado': df_0_2['MONTO ASEGURADO']=np.where(df_0_2['MESES RENTA']==1,1,(1-(1+tdm_mensual)**(-df_0_2['MESES RENTA']))/tdm_mensual)*df_0_2['ICAPITAL']
            else: df_0_2['MONTO ASEGURADO']=df_0_2['ICAPITAL']
            df_0_3=df_0_2.merge(saldo_insoluto,how='left',on=['PRODUCTO','CODIGO COBERTURA','BASE'],suffixes=['','_x'])
            df_0_3['APLICA CALCULO SALDO INSOLUTO']=df_0_3['APLICA CALCULO SALDO INSOLUTO'].fillna(0)
            # ESPECIFICO DE DESG NL: PRODUCTOS CON CAPITAL COMO EL SALDO INSOLUTO DEBEN CALCULARSE
            if contrato in ['Desgravamen No Licitado']:
                df_0_4=df_0_3[df_0_3['APLICA CALCULO SALDO INSOLUTO']==1].copy()
                df_0_4_resto=df_0_3[df_0_3['APLICA CALCULO SALDO INSOLUTO']==0].copy()
                df_0_4['FECHA_FIN_CRED']=np.where(df_0_4['BASE']=='GES',np.maximum(df_0_4['FECHA_VENCIMIENTO'],df_0_4['FECHA_FIN_CRED']),df_0_4['FECHA_VENCIMIENTO'])
                df_0_4['NCUOTAS']=((df_0_4['FECHA_FIN_CRED']-df_0_4['FECHA_EFECTO']).dt.days/365*12).round(0)
                df_0_4['NCUOTAS FALTANTES']=((df_0_4['FECHA_FIN_CRED']-fecha_cierre).dt.days/365*12).round(0)
                df_0_4['PERIODO_EFECTO']=df_0_4['FECHA_EFECTO'].dt.year*100+df_0_4['FECHA_EFECTO'].dt.month
                df_0_4=completa_campo_total(df_0_4,'TASA_CRED',[['PRODUCTO','PERIODO_EFECTO'],['PERIODO_EFECTO']])
                df_0_4['SALDO INSOLUTO CALCULADO']=df_0_4['ICAPITAL']*(1-(1+df_0_4['TASA_CRED_FINAL'])**(-df_0_4['NCUOTAS FALTANTES']))/(1-(1+df_0_4['TASA_CRED_FINAL'])**(-df_0_4['NCUOTAS']))
                df_0_4['MONTO ASEGURADO']=np.where(df_0_4['SALDO_INSOLUTO']>0,df_0_4['SALDO_INSOLUTO'],np.maximum(df_0_4['SALDO INSOLUTO CALCULADO'],0)) 
                df_0_5=pd.concat([df_0_4,df_0_4_resto],axis=0)
                df_final_0=df_0_5.copy()
            else: df_final_0=df_0_3.copy()
            if contrato in ['Multisocios']:
                df_0_3['FECHA_FIN_CRED']=np.where(df_0_3['BASE']=='GES',np.maximum(df_0_3['FECHA_VENCIMIENTO'],df_0_3['FECHA_FIN_CRED']),df_0_3['FECHA_VENCIMIENTO'])
                df_0_3['NCUOTAS']=((df_0_3['FECHA_FIN_CRED']-df_0_3['FECHA_EFECTO']).dt.days/365*12).round(0)
                df_0_3['NCUOTAS FALTANTES']=((df_0_3['FECHA_FIN_CRED']-fecha_cierre).dt.days/365*12).round(0)
                df_0_3['PERIODO_EFECTO']=df_0_3['FECHA_EFECTO'].dt.year*100+df_0_3['FECHA_EFECTO'].dt.month
                df_0_3=completa_campo_total(df_0_3,'TASA_CRED',[['PRODUCTO','PERIODO_EFECTO'],['PERIODO_EFECTO']])
                df_0_3['SALDO INSOLUTO CALCULADO']=np.where(df_0_3['FECHA_FIN_CRED']<fecha_cierre,0,df_0_3['ICAPITAL']*(1-(1+df_0_3['TASA_CRED_FINAL'])**(-df_0_3['NCUOTAS FALTANTES']))/(1-(1+df_0_3['TASA_CRED_FINAL'])**(-df_0_3['NCUOTAS'])))
                df_0_3['MONTO ASEGURADO']=np.maximum(df_0_3['SALDO INSOLUTO CALCULADO'],0)
                df_final_0=df_0_3.copy()
        # CALCULOS PARA K-FIJO
        elif (tipo_contrato=='Vida')&(contrato=='K-Fijo'): 
            df_0_1['EXPOSICION MENSUAL']=1
            escribe_reporta(archivo_reporte,'Calculando edad de renovacion')
            df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_inicio_mes,edad_casos_perdidos,108,reporta_issues=1)
            df_0_1['PLAZO MESES']=np.maximum(1,round((df_0_1['FECHA_VENCIMIENTO']-df_0_1['FECHA_EFECTO']).dt.days/(365.25/12),0))
            df_0_1['MONTO ASEGURADO']=df_0_1['ICAPITAL']
            df_0_2=df_0_1[df_0_1['FECHA_EFECTO']<=fecha_cierre]
            df_final_0=df_0_2.copy()
        # CALCULOS CONTRATOS DE INCENDIO Y SISMO
        elif (tipo_contrato=='Generales')&('Incendio y Sismo' in contrato):
            df_0_1['MONTO ASEGURADO']=df_0_1['ICAPITAL']
            df_0_1['EXPOSICION MENSUAL']=calcula_exposicion(df_0_1,'FECHA_EFECTO','FECHA FIN EXP',dias_exposicion,fecha_inicio_mes,fecha_cierre)
            base_fecu=pd.read_csv(ruta_regiones+'1. Inputs Auxiliares\\Regiones I&S\\'+'Base Fecu I&S.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
            base_fecu['LLAVE_REGION']=np.where(base_fecu['LLAVE_GES'].isnull(),base_fecu['LLAVE_IAXIS'].fillna(0).astype('int64'),base_fecu['LLAVE_GES'])
            base_fecu=base_fecu[['LLAVE_REGION','COD_REGION']].drop_duplicates()
            duplicados_base_fecu=base_fecu.loc[base_fecu.duplicated(subset=['LLAVE_REGION'],keep=False)]
            if not duplicados_base_fecu.empty:
                escribe_reporta(archivo_reporte,'Base Fecu de I&S contiene {} registros duplicados. Revisar'.format(duplicados_base_fecu.shape[0]))
                duplicados_base_fecu.to_csv(ruta_output+'0. Duplicados Base FECU.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
            df_0_1['LLAVE_REGION']=np.where(df_0_1['BASE']=='IAXIS',df_0_1['SSEGURO'].astype('int64'),df_0_1['POLIZA'].astype('int64').astype('string')+'-'+df_0_1['RUT_CONTRATANTE'].astype('int64').astype('string')+'-'+df_0_1['CERTIFICADO'].astype('int64').astype('string'))
            df_0_1['ENCUENTRA REGION BASE']=np.where((df_0_1['REGION']<1)|(df_0_1['REGION']>16)|(df_0_1['REGION'].isnull()),0,1)
            df_0_1_sin_region=df_0_1[(df_0_1['REGION']<1)|(df_0_1['REGION']>16)|(df_0_1['REGION'].isnull())].copy()
            df_0_1_con_region=df_0_1[(df_0_1['REGION']>=1)&(df_0_1['REGION']<=16)].copy()
            df_0_1_sin_region.drop(columns=['REGION'],axis=1,inplace=True)
            df_0_1_sin_region=df_0_1_sin_region.merge(base_fecu,how='left',on='LLAVE_REGION')
            df_0_1_sin_region.rename(columns={'COD_REGION':'REGION'},inplace=True)
            df_0_2=pd.concat([df_0_1_con_region,df_0_1_sin_region],axis=0)
            df_0_2_sin_region=df_0_2[(df_0_2['REGION']<1)|(df_0_2['REGION']>16)|(df_0_2['REGION'].isnull())].copy()
            df_0_2_con_region=df_0_2[(df_0_2['REGION']>=1)&(df_0_2['REGION']<=16)].copy()
            df_0_2_sin_region.drop(columns=['REGION'],axis=1,inplace=True)
            df_0_2_sin_region=df_0_2_sin_region.merge(regiones[['SSEGURO','REGION']],how='left',on=['SSEGURO'])
            df_0_3=pd.concat([df_0_2_con_region,df_0_2_sin_region],axis=0)
            df_0_3=df_0_3.merge(zonas[['REGION','ZONA']],how='left',on=['REGION'])
            df_0_3['ZONA']=df_0_3['ZONA'].fillna('VI')
            df_final_0=df_0_3.copy()
            if contrato=='Incendio y Sismo No Licitado':
                df_cruzar=df_0_3[(df_0_3['PRODUCTO']==712)&(df_0_3['CODIGO COBERTURA']==118)]
                df_cruzar=df_cruzar.drop_duplicates()
                df_casos_712=pd.read_csv(ruta_otros+'1. Inputs Auxiliares\\Otros\\'+'Casos 712.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
                lista_casos_712=list(df_casos_712['POLIZA'])
                casos_con_cob_120=list(df_0_3[(df_0_3['POLIZA'].isin(lista_casos_712))&(df_0_3['CODIGO COBERTURA'].isin([120,137,170]))]['POLIZA'])
                lista_casos_712_final=[pol for pol in lista_casos_712 if pol not in casos_con_cob_120]
                df_casos_712=df_casos_712[df_casos_712['POLIZA'].isin(lista_casos_712_final)]
                df_casos_712_cross=df_casos_712.merge(df_cruzar,how='inner',on=['POLIZA'],suffixes=['','_OLD'])
                df_casos_712_cross['CODIGO COBERTURA']=120
                df_casos_712_cross['CODIGO COBERTURA IAXIS']=120
                df_casos_712_cross['MARCA 712']=1
                df_casos_712_cross['MONTO ASEGURADO']=df_casos_712_cross['ICAPITAL']
                df_casos_712_cross=df_casos_712_cross.drop(columns=['ICAPITAL_OLD'],axis=1)
                df_0_4=pd.concat([df_0_3,df_casos_712_cross],axis=0)
                sum(df_0_4[(df_0_4['EXPOSICION MENSUAL']>0)&(df_0_4['PRODUCTO']==712)&(df_0_4['CODIGO COBERTURA']==120)&(df_0_4['MARCA 712']==1)]['ICAPITAL'])
                # df_0_5=df_0_4[~df_0_4['PRODUCTO'].isin([698,730])].copy()
                df_final_0=df_0_4.copy()
        # CALCULOS PARA CESANTIA PRIMA UNICA
        elif clasificacion_contrato =='Cesantia PU':
            df_0_1['EXPOSICION MENSUAL']=1
            escribe_reporta(archivo_reporte,'Calculando edad de renovacion')
            df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_inicio_mes,edad_casos_perdidos,108,reporta_issues=1)
            df_0_1['PLAZO MESES']=((df_0_1['FECHA_VENCIMIENTO']-df_0_1['FECHA_EFECTO']).dt.days/365*12).round(0)
            df_0_1['PLAZO SEMESTRES']=((df_0_1['FECHA_VENCIMIENTO']-df_0_1['FECHA_EFECTO']).dt.days/182.625).round(0)
            df_0_1['CAPITAL BASE 50']=(df_0_1['ICAPITAL']/50).astype(int)*50
            df_0_2=df_0_1.merge(parametros_cesantia,how='left',on='POLIZA')
            df_0_3=completa_campo_total(df_0_2,'TASA_CRED',[['POLIZA ORIGEN','PLAZO SEMESTRES','CAPITAL BASE 50'],['POLIZA ORIGEN','CAPITAL BASE 50'],['POLIZA ORIGEN']])
            df_0_3['CUOTA CREDITO']=df_0_3['ICAPITAL']*df_0_3['TASA_CRED_FINAL']/(1-(1+df_0_3['TASA_CRED_FINAL'])**(-np.maximum(1,df_0_3['PLAZO MESES'])))
            df_0_3['PERIODO EFECTO']=pd.DatetimeIndex(df_0_3['FECHA_EFECTO']).year*100+pd.DatetimeIndex(df_0_3['FECHA_EFECTO']).month
            df_0_3['MONTO ASEGURADO']=np.minimum(df_0_3['CUOTA CREDITO'],df_0_3['TOPE CUOTA'])*np.maximum(1,df_0_3['PLAZO MESES'])
            df_0_4=df_0_3[df_0_3['FECHA_EFECTO']<=fecha_cierre]
            df_0_5=df_0_4[df_0_4['FECHA_EFECTO']>=datetime.datetime(2012,1,1)].copy()
            # DEBEMOS REVISAR ESTE CRITERIO
            # df_0_5=df_0_4[df_0_4['POLIZA'].isin([5000000226,5000000227,5000000229,5000000230,5000000231,5000000269,5000000270,5000000272,5000000273,5000000274,5000000280,5000000287,5000000319,5000000328,5000000329,5000000331])]
            df_final_0=df_0_5.copy()
        # CALCULOS PARA CESANTIA PRIMA RECURRENTE
        elif (tipo_contrato=='Generales')&(contrato=='Cesantia PR'):
            escribe_reporta(archivo_reporte,'Calculando edad de renovacion')
            df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_inicio_mes,edad_casos_perdidos,108,reporta_issues=1)
            df_0_1['MONTO ASEGURADO']=df_0_1['ICAPITAL']
            df_0_1['EXPOSICION MENSUAL']=calcula_exposicion(df_0_1,'FECHA_EFECTO','FECHA FIN EXP',dias_exposicion,fecha_inicio_mes,fecha_cierre)
            df_final_0=df_0_1.copy()
        # EXPORTO EDADES CON ISSUES
        if 'ISSUE EDAD INGR' in df_final_0.columns:
            if sum(df_final_0['ISSUE EDAD INGR'])>0: df_final_0[df_final_0['ISSUE EDAD INGR']==1].to_csv(ruta_output+'0. Edades de Ingreso a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        if 'ISSUE EDAD RENOV' in df_final_0.columns:
            if sum(df_final_0['ISSUE EDAD RENOV'])>0: df_final_0[df_final_0['ISSUE EDAD RENOV']==1].to_csv(ruta_output+'0. Edades de Renovacion a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        escribe_reporta(archivo_reporte,'El dataframe input luego de ser pre-procesado posee una prima neta de {}'.format(np.nansum(df_final_0['PRIMA NETA ANUAL'])))

    elif (tipo_calculo=='Siniestros de Reaseguro')&(contrato=='Vida'):
        # cols dates varian segun el periodo de la data
        if periodo >= 202303:
            date_cols_pagados=['Fec. Inicio Vigencia', 'Fec. Fin Vigencia', 'Fecha Denuncia','Fec. Nacimiento', 'Fec. Siniestros','Fec. Defunción','Fec. Liquidación']
            date_cols_pendientes=['Fec Denuncia','Fec Inicio Vigencia', 'Fec Fin Vigencia','Fec Nacimiento','Fec  Siniestros', 'Fec Defunción']
        else:
            date_cols_pagados=['Fecha de Denuncia','Fecha de Siniestros','Fecha de Defunción','Fecha de Liquidación','Fecha de Nacimiento','Fec. Inicio Vigencia','Fec. Fin Vigencia']
            date_cols_pendientes=['Fecha de Denuncia','Fecha de Nacimiento','Fecha de Siniestros','Fecha de Defunción','Fec Inicio Vigencia','Fec Fin Vigencia']
        # Tratamiento Base Pagados
        base_pagados = pd.read_excel(io=f'{ruta_input}{str(periodo)[4:]}_{str(periodo)[:4]}_Base de Siniestros Cierre Life.xlsx',sheet_name='Pagados',parse_dates = date_cols_pagados, date_format="%d/%m/%Y")
        for col in date_cols_pagados:
            base_pagados[col] = pd.to_datetime(base_pagados[col], format='mixed')
        base_pagados['ESTADO SINIESTRO']='PAGADO'
        base_pagados['FECHA_VENCIMIENTO']=datetime.datetime(1900,1,1)
        base_pagados.columns=[x.upper() for x in base_pagados.columns]
        base_pagados.columns = [col.strip() for col in base_pagados.columns]
        base_pagados.columns = [unidecode(col) for col in base_pagados.columns]
        base_pagados.columns = [col.replace('  ',' ') for col in base_pagados.columns]
        base_pagados.columns = [col.replace('.','') for col in base_pagados.columns]
        if periodo >= 202303:
            base_pagados = base_pagados.rename(columns={'Ndeg SINIESTRO':'N_SINIESTRO','Ndeg POLIZA':'POLIZA','FEC NACIMIENTO':'FECHA DE NACIMIENTO','FEC DEFUNCION':'FECHA DE DEFUNCION','FEC SINIESTROS':'FECHA_SINIESTRO','FECHA DENUNCIA':'FECHA_DENUNCIO','FEC LIQUIDACION':'FECHA_PAGO_SANTANDER','FEC INICIO VIGENCIA':'INICIO_VIGENCIA','FEC FIN VIGENCIA':'FECHA_TERMINO_VIGENCIA','Ndeg OPERACION':'N_OPERACION','TIPO PAGO':'TIPO_PAGO','COD COBERTURA':'CODIGO COBERTURA','MONTO UF':'MONTO SINIESTRO UF','MONTO PESOS':'MONTO SINIESTRO CLP','COD FECU':'CODIGO FECU'})
        else:
            base_pagados = base_pagados.rename(columns={'No SINIESTROS':'N_SINIESTRO','No POLIZA':'POLIZA','FECHA DE SINIESTROS':'FECHA_SINIESTRO','FECHA DE DENUNCIA':'FECHA_DENUNCIO','FECHA DE LIQUIDACION':'FECHA_PAGO_SANTANDER','FEC INICIO VIGENCIA':'INICIO_VIGENCIA','FEC FIN VIGENCIA':'FECHA_TERMINO_VIGENCIA','No OPERACION':'N_OPERACION','TIPO PAGO':'TIPO_PAGO','COD COB':'CODIGO COBERTURA','MONTO EN UF':'MONTO SINIESTRO UF','MONTO EN PESOS':'MONTO SINIESTRO CLP','COD FECU':'CODIGO FECU'})
        # Tratamiento Base Pendientes
        base_pendientes = pd.read_excel(io=f'{ruta_input}{str(periodo)[4:]}_{str(periodo)[:4]}_Base de Siniestros Cierre Life.xlsx',sheet_name='Pendientes',parse_dates = date_cols_pendientes, date_format="%d/%m/%Y")
        for col in date_cols_pendientes:
            base_pendientes[col] = pd.to_datetime(base_pendientes[col], format='mixed')
        base_pendientes['ESTADO SINIESTRO']='PENDIENTE'
        base_pendientes['TIPO_PAGO']='Indemnizacion'
        base_pendientes['FECHA_VENCIMIENTO']=datetime.datetime(1900,1,1)
        base_pendientes['FECHA_PAGO_SANTANDER']=fecha_cierre
        base_pendientes.columns=[x.upper() for x in base_pendientes.columns]
        base_pendientes.columns = [col.strip() for col in base_pendientes.columns]
        base_pendientes.columns = [unidecode(col) for col in base_pendientes.columns]
        base_pendientes.columns = [col.replace('  ',' ') for col in base_pendientes.columns]
        base_pendientes.columns = [col.replace('.','') for col in base_pendientes.columns]
        if periodo >= 202303:
            base_pendientes = base_pendientes.rename(columns={'Ndeg SINIESTRO':'N_SINIESTRO','Ndeg POLIZA':'POLIZA','FEC NACIMIENTO':'FECHA DE NACIMIENTO','FEC DEFUNCION':'FECHA DE DEFUNCION','FEC SINIESTROS':'FECHA_SINIESTRO','FEC DENUNCIA':'FECHA_DENUNCIO','FEC INICIO VIGENCIA':'INICIO_VIGENCIA','FEC FIN VIGENCIA':'FECHA_TERMINO_VIGENCIA','Ndeg OPERACION':'N_OPERACION','TIPO PAGO':'TIPO_PAGO','COD COB':'CODIGO COBERTURA','MONTO UF':'MONTO SINIESTRO UF','MONTO PESOS':'MONTO SINIESTRO CLP','COD FECU':'CODIGO FECU'})
        else:
            base_pendientes = base_pendientes.rename(columns={'No SINIESTROS':'N_SINIESTRO','No POLIZA':'POLIZA','FECHA DE SINIESTROS':'FECHA_SINIESTRO','FECHA DE DENUNCIA':'FECHA_DENUNCIO','FECHA DE LIQUIDACION':'FECHA_PAGO_SANTANDER','FEC INICIO VIGENCIA':'INICIO_VIGENCIA','FEC FIN VIGENCIA':'FECHA_TERMINO_VIGENCIA','No OPERACION':'N_OPERACION','TIPO PAGO':'TIPO_PAGO','COD COB':'CODIGO COBERTURA','MONTO EN UF':'MONTO SINIESTRO UF','MONTO EN PESOS':'MONTO SINIESTRO CLP','COD FECU':'CODIGO FECU'})
        # Unimos las bases
        cols_select = ['N_SINIESTRO','POLIZA','RUT','DV','FECHA DE NACIMIENTO','FECHA DE DEFUNCION','FECHA_SINIESTRO','FECHA_DENUNCIO','FECHA_PAGO_SANTANDER','INICIO_VIGENCIA','FECHA_TERMINO_VIGENCIA','N_OPERACION','TIPO_PAGO','PRODUCTO','CODIGO COBERTURA','MONTO SINIESTRO UF','MONTO SINIESTRO CLP','FECHA_VENCIMIENTO','ESTADO SINIESTRO']
        df_0_0=pd.concat([base_pagados[cols_select],base_pendientes[cols_select]#,base_rentas[cols_select]
                          ],axis=0)
        df_0_1=df_0_0[(~df_0_0['MONTO SINIESTRO UF'].isnull())&(df_0_0['MONTO SINIESTRO UF']>0)].copy()
        df_0_1['TIPO_PAGO']=df_0_1['TIPO_PAGO'].fillna('Vacio')
        df_0_1['RUT']=np.where(df_0_1['RUT'].isnull(),df_0_1['N_SINIESTRO'],df_0_1['RUT'])
        df_0_1['EXPOSICION MENSUAL']=1
        df_0_1_nulls = df_0_1[df_0_1['FECHA DE NACIMIENTO'].isnull()].copy()
        df_0_1_notnulls = df_0_1[~df_0_1['FECHA DE NACIMIENTO'].isnull()].copy()
        df_0_1_nulls['EDAD SINIESTRO']=edad_casos_perdidos
        df_0_1_notnulls['EDAD SINIESTRO']=((df_0_1_notnulls['FECHA_SINIESTRO']-df_0_1_notnulls['FECHA DE NACIMIENTO']).dt.days/365.25).astype('int')
        df_0_2=pd.concat([df_0_1_nulls,df_0_1_notnulls],axis=0)
        df_0_2['CARENCIA']=np.nan
        df_0_2['DIAS DESDE INICIO VIGENCIA']=(df_0_2['FECHA_SINIESTRO']-df_0_2['INICIO_VIGENCIA']).dt.days
        df_0_2['FECHA CIERRE MES'] = fecha_cierre
        df_0_2['DIRECTO_TOTAL_UF'] = df_0_2['MONTO SINIESTRO UF']
        df_final_0 = df_0_2.copy()

    elif (tipo_calculo=='Siniestros de Reaseguro')&(contrato=='Generales'):
        if base_input_siniestros_generales == 'TUXPAN':
            date_cols=['CIERRE','FECHA_SINIESTRO','FECHA_DENUNCIA','FECHA_PAGO','MES_SINIESTRO','MES_DENUNCIA','MES_PAGO','INICIO_VIGENCIA','TERMINO_VIGENCIA','FECHA_INGRESO']
            # Input de TUXPAN
            base = pd.read_csv(ruta_input+archivo_input, sep=separador_input,decimal = decimal_input,parse_dates=date_cols,date_format=f'%d{separador_fechas_input}%m{separador_fechas_input}%Y',encoding='latin-1',low_memory=False)
            df_0_0=base.copy()
            df_0_0['POL_PROD']=np.where(df_0_0['TIPO2']=='C',df_0_0['N_POLIZA'],df_0_0['PRODUCTO'])
            df_0_1=df_0_0.rename(columns={'COBERTURA':'CODIGO COBERTURA','N_POLIZA':'POLIZA','BASE':'ESTADO SINIESTRO','RUT_ASEGURADO':'RUT_CONTRATANTE','SECUENCIAL':'CERTIFICADO'})
            df_0_2=df_0_1.merge(catastrofes,how='left',on=['CATASTROFE_DETALLE'])
            if sum(df_0_2['TIPO EVENTO'].isnull())>0:
                escribe_reporta(archivo_reporte,f'Cruce de Eventos CAT no encontró llave para todos los registros. Quedaron {sum(df_0_2["TIPO EVENTO"].isnull())} registros afuera')
                df_0_2[df_0_2['TIPO EVENTO'].isnull()]['CATASTROFE_DETALLE'].drop_duplicates().to_csv(ruta_output+'Catastrofes por Parametrizar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
                print(list(df_0_2[df_0_2['TIPO EVENTO'].isnull()]['CATASTROFE_DETALLE'].drop_duplicates()))
                sys.exit()
            df_0_2['MONTO SINIESTRO UF']=df_0_2['DIRECTO_TOTAL_UF']*np.where((df_0_2['CONCEPTO'].str.contains('Service'))|(df_0_2['CONCEPTO'].str.contains('Factura')),0,1)
            df_0_2['EXPOSICION MENSUAL']=1
            df_0_2['EDAD SINIESTRO']=18
            df_0_2['ESTADO SINIESTRO']=np.where(df_0_2['ESTADO SINIESTRO']=='SPAG','PAGADO','PENDIENTE')
            df_0_2=df_0_2.merge(parametros_cesantia,how='left',on='POLIZA')
            df_0_2['DIAS DESDE INICIO VIGENCIA']=(df_0_2['FECHA_SINIESTRO']-df_0_2['INICIO_VIGENCIA']).dt.days
            df_final_0=df_0_2.copy()
        if base_input_siniestros_generales == 'Beneficios':
            # Input de Beneficios
            # CAMPOS 202407 HACIA ATRAS
            names_cols_pagados = ['FOLIO O CARPETA','N_SINIESTRO','N_POLIZA','RUT_ASEGURADO','DV','DIRECCION','NOMBRE','NOMBRE_PRODUCTO','NOMBRE_COBERTURA_RECLAMADA','FECHA_SINIESTRO','FECHA_DENUNCIA','FECHA CIERRE CONTABLE','FECHA_PAGO','DIRECTO_TOTAL_CLP','DIRECTO_TOTAL_UF','RETENIDO_TOTAL_CLP','RETENIDO_TOTAL_UF','FECHA_INGRESO','INSTITUCION','DETALLE CONCEPTO','COBERTURA','ESTADO_LIQUIDACION','SUBESTADO','PRODUCTO','TC','POLIZA FLOTANTE','INICIO_VIGENCIA','TERMINO_VIGENCIA','N_OPERACION','TIPO1','GASTO','PERIODICIDAD','CATASTROFE_DETALLE','CODIGO_FECU','CONCEPTO','SUB CLASIFICACION','TIPO','SECUENCIAL','CLAS_PROFIT','CUOTA PAGADA','NRO IDENTIFICADOR REGISTRO','TIPO DE LIQUIDACION','TIPO DE BANCO','IAXIS','SSEGURO','APERTURA_COBERTURA_DE_CS']
            names_cols_pendientes = ['FOLIO O CARPETA','N_SINIESTRO','N_POLIZA','RUT_ASEGURADO','DV','DIRECCION','NOMBRE','NOMBRE_PRODUCTO','NOMBRE_COBERTURA_RECLAMADA','FECHA_SINIESTRO','FECHA_DENUNCIA','FECHA CIERRE CONTABLE','FECHA_PAGO','DIRECTO_TOTAL_CLP','DIRECTO_TOTAL_UF','RETENIDO_TOTAL_CLP','RETENIDO_TOTAL_UF','FECHA_INGRESO','INSTITUCION','CONCEPTO','COBERTURA','ESTADO_LIQUIDACION','SUBESTADO','PRODUCTO','TC','POLIZA FLOTANTE','INICIO_VIGENCIA','TERMINO_VIGENCIA','N_OPERACION','TIPO1','GASTO','PERIODICIDAD','CATASTROFE_DETALLE','CODIGO_FECU','SUB CLASIFICACION','TIPO','OBSERVACION','SECUENCIAL','CLAS_PROFIT','PROVISION TECNICA','NRO IDENTIFICADOR REGISTRO','TIPO DE LIQUIDACION','TIPO DE BANCO','IAXIS','SSEGURO','APERTURA_COBERTURA_DE_CS']
            # CAMPOS 202408
            # names_cols_pagados = ['FOLIO O CARPETA','N_SINIESTRO','N_POLIZA','RUT_ASEGURADO','DV','DIRECCION','NOMBRE','NOMBRE_PRODUCTO','NOMBRE_COBERTURA_RECLAMADA','CAUSAL','FECHA_SINIESTRO','FECHA_DENUNCIA','FECHA CIERRE CONTABLE','FECHA_PAGO','DIRECTO_TOTAL_CLP','DIRECTO_TOTAL_UF','RETENIDO_TOTAL_CLP','RETENIDO_TOTAL_UF','FECHA_INGRESO','INSTITUCION','DETALLE CONCEPTO','COBERTURA','ESTADO_LIQUIDACION','SUBESTADO','PRODUCTO','TC','POLIZA FLOTANTE','INICIO_VIGENCIA','TERMINO_VIGENCIA','N_OPERACION','TIPO1','GASTO','PERIODICIDAD','CATASTROFE_DETALLE','CODIGO_FECU','CONCEPTO','SUB CLASIFICACION','TIPO','SECUENCIAL','CLAS_PROFIT','CUOTA PAGADA','NRO IDENTIFICADOR REGISTRO','TIPO DE LIQUIDACION','TIPO DE BANCO','IAXIS','SSEGURO','APERTURA_COBERTURA_DE_CS']
            # names_cols_pendientes = ['FOLIO O CARPETA','N_SINIESTRO','N_POLIZA','RUT_ASEGURADO','DV','DIRECCION','NOMBRE','NOMBRE_PRODUCTO','NOMBRE_COBERTURA_RECLAMADA','CAUSAL','FECHA_SINIESTRO','FECHA_DENUNCIA','FECHA CIERRE CONTABLE','FECHA_PAGO','DIRECTO_TOTAL_CLP','DIRECTO_TOTAL_UF','RETENIDO_TOTAL_CLP','RETENIDO_TOTAL_UF','FECHA_INGRESO','INSTITUCION','CONCEPTO','COBERTURA','ESTADO_LIQUIDACION','SUBESTADO','PRODUCTO','TC','POLIZA FLOTANTE','INICIO_VIGENCIA','TERMINO_VIGENCIA','N_OPERACION','TIPO1','GASTO','PERIODICIDAD','CATASTROFE_DETALLE','CODIGO_FECU','SUB CLASIFICACION','TIPO','OBSERVACION','SECUENCIAL','CLAS_PROFIT','PROVISION TECNICA','NRO IDENTIFICADOR REGISTRO','TIPO DE LIQUIDACION','TIPO DE BANCO','IAXIS','SSEGURO','APERTURA_COBERTURA_DE_CS','OBSERVACIONES BENEFICIOS']
            # date_cols_pagados = ['FECHA_SINIESTRO','FECHA_DENUNCIA','FECHA CIERRE CONTABLE','FECHA_PAGO','DIRECTO_TOTAL_CLP','DIRECTO_TOTAL_UF','RETENIDO_TOTAL_CLP','RETENIDO_TOTAL_UF','FECHA_INGRESO','INSTITUCION','DETALLE CONCEPTO','COBERTURA','ESTADO_LIQUIDACION','SUBESTADO','PRODUCTO','TC','POLIZA FLOTANTE','INICIO_VIGENCIA','TERMINO_VIGENCIA']
            base_pagados = pd.read_excel(io=f'{ruta_input}{str(periodo)[4:]}_{str(periodo)[:4]}_Base de Siniestros Cierre GI.xlsx',sheet_name='PAGADOS',names = names_cols_pagados)#,parse_dates = date_cols_pagados, date_format="%d-%m-%Y")
            base_pendientes = pd.read_excel(io=f'{ruta_input}{str(periodo)[4:]}_{str(periodo)[:4]}_Base de Siniestros Cierre GI.xlsx',sheet_name='PENDIENTES',names = names_cols_pendientes)#,parse_dates = date_cols_pendientes, date_format="%d/%m/%Y")
            base_pagados['ESTADO SINIESTRO'] = 'PAGADO'
            base_pendientes['ESTADO SINIESTRO'] = 'PENDIENTE'
            base = pd.concat([base_pagados,base_pendientes])
            df_0_0=base.copy()
            df_0_0['POL_PROD']=np.where(df_0_0['TIPO1']=='Colectivo',df_0_0['N_POLIZA'],df_0_0['PRODUCTO'])
            df_0_1=df_0_0.rename(columns={'COBERTURA':'CODIGO COBERTURA','N_POLIZA':'POLIZA','RUT_ASEGURADO':'RUT_CONTRATANTE','SECUENCIAL':'CERTIFICADO'})
            df_0_2=df_0_1.merge(catastrofes,how='left',on=['CATASTROFE_DETALLE'])
            if sum(df_0_2['TIPO EVENTO'].isnull())>0:
                escribe_reporta(archivo_reporte,f'Cruce de Eventos CAT no encontró llave para todos los registros. Quedaron {sum(df_0_2["TIPO EVENTO"].isnull())} registros afuera')
                df_0_2[df_0_2['TIPO EVENTO'].isnull()]['CATASTROFE_DETALLE'].drop_duplicates().to_csv(ruta_output+'Catastrofes por Parametrizar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
                print(list(df_0_2[df_0_2['TIPO EVENTO'].isnull()]['CATASTROFE_DETALLE'].drop_duplicates()))
                sys.exit()
            df_0_2['MONTO SINIESTRO UF']=df_0_2['DIRECTO_TOTAL_UF']*np.where((df_0_2['CONCEPTO'].str.contains('Service'))|(df_0_2['CONCEPTO'].str.contains('Factura')),0,1)
            df_0_2['EXPOSICION MENSUAL']=1
            df_0_2['EDAD SINIESTRO']=18
            df_0_2=df_0_2.merge(parametros_cesantia,how='left',on='POLIZA')
            df_0_2['DIAS DESDE INICIO VIGENCIA']=(df_0_2['FECHA_SINIESTRO']-df_0_2['INICIO_VIGENCIA']).dt.days
            df_0_2 = df_0_2.drop(columns=['PERIODICIDAD','CLAS_PROFIT'],axis=1)
            df_final_0=df_0_2.copy()
    # Trabajamos con el df_final_0 y hacemos todas las operaciones que debamos hacer en general
    df_final_1=df_final_0[df_final_0['EXPOSICION MENSUAL']>0].copy()
    return df_final_1

            

