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
from S2_Funciones import calcula_exposicion, calcula_edad, calculo_fechas_renovacion, escribe_reporta, completa_campo_total, corrige_tasas_ges

# Prueba de Ejecucion del codigo
print(f'El script {__name__} se está ejecutando')

# Importamos tablas de parametrizaciones
# Tablas de Primas y Siniestros
contrato_cob = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
parametros_contratos = pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Vigencias')
reaseguradores = pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Matriz Reaseguradores')
# Tablas de Primas
cobs_ges=pd.read_excel(io=archivo_parametros,sheet_name='Coberturas GES')
meses_renta=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Meses Renta')
saldo_insoluto=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Saldo Insoluto')
estados_ges=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Estados GES')
estados_iaxis=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Estados IAXIS')
canales_venta=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Canal Venta')
forma_pago=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Forma Pago')
planes_ges=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Planes GES')

# Tablas de Cumulos
cumulos_individuales=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Individual')
cumulos_contrato=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Contrato')
cumulos_excedente=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Excedente')
# cumulos_individuales_siniestros=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Individual Sinies')
# cumulos_excedente_siniestros=pd.read_excel(io=ruta_extensa+archivo_parametros,sheet_name='Matriz Cumulo Excedente Sinies')
# Tablas de Siniestros
ocurrencias=pd.read_excel(io=ruta_extensa+archivo_parametros, sheet_name='Ocurrencias')





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


def pre_procesamiento(tipo_calculo=tipo_calculo):
    """
    # Funcion de pre-procesamiento de la data
    # Corresponden a modificaciones iniciales a las bbdd antes de hacer el calculo generico
    """
    escribe_reporta(archivo_reporte,'COMIENZA LA LECTURA DE LAS BASES DE DATOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    if tipo_calculo=='Prima de Reaseguro':
        # Inputs de otras fuentes
        polizas_pyme=pd.read_csv(ruta_pyme+'1. Inputs Auxiliares\\Polizas Pyme\\'+'Polizas Pyme.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
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
            if 'POLVIGENTE' in df_ges.columns: df_ges=df_ges[~df_ges['POLVIGENTE'].isin([9])]
            # ACA VEMOS QUE CONSIDERAR COMO ICAPITAL EN CASO DEL CONTRATO DESGRAVAMEN NO LICITADO
            if contrato=='Desgravamen No Licitado':
                # df_ges['ICAPITAL']=np.where(df_ges['POLCFIORI'].isnull(),df_ges['POLASECFI'],df_ges['POLCFIORI'])
                df_ges['ICAPITAL']=df_ges['POLASECFI']
                df_ges.drop(columns=['POLCFIORI','POLASECFI'],axis=1,inplace=True)
                df_ges['NRO_OPERACION']=pd.to_numeric(df_ges['NRO_OPERACION'],errors = 'coerce')
                df_ges=df_ges.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
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
        # EXPORTO EDADES CON ISSUES
        if 'ISSUE EDAD INGR' in df_final_0.columns:
            if sum(df_final_0['ISSUE EDAD INGR'])>0: df_final_0[df_final_0['ISSUE EDAD INGR']==1].to_csv(ruta_output+'0. Edades de Ingreso a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        if 'ISSUE EDAD RENOV' in df_final_0.columns:
            if sum(df_final_0['ISSUE EDAD RENOV'])>0: df_final_0[df_final_0['ISSUE EDAD RENOV']==1].to_csv(ruta_output+'0. Edades de Renovacion a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        escribe_reporta(archivo_reporte,'El dataframe input luego de ser pre-procesado posee una prima neta de {}'.format(np.nansum(df_final_0['PRIMA NETA ANUAL'])))
    # Trabajamos con el df_final_0 y hacemos todas las operaciones que debamos hacer en general
    df_final_1=df_final_0[df_final_0['EXPOSICION MENSUAL']>0].copy()
    return df_final_1

            

