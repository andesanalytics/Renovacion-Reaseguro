"""
Preprocesamiento de las bases de datos
"""

import pandas as pd
import numpy as np
import datetime
import time
import unicodedata
import sys
from unidecode import unidecode
from typing import Any
from pandas.tseries.offsets import MonthEnd
from S0_Loaders import Parameter_Loader
from S2_Funciones import calcula_exposicion, calcula_edad, escribe_reporta, completa_campo_total, corrige_tasas_ges, calculo_fechas_renovacion


def pre_procesamiento(parameters: Parameter_Loader, tables: Parameter_Loader) -> pd.DataFrame:
    """Funcion de preprocesamiento de la data
    Leemos la data proveniente de ambos sistemas de administracion de bases de datos (GES e IAXIS) y entregamos una unica salida que será el input de los calculos de reaseguro (prima o siniestros)
    Antes de realizar los calculos de asignacion de contratos de reaseguro, calculo de primas de reaseguro y cesiones, debemos hacer ciertas transformaciones a la data
    Estas dependerán principalmente de que contrato de reaseguro estemos trabajando, o si el contrato es de prima unica o recurrente
    Tambien hay un tratamiento completamente diferente cuando trabajamos los siniestros, que se dividen entre Vida y Generales

    Parameters
    ----------
    parameters : Parameter_Loader
        Contiene los parametros del calculo 
    tables : Parameter_Loader
        Contiene las tablas que ayudan a calcular los contratos de reaseguro

    Returns
    -------
    pd.DataFrame
        Entrega el dataframe listo para ser procesado por el calculador de prima de reaseguro o de siniestros de reaseguro, segun corresponda
    """
    # * definimos variables de uso frecuente dentro de `parameters` que se utilizaran en la funcion
    # Caracteristicas del contrato
    tipo_calculo: str = parameters.parameters['tipo_calculo']
    tipo_contrato: str = parameters.parameters['tipo_contrato']
    contrato: str = parameters.parameters['contrato']
    clasificacion_contrato: str = parameters.parameters['clasificacion_contrato']
    # Fechas de cierre
    fecha_cierre: datetime.datetime = parameters.parameters['fecha_cierre']
    fecha_inicio_mes: datetime.datetime = parameters.parameters['fecha_inicio_mes']
    periodo: int = parameters.parameters['periodo']
    # Campos tecnicos mas especializados
    campo_rut_duplicados: str = parameters.parameters['campo_rut_duplicados']
    edad_casos_perdidos: int = parameters.parameters['edad_casos_perdidos']
    dias_exposicion: int = parameters.parameters['dias_exposicion']
    tdm_mensual: float = parameters.parameters['tdm_mensual']
    archivo_reporte: Any = parameters.parameters['archivo_reporte']
    # Sobre que bases considerar y cuales nombres tienen los archivos que debemos leer
    base_iaxis: int = parameters.parameters['base_iaxis']
    base_ges: int = parameters.parameters['base_ges']   
    archivo_input: str = parameters.parameters['archivo_input']
    archivo_input_ges: str = parameters.parameters['archivo_input_ges']
    # Sobre como debemos importar y exportar la data
    separador_input: str = parameters.parameters['separador_input']
    decimal_input: str = parameters.parameters['decimal_input']
    separador_output: str = parameters.parameters['separador_output']
    decimal_output: str = parameters.parameters['decimal_output']
    # Rutas de entradas y de salidas
    ruta_output: str = parameters.parameters['ruta_output']
    ruta_input: str = parameters.parameters['ruta_input']
    ruta_pyme: str = parameters.parameters['ruta_pyme']
    ruta_otros: str = parameters.parameters['ruta_otros']
    ruta_si: str = parameters.parameters['ruta_si']
    ruta_uso_seguro: str = parameters.parameters['ruta_uso_seguro']
    
    # Algunas escrituras en pantalla y en el archivo de reportes
    escribe_reporta(archivo_reporte,'COMIENZA LA LECTURA DE LAS BASES DE DATOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
    print(f'Contrato {contrato}')
    
    # * preprocesamiento de los expuestos (para calculo de prima de reaseguro)
    if tipo_calculo=='Prima de Reaseguro':
        # Inputs de otras fuentes
        polizas_pyme: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_pyme}1. Inputs Auxiliares\\Polizas Pyme\\Polizas Pyme.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
        cti: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_otros}1. Inputs Auxiliares\\Otros\\CTI.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
        innominadas: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_otros}1. Inputs Auxiliares\\Otros\\polizas_innominadas.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
        cobs_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Coberturas GES')
        if contrato=='Complementario UC': uso_seguro_com_uc: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_uso_seguro}1. Inputs Auxiliares\\Com UC\\COM UC Uso del Seguro Hist {periodo}.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
        # Dependiendo del contrato, son diferentes los campos de tipo fecha que debemos transformar
        if (tipo_contrato=='Vida')&(contrato not in ['K-Fijo','Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO']
        elif (tipo_contrato=='Vida')&(contrato in ['Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_INICIO_CRED','FECHA_FIN_CRED']
        elif (tipo_contrato=='Vida')&(contrato=='K-Fijo'): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
        elif clasificacion_contrato =='Cesantia PU': cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
        elif (tipo_contrato=='Generales')&('Incendio y Sismo' in contrato): cols_date,cols_date_ges=['FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FECHA_EFECTO','FECHA_VENCIMIENTO']
        elif (tipo_contrato=='Generales')&(contrato=='Cesantia PR'): cols_date: list[str]=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION']
        # * LECTURA DE BASES DE DATOS IAXIS
        if base_iaxis==1:
            # Lectura de BBDD y tablas de parametria
            df_iaxis: pd.DataFrame=pd.read_csv(ruta_input+archivo_input,sep=separador_input,decimal=decimal_input,parse_dates=cols_date,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
            estados_iaxis: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Estados IAXIS')
            canales_venta: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Canal Venta')
            for col in cols_date:
                if df_iaxis[col].dtype!='datetime64[ns]': df_iaxis[col]=pd.to_datetime(df_iaxis[col],format = '%d-%m-%Y', errors='coerce')   
            # Algunas transformaciones iniciales
            df_iaxis['IPRIANU']=round(df_iaxis['IPRIANU'],4)
            df_iaxis['ICAPITAL']=round(df_iaxis['ICAPITAL'],4)
            df_iaxis['BASE']='IAXIS'
            if 'FECHA_CONTABILIZACION_ANULACION' in df_iaxis.columns:df_iaxis['PERIODO_CONTABILIZACION']=df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.year*100+df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.month
            if 'NRO_OPERACION' not in df_iaxis.columns:df_iaxis['NRO_OPERACION']=0
            else: df_iaxis['NRO_OPERACION']=df_iaxis['NRO_OPERACION'].fillna(0)
            # Obtenemos nombre del canal de venta
            if 'CANAL_VENTA' in df_iaxis.columns: df_iaxis=df_iaxis.merge(canales_venta,how='left',on=['CANAL_VENTA'])
            # uso del seguro para el contrato Complementario UC
            if contrato=='Complementario UC': df_iaxis['USO SEGURO']= np.where((df_iaxis['SSEGURO'].isin(uso_seguro_com_uc['SSEGURO']))&(df_iaxis['MOTIVO_BAJA']==306),1,0)
            # Tratamiento del campo PERIOD_TASA para aquellos contratos que poseen saldo insoluto
            if 'PERIOD_TASA' in df_iaxis.columns:df_iaxis['TASA_CRED']=np.where(df_iaxis['PERIOD_TASA']==12,df_iaxis['TASA_CRED']/100,np.where(df_iaxis['PERIOD_TASA']==1,(1+df_iaxis['TASA_CRED']/100)**(1/12)-1,df_iaxis['TASA_CRED']/100))
            # Marcamos polizas CTI (Colectivo de Tratamiento Individual)
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
            # Cruces con tablas de parametria
            df_iaxis=df_iaxis.merge(estados_iaxis[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
            df_iaxis=df_iaxis[df_iaxis['APLICA ESTADO']==1].copy()
            df_iaxis=df_iaxis.merge(polizas_pyme,how='left',on=['POLIZA'])
            df_iaxis['TIPO_POLIZA_LETRA']=np.where(df_iaxis['TIPO_POLIZA_LETRA'].isnull(),np.where(df_iaxis['TIPO_POLIZA']==1,'I','C'),df_iaxis['TIPO_POLIZA_LETRA'])
            # Tratamiento de saldos insolutos para el contrato de desgravamen no licitado
            if contrato =='Desgravamen No Licitado':
                saldos_insolutos_detalle: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_si}1. Inputs Auxiliares\\Saldos Insolutos\\Saldos Insolutos {periodo}.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
                saldos_insolutos_detalle['NRO_OPERACION']=saldos_insolutos_detalle['NRO_OPERACION'].astype(str).str.replace('K','').astype(float)
                df_iaxis=df_iaxis.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
        # * LECTURA DE BASES DE DATOS GES
        if base_ges==1: 
            # Lectura de BBDD y tablas de parametria
            df_ges: pd.DataFrame=pd.read_csv(ruta_input+archivo_input_ges,sep=separador_input,decimal=decimal_input,parse_dates=cols_date_ges,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
            estados_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Estados GES')
            forma_pago: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Forma Pago')
            planes_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Planes GES')
            for col in cols_date_ges:
                if df_ges[col].dtype!='datetime64[ns]': df_ges[col]=pd.to_datetime(df_ges[col],format = '%d-%m-%Y', errors='coerce')            
            # Algunas transformaciones iniciales
            df_ges['CTI']=0
            # Tratamiento del campo PERIOD_TASA para aquellos contratos que poseen saldo insoluto
            if 'PERIOD_TASA' in df_ges.columns:df_ges['TASA_CRED']=np.where(df_ges['PERIOD_TASA']=='M',df_ges['TASA_CRED']/100,np.where(df_ges['PERIOD_TASA']=='A',(1+df_ges['TASA_CRED']/100)**(1/12)-1,df_ges['TASA_CRED']/100))
            # Corrige las tasas para los contratos de prima unica de cesantia
            if clasificacion_contrato=='Cesantia PU': df_ges=corrige_tasas_ges(df_ges, parameters)
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
            # Creacion de la variable FECHA_ANULACION (no existe en GES) que depende de que contrato estemos trabajando
            if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'): df_ges['FECHA_ANULACION']=pd.to_datetime(np.where((df_ges['FECHA_VENCIMIENTO']>=fecha_inicio_mes)&(df_ges['FECHA_VENCIMIENTO']<=fecha_cierre),df_ges['FECHA_VENCIMIENTO'].astype(str),''), format = '%Y-%m-%d', errors='coerce')
            elif contrato=='K-Fijo':
                df_ges['FEC AUX NA']=0
                df_ges['FEC AUX NA']=pd.to_datetime(df_ges['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
                df_ges['FECHA_ANULACION']=np.where(~df_ges['FECHA_RENUNCIA'].isnull(),df_ges['FECHA_RENUNCIA'],np.where(~df_ges['FECHA_PREPAGO'].isnull(),df_ges['FECHA_PREPAGO'],np.where(df_ges['FECHA_FIN_VIGENCIA']==df_ges['FECHA_VENCIMIENTO'],df_ges['FEC AUX NA'],df_ges['FECHA_FIN_VIGENCIA'])))
                df_ges=df_ges.drop(columns=['FEC AUX NA'],axis=1)
                df_ges['PERIODO_CONTABILIZACION']=np.where(df_ges['FECHA_ANULACION'].isnull(),np.nan,np.maximum(df_ges['PERIODO_CONTABILIZACION'],df_ges['FECHA_ANULACION'].dt.year*100+df_ges['FECHA_ANULACION'].dt.month))
                df_ges['FECHA_CONTABILIZACION_ANULACION']=pd.to_datetime(df_ges['PERIODO_CONTABILIZACION'],format='%Y%m', errors='coerce')+ MonthEnd(0)
            # Transformaciones finales
            df_ges=df_ges.merge(forma_pago,how='left',on='FORMA_PAGO')
            df_ges['TIPO_POLIZA_LETRA']=df_ges['TIPO_POLIZA']
            df_ges['TIPO_POLIZA']=np.where(df_ges['TIPO_POLIZA_LETRA']=='C',2,1)
            df_ges['BASE']='GES'
            # Calculamos fechas de renovacion de los contratos de prima recurrente
            df_ges['FINI_RENOV_ANUAL'],df_ges['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_ges, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo)
            # Anualizacion de la prima de vida GES
            if tipo_contrato=='Vida':df_ges['IPRIANU']=df_ges['IPRIANU']*df_ges['FACTOR ANUALIZACION']
            # traemos codigos de planes GES
            df_ges=df_ges.merge(planes_ges,how='left',on=['PRODUCTO','PLAN_DESC'])
            df_ges['COD_PLAN']=df_ges['COD_PLAN'].fillna(0)
            # Algunos estados pueden no aplicar
            df_ges=df_ges.merge(estados_ges[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
            df_ges=df_ges[df_ges['APLICA ESTADO']==1].copy()
            if 'POLVIGENTE' in df_ges.columns: df_ges=df_ges[~df_ges['POLVIGENTE'].isin([9])]
            # Tratamiento de saldos insolutos para el contrato de desgravamen no licitado
            if contrato=='Desgravamen No Licitado':
                df_ges['ICAPITAL']=df_ges['POLASECFI']
                df_ges.drop(columns=['POLCFIORI','POLASECFI'],axis=1,inplace=True)
                df_ges['NRO_OPERACION']=pd.to_numeric(df_ges['NRO_OPERACION'],errors = 'coerce')
                df_ges=df_ges.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
        # JUNTAMOS LAS BASES DEPENDIENDO DE CUALES EXISTEN
        if (base_iaxis==1)&(base_ges==1):
            df_0_0: pd.DataFrame=pd.concat([df_iaxis,df_ges],axis=0)
        elif base_iaxis==1:
            df_0_0: pd.DataFrame=df_iaxis
        elif base_ges==1:
            df_0_0: pd.DataFrame=df_ges
        else:
            return pd.DataFrame()
        # * Calculos con las bases de GES a iAxis unidas
        # CALCULOS DE VARIABLES EXTRAS Y CAMBIOS DE NOMBRE DE ALGUNAS VARIABLES
        escribe_reporta(archivo_reporte,'El dataframe input posee una prima neta de {}'.format(np.nansum(df_0_0['IPRIANU'])))
        df_0_0['NRO_OPERACION']=df_0_0['NRO_OPERACION'].fillna(0)
        if 'CANAL_DESC' in df_0_0.columns: df_0_0['CANAL_DESC']=df_0_0['CANAL_DESC'].str.strip()
        df_0_1=df_0_0.merge(cobs_ges[['COD_COB','COB_GES']],how='left',on=['COD_COB'],suffixes=['','_x']) # type: ignore
        df_0_1['COB_GES']=np.where(df_0_1['COB_GES'].isnull(),df_0_1['COD_COB'],df_0_1['COB_GES'])
        df_0_1.rename(columns={'COD_PLAN':'PLAN','IPRIANU':'PRIMA NETA ANUAL','COB_GES':'CODIGO COBERTURA','COD_COB':'CODIGO COBERTURA IAXIS'},inplace=True)
        df_0_1['POL_PROD']=np.where((df_0_1['TIPO_POLIZA_LETRA']=='I')|(df_0_1['CTI']==1),df_0_1['PRODUCTO'],df_0_1['POLIZA'])
        df_0_1['FECHA CIERRE']=fecha_cierre
        df_0_1['FECHA CIERRE']=df_0_1['FECHA CIERRE'].astype(df_0_1['FECHA_EFECTO'].dtype)
        df_0_1['INNOMINADA'] = np.where(df_0_1['POLIZA'].isin(list(innominadas['POLIZA'])),1,0)
        df_0_1['EDAD'],df_0_1['ISSUE EDAD']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_cierre,edad_casos_perdidos,108,archivo_reporte,reporta_issues=1, edad_inf = 18, aplica_edad_prom_cartera = 1)
        # Calculo edad de ingreso
        if 'FEC_NAC' in df_0_1.columns: 
            escribe_reporta(archivo_reporte,'Calculando edad de ingreso')
            df_0_1['EDAD INGRESO'],df_0_1['ISSUE EDAD INGR']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],df_0_1['FECHA_EFECTO'],edad_casos_perdidos,108,archivo_reporte,reporta_issues=1, edad_inf = 18, aplica_edad_prom_cartera = 1)
        # CALCULOS ESPECIFICOS POR CADA CONTRATO
        # CALCULOS DE FECHAS DE INICIO/FIN DE EXPOSICION: SE DIFERENCIAN ENTRE PRIMA UNICA (CESANTIA Y K-FIJO) DEL RESTO
        if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'):
            df_0_1['FINI_RENOV_ANUAL'],df_0_1['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_0_1, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo,0)
            df_0_1['FECHA FIN EXP']=np.where(~df_0_1['FECHA_ANULACION'].isnull(),df_0_1['FECHA_ANULACION'],np.where(df_0_1['FECHA_VENCIMIENTO'].isnull(),df_0_1['FFIN_RENOV_ANUAL'],df_0_1['FECHA_VENCIMIENTO']))
        else:
            df_0_1['FEC AUX NA']=0
            df_0_1['FEC AUX NA']=pd.to_datetime(df_0_1['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
            df_0_1['FECHA_ANULACION']=np.where(df_0_1['FECHA_ANULACION']<=fecha_cierre,df_0_1['FECHA_ANULACION'],df_0_1['FEC AUX NA'])
        # * CALCULOS GENERICOS PARA BASES DE VIDA PRIMA RECURRENTE
        if (tipo_contrato=='Vida')&(contrato!='K-Fijo'):
            meses_renta: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Meses Renta')
            saldo_insoluto: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Saldo Insoluto')
            df_0_1['EXPOSICION MENSUAL']=calcula_exposicion(df_0_1,'FECHA_EFECTO','FECHA FIN EXP',dias_exposicion,fecha_inicio_mes,fecha_cierre)
            df_0_1['TIPO ASEGURADO']=np.where((df_0_1['RUT'].isnull())|(df_0_1['RUT']==df_0_1['RUT_CONTRATANTE']),'Titular','Adicional')
            escribe_reporta(archivo_reporte,'Calculando edad de renovacion')
            if contrato == 'Desgravamen No Licitado': df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_inicio_mes,edad_casos_perdidos,108,archivo_reporte,reporta_issues=1,edad_inf = 18, aplica_edad_prom_cartera = 1)
            else: df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],df_0_1['FINI_RENOV_ANUAL'],edad_casos_perdidos,108,archivo_reporte,reporta_issues=1, edad_inf = 18, aplica_edad_prom_cartera = 1)
            df_0_2=df_0_1.merge(meses_renta,how='left',on=['CODIGO COBERTURA'],suffixes=['','_x']) # type: ignore
            if contrato=='Desgravamen No Licitado': df_0_2['MONTO ASEGURADO']=np.where(df_0_2['MESES RENTA']==1,1,(1-(1+tdm_mensual)**(-df_0_2['MESES RENTA']))/tdm_mensual)*df_0_2['ICAPITAL']
            else: df_0_2['MONTO ASEGURADO']=df_0_2['ICAPITAL']
            df_0_3=df_0_2.merge(saldo_insoluto,how='left',on=['PRODUCTO','CODIGO COBERTURA','BASE'],suffixes=['','_x']) # type: ignore
            df_0_3['APLICA CALCULO SALDO INSOLUTO']=df_0_3['APLICA CALCULO SALDO INSOLUTO'].fillna(0)
            # * ESPECIFICO DE DESG NL: PRODUCTOS CON CAPITAL COMO EL SALDO INSOLUTO DEBEN CALCULARSE
            if contrato in ['Desgravamen No Licitado']:
                df_0_4=df_0_3[df_0_3['APLICA CALCULO SALDO INSOLUTO']==1].copy()
                df_0_4_resto=df_0_3[df_0_3['APLICA CALCULO SALDO INSOLUTO']==0].copy()
                df_0_4['FECHA_FIN_CRED']=np.where(df_0_4['BASE']=='GES',np.maximum(df_0_4['FECHA_VENCIMIENTO'],df_0_4['FECHA_FIN_CRED']),df_0_4['FECHA_VENCIMIENTO'])
                df_0_4['NCUOTAS']=((df_0_4['FECHA_FIN_CRED']-df_0_4['FECHA_EFECTO']).dt.days/365*12).round(0)
                df_0_4['NCUOTAS FALTANTES']=((df_0_4['FECHA_FIN_CRED']-fecha_cierre).dt.days/365*12).round(0)
                df_0_4['PERIODO_EFECTO']=df_0_4['FECHA_EFECTO'].dt.year*100+df_0_4['FECHA_EFECTO'].dt.month
                df_0_4=completa_campo_total(df_0_4,'TASA_CRED',[['PRODUCTO','PERIODO_EFECTO'],['PERIODO_EFECTO']], parameters)
                df_0_4['SALDO INSOLUTO CALCULADO']=df_0_4['ICAPITAL']*(1-(1+df_0_4['TASA_CRED_FINAL'])**(-df_0_4['NCUOTAS FALTANTES']))/(1-(1+df_0_4['TASA_CRED_FINAL'])**(-df_0_4['NCUOTAS']))
                df_0_4['MONTO ASEGURADO']=np.where(df_0_4['SALDO_INSOLUTO']>0,df_0_4['SALDO_INSOLUTO'],np.maximum(df_0_4['SALDO INSOLUTO CALCULADO'],0)) 
                df_0_5=pd.concat([df_0_4,df_0_4_resto],axis=0)
                df_0_5 = df_0_5.reset_index(drop=True)
                # ! Solicitud area de productos: producto 331 se encuentra con coberturas duplicados y problemas de capitales mal asignados
                df_331 = df_0_5[(df_0_5['PRODUCTO']==331)&(df_0_5['BASE']=='IAXIS')&(df_0_5['CODIGO COBERTURA']==12)].copy()
                df_resto = df_0_5[~df_0_5.index.isin(df_331.index)].copy()
                df_331['PRIMA NETA ANUAL'] = df_331.groupby('SSEGURO')['PRIMA NETA ANUAL'].transform('sum')
                df_331 = df_331[df_331['CODIGO COBERTURA IAXIS']==1200].copy()
                df_0_6 = pd.concat([df_resto,df_331]).reset_index(drop=True)
                df_final_0=df_0_6.copy()
            else: df_final_0=df_0_3.copy()
            # * Tratamiento saldo insoluto para Multisocios
            if contrato in ['Multisocios']:
                df_0_3['FECHA_FIN_CRED']=np.where(df_0_3['BASE']=='GES',np.maximum(df_0_3['FECHA_VENCIMIENTO'],df_0_3['FECHA_FIN_CRED']),df_0_3['FECHA_VENCIMIENTO'])
                df_0_3['NCUOTAS']=((df_0_3['FECHA_FIN_CRED']-df_0_3['FECHA_EFECTO']).dt.days/365*12).round(0)
                df_0_3['NCUOTAS FALTANTES']=((df_0_3['FECHA_FIN_CRED']-fecha_cierre).dt.days/365*12).round(0)
                df_0_3['PERIODO_EFECTO']=df_0_3['FECHA_EFECTO'].dt.year*100+df_0_3['FECHA_EFECTO'].dt.month
                df_0_3=completa_campo_total(df_0_3,'TASA_CRED',[['PRODUCTO','PERIODO_EFECTO'],['PERIODO_EFECTO']], parameters)
                df_0_3['SALDO INSOLUTO CALCULADO']=np.where(df_0_3['FECHA_FIN_CRED']<fecha_cierre,0,df_0_3['ICAPITAL']*(1-(1+df_0_3['TASA_CRED_FINAL'])**(-df_0_3['NCUOTAS FALTANTES']))/(1-(1+df_0_3['TASA_CRED_FINAL'])**(-df_0_3['NCUOTAS'])))
                df_0_3['MONTO ASEGURADO']=np.maximum(df_0_3['SALDO INSOLUTO CALCULADO'],0)
                df_final_0=df_0_3.copy()
        # * CALCULOS PARA K-FIJO
        elif (tipo_contrato=='Vida')&(contrato=='K-Fijo'): 
            df_0_1['EXPOSICION MENSUAL']=1
            escribe_reporta(archivo_reporte,'Calculando edad de renovacion')
            df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_inicio_mes,edad_casos_perdidos,108,archivo_reporte,reporta_issues=1, edad_inf = 18, aplica_edad_prom_cartera = 1)
            df_0_1['PLAZO MESES']=np.maximum(1,round((df_0_1['FECHA_VENCIMIENTO']-df_0_1['FECHA_EFECTO']).dt.days/(365.25/12),0))
            df_0_1['MONTO ASEGURADO']=df_0_1['ICAPITAL']
            df_0_2=df_0_1[df_0_1['FECHA_EFECTO']<=fecha_cierre]
            df_final_0=df_0_2.copy()
        # * Exportamos Edades con problemas
        # EXPORTO EDADES CON ISSUES
        if 'ISSUE EDAD INGR' in df_final_0.columns:
            if sum(df_final_0['ISSUE EDAD INGR'])>0: df_final_0[df_final_0['ISSUE EDAD INGR']==1].to_csv(ruta_output+'0. Edades de Ingreso a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        if 'ISSUE EDAD RENOV' in df_final_0.columns:
            if sum(df_final_0['ISSUE EDAD RENOV'])>0: df_final_0[df_final_0['ISSUE EDAD RENOV']==1].to_csv(ruta_output+'0. Edades de Renovacion a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
        escribe_reporta(archivo_reporte,'El dataframe input luego de ser pre-procesado posee una prima neta de {}'.format(np.nansum(df_final_0['PRIMA NETA ANUAL'])))
    # * Ultimos filtros
    # Trabajamos con el df_final_0 y hacemos todas las operaciones que debamos hacer en general
    df_final_1=df_final_0[df_final_0['EXPOSICION MENSUAL']>0].copy()
    # ! Solicitud Productos: Trabajar con los asegurados que estan vigentes a fin de mes
    df_final_2=df_final_1[(df_final_1['FECHA_VENCIMIENTO']>=fecha_cierre)|(df_final_1['FECHA_VENCIMIENTO'].isnull())]
    df_final_3=df_final_2[(df_final_2['FECHA_ANULACION']>=fecha_cierre)|(df_final_2['FECHA_ANULACION'].isnull())]
    return df_final_3

            

