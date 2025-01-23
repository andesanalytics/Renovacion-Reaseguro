# Modulo S3_Pre_Procesamiento

El módulo se encarga de **preparar y limpiar** las bases de datos para su análisis. Esto incluye tareas como:

- **Eliminar datos innecesarios**.
- **Corregir errores** en los datos.
- **Transformar formatos** de datos para facilitar su uso.

El objetivo es asegurar que los datos sean **consistentes y utilizables** para posteriores análisis o modelados.

# Funcion `pre_procesamiento`

La función `pre_procesamiento` se encarga de **leer datos** de dos sistemas de administración de bases de datos (GES e IAXIS) y generar una **salida unificada**. Esta salida es necesaria para realizar cálculos relacionados con el **reaseguro**, como primas o siniestros.

Antes de los cálculos, se realizan **transformaciones** en los datos, que varían según el tipo de contrato de reaseguro (prima única o recurrente) y el tratamiento de siniestros (Vida y Generales).

### Parámetros:
- **parameters**: Contiene los parámetros del cálculo.
- **tables**: Incluye las tablas necesarias para calcular los contratos de reaseguro.

### Retorno:
- Devuelve un `pd.DataFrame` que está listo para ser utilizado en el cálculo de primas o siniestros de reaseguro.

## Definición de Variables Comunes

Se establecen variables que se usarán frecuentemente en el bloque de parámetros de una función. Estas variables facilitarán la gestión y el uso de datos dentro de la función.

Las instrucciones de código extraen información sobre un contrato desde un conjunto de parámetros. 

- `tipo_calculo`, `tipo_contrato`, `contrato` y `clasificacion_contrato` son variables que almacenan diferentes características del contrato, como su tipo y clasificación. 

Esta información es fundamental para entender las **características del contrato**.
```python
tipo_calculo: str = parameters.parameters['tipo_calculo']
tipo_contrato: str = parameters.parameters['tipo_contrato']
contrato: str = parameters.parameters['contrato']
clasificacion_contrato: str = parameters.parameters['clasificacion_contrato']
```

Las instrucciones de código extraen **valores de fechas** y un **período** desde un conjunto de parámetros. 

- `fecha_cierre` almacena la fecha de cierre.
- `fecha_inicio_mes` guarda la fecha de inicio del mes.
- `periodo` contiene un valor entero que representa un período específico.

Estos valores son esenciales para gestionar **fechas y períodos** en el contexto del código.
```python
fecha_cierre: datetime.datetime = parameters.parameters['fecha_cierre']
fecha_inicio_mes: datetime.datetime = parameters.parameters['fecha_inicio_mes']
periodo: int = parameters.parameters['periodo']
```

El código asigna valores a variables a partir de un conjunto de parámetros. Cada variable representa un campo específico:

- **campo_rut_duplicados**: Almacena un valor relacionado con RUT duplicados.
- **edad_casos_perdidos**: Guarda la edad de casos que se han perdido.
- **dias_exposicion**: Indica la cantidad de días de exposición.
- **tdm_mensual**: Contiene un valor de tasa de descuento mensual.
```python
campo_rut_duplicados: str = parameters.parameters['campo_rut_duplicados']
edad_casos_perdidos: int = parameters.parameters['edad_casos_perdidos']
dias_exposicion: int = parameters.parameters['dias_exposicion']
tdm_mensual: float = parameters.parameters['tdm_mensual']
archivo_reporte: Any = parameters.parameters['archivo_reporte']
```

El código extrae valores de un conjunto de parámetros. Se definen cuatro variables:

- **base_iaxis**: Almacena un valor entero relacionado con la base de un eje.
- **base_ges**: Almacena un valor entero relacionado con la base de gestión.
- **archivo_input**: Almacena el nombre de un archivo de entrada.
- **archivo_input_ges**: Almacena el nombre de un archivo de entrada para gestión.

Estos valores son
```python
base_iaxis: int = parameters.parameters['base_iaxis']
base_ges: int = parameters.parameters['base_ges']
archivo_input: str = parameters.parameters['archivo_input']
archivo_input_ges: str = parameters.parameters['archivo_input_ges']
```

El código extrae configuraciones de un conjunto de parámetros. 

- **`separador_input`**: Define el carácter que separa los valores en la entrada.
- **`decimal_input`**: Especifica el símbolo utilizado para los decimales en la entrada.
- **`separador_output`**: Establece el carácter que separará los valores en la salida.
- **`decimal_output`**: Indica el símbolo para los decimales en
```python
separador_input: str = parameters.parameters['separador_input']
decimal_input: str = parameters.parameters['decimal_input']
separador_output: str = parameters.parameters['separador_output']
decimal_output: str = parameters.parameters['decimal_output']
```

Las instrucciones de código definen varias **rutas** que se utilizarán para entradas y salidas de datos. Cada ruta se obtiene de un conjunto de **parámetros**. Las variables son:

- `ruta_output`: Ruta de salida.
- `ruta_input`: Ruta de entrada.
- `ruta_pyme`: Ruta específica para pymes.
- `ruta_otros`: Ruta para otros datos.
- `ruta_si`: Ruta para información específica.
- `ruta_uso_seguro`:
```python
ruta_output: str = parameters.parameters['ruta_output']
ruta_input: str = parameters.parameters['ruta_input']
ruta_pyme: str = parameters.parameters['ruta_pyme']
ruta_otros: str = parameters.parameters['ruta_otros']
ruta_si: str = parameters.parameters['ruta_si']
ruta_uso_seguro: str = parameters.parameters['ruta_uso_seguro']
```

El código realiza **escrituras** tanto en un archivo de reportes como en la pantalla. 

1. **`escribe_reporta(...)`**: Registra un mensaje en el archivo especificado, indicando el inicio de la lectura de las bases de datos junto con la fecha y hora actual.
   
2. **`print(...)`**: Muestra en la pantalla el número de contrato.

Ambas acciones son parte de un proceso de **registro y visualización** de información.
```python
escribe_reporta(archivo_reporte,'COMIENZA LA LECTURA DE LAS BASES DE DATOS:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
print(f'Contrato {contrato}')
```

## Preprocesamiento de Expuestos para Cálculo de Prima de Reaseguro

Este proceso se encarga de preparar los datos de los expuestos, lo cual es esencial para calcular de manera adecuada la prima de reaseguro. Se asegura de que la información esté en el formato correcto y lista para su análisis posterior.
```python
if tipo_calculo=='Prima de Reaseguro':
```

El código carga datos desde diferentes archivos en formato de texto y Excel para su posterior uso. 

- **`polizas_pyme`**: Carga datos de un archivo de texto relacionado con pólizas Pyme.
- **`cti`**: Carga datos de otro archivo de texto llamado CTI.
- **`innominadas`**: Carga datos de un archivo de texto sobre pólizas innominadas.
- **`cobs_ges`**: Carga
```python
polizas_pyme: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_pyme}1. Inputs Auxiliares\\Polizas Pyme\\Polizas Pyme.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
cti: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_otros}1. Inputs Auxiliares\\Otros\\CTI.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
innominadas: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_otros}1. Inputs Auxiliares\\Otros\\polizas_innominadas.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
cobs_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Coberturas GES')
if contrato=='Complementario UC': uso_seguro_com_uc: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_uso_seguro}1. Inputs Auxiliares\\Com UC\\COM UC Uso del Seguro Hist {periodo}.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
```

El código establece diferentes listas de campos de fecha (`cols_date` y `cols_date_ges`) según el tipo de contrato y su clasificación. 

- Si el **tipo de contrato** es `'Vida'`, se definen campos específicos basados en si el contrato está en una lista determinada.
- Para la **clasificación de contrato** `'Cesantia PU'`, se asignan campos similares a los de `'Vida'`.
- En el caso de **contratos generales**, se asignan
```python
if (tipo_contrato=='Vida')&(contrato not in ['K-Fijo','Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO']
elif (tipo_contrato=='Vida')&(contrato in ['Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_INICIO_CRED','FECHA_FIN_CRED']
elif (tipo_contrato=='Vida')&(contrato=='K-Fijo'): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
elif clasificacion_contrato =='Cesantia PU': cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
elif (tipo_contrato=='Generales')&('Incendio y Sismo' in contrato): cols_date,cols_date_ges=['FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FECHA_EFECTO','FECHA_VENCIMIENTO']
elif (tipo_contrato=='Generales')&(contrato=='Cesantia PR'): cols_date: list[str]=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION']
```

### Lectura de Bases de Datos IAXIS

Este código se encarga de **leer datos** de una base de datos específica llamada IAXIS. A través de este proceso, se obtienen y organizan los datos necesarios para su posterior análisis o uso en otras aplicaciones.
```python
if base_iaxis==1:
```

El código realiza las siguientes acciones:

1. **Carga de datos**: Lee un archivo CSV y lo almacena en un DataFrame llamado `df_iaxis`, utilizando parámetros como el separador, el formato decimal y la codificación.

2. **Obtención de tablas**: Extrae dos tablas de un archivo Excel, una llamada `estados_iaxis` y otra `canales_venta`, a partir de las hojas especificadas.

3. **Conversión de fechas**
```python
df_iaxis: pd.DataFrame=pd.read_csv(ruta_input+archivo_input,sep=separador_input,decimal=decimal_input,parse_dates=cols_date,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
estados_iaxis: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Estados IAXIS')
canales_venta: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Canal Venta')
for col in cols_date:
if df_iaxis[col].dtype!='datetime64[ns]': df_iaxis[col]=pd.to_datetime(df_iaxis[col],format = '%d-%m-%Y', errors='coerce')
```

El código realiza **transformaciones iniciales** en un DataFrame llamado `df_iaxis`. 

1. **Redondeo**: Las columnas `IPRIANU` e `ICAPITAL` se redondean a 4 decimales.
2. **Asignación de valor**: Se establece la columna `BASE` con el valor `'IAXIS'`.
3. **Cálculo de periodo**: Si existe la columna `FECHA_CONTABILIZACION
```python
df_iaxis['IPRIANU']=round(df_iaxis['IPRIANU'],4)
df_iaxis['ICAPITAL']=round(df_iaxis['ICAPITAL'],4)
df_iaxis['BASE']='IAXIS'
if 'FECHA_CONTABILIZACION_ANULACION' in df_iaxis.columns:df_iaxis['PERIODO_CONTABILIZACION']=df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.year*100+df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.month
if 'NRO_OPERACION' not in df_iaxis.columns:df_iaxis['NRO_OPERACION']=0
else: df_iaxis['NRO_OPERACION']=df_iaxis['NRO_OPERACION'].fillna(0)
```

El código verifica si la columna **`CANAL_VENTA`** existe en el DataFrame **`df_iaxis`**. Si es así, se realiza una combinación (merge) con otro DataFrame llamado **`canales_venta`** utilizando la columna **`CANAL_VENTA`** como clave. Esto permite agregar información sobre el nombre del canal de venta al DataFrame original.
```python
if 'CANAL_VENTA' in df_iaxis.columns: df_iaxis=df_iaxis.merge(canales_venta,how='left',on=['CANAL_VENTA'])
```

El código verifica si el **contrato** es igual a `'Complementario UC'`. Si es así, se actualiza la columna **'USO SEGURO'** en el DataFrame **df_iaxis**. 

Se asigna un valor de **1** si se cumplen dos condiciones:
1. El valor en la columna **'SSEGURO'** de **df_iaxis** está presente en la columna **'SSEGURO'** de **
```python
if contrato=='Complementario UC': df_iaxis['USO SEGURO']= np.where((df_iaxis['SSEGURO'].isin(uso_seguro_com_uc['SSEGURO']))&(df_iaxis['MOTIVO_BAJA']==306),1,0)
```

El código verifica si la columna **`PERIOD_TASA`** existe en el DataFrame **`df_iaxis`**. Si está presente, ajusta los valores de la columna **`TASA_CRED`** según el valor de **`PERIOD_TASA`**:

- Si **`PERIOD_TASA`** es 12, divide **`TASA_CRED`** entre 100.
- Si **`PERIOD_TASA`** es 1
```python
if 'PERIOD_TASA' in df_iaxis.columns:df_iaxis['TASA_CRED']=np.where(df_iaxis['PERIOD_TASA']==12,df_iaxis['TASA_CRED']/100,np.where(df_iaxis['PERIOD_TASA']==1,(1+df_iaxis['TASA_CRED']/100)**(1/12)-1,df_iaxis['TASA_CRED']/100))
```

El código asigna un valor a una nueva columna llamada **CTI** en el DataFrame **df_iaxis**. 

- Se utiliza `np.where` para verificar si los productos en la columna **PRODUCTO** de **df_iaxis** están presentes en la lista de productos de otro DataFrame llamado **cti**.
- Si un producto está en la lista, se asigna el valor **1**; de lo contrario, se asigna **0**.

Esto
```python
df_iaxis['CTI']=np.where(df_iaxis['PRODUCTO'].isin(list(cti['PRODUCTO'])),1,0)
```

El código se encarga de **validar la unicidad** de los registros en un conjunto de datos llamado `df_iaxis`. 

1. **Definición de columnas**: Selecciona las columnas a considerar para identificar duplicados, dependiendo del tipo de contrato.
2. **Identificación de duplicados**: Busca registros duplicados en `df_iaxis` y los almacena en `duplicados_iaxis`.
3. **Reporte de duplicados**: Si
```python
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
```

El código realiza **uniones** y **filtrados** en un conjunto de datos llamado `df_iaxis` utilizando información de otras tablas.

1. **Unión con `estados_iaxis`**: Se agrega información sobre el estado, manteniendo solo las filas que coinciden.
2. **Filtrado**: Se seleccionan solo las filas donde `APLICA ESTADO` es igual a 1.
3. **Unión con `polizas_pyme
```python
df_iaxis=df_iaxis.merge(estados_iaxis[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
df_iaxis=df_iaxis[df_iaxis['APLICA ESTADO']==1].copy()
df_iaxis=df_iaxis.merge(polizas_pyme,how='left',on=['POLIZA'])
df_iaxis['TIPO_POLIZA_LETRA']=np.where(df_iaxis['TIPO_POLIZA_LETRA'].isnull(),np.where(df_iaxis['TIPO_POLIZA']==1,'I','C'),df_iaxis['TIPO_POLIZA_LETRA'])
```

El código realiza un **tratamiento de saldos insolutos** específicamente para el contrato de **desgravamen no licitado**. 

1. **Carga de datos**: Se obtiene un conjunto de datos desde un archivo de texto que contiene información sobre saldos insolutos.
2. **Transformación de datos**: Se modifica la columna `NRO_OPERACION` para eliminar caracteres no deseados y convertirla a un tipo numérico.
3. **Fusión de datos**: Se
```python
if contrato =='Desgravamen No Licitado':
saldos_insolutos_detalle: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_si}1. Inputs Auxiliares\\Saldos Insolutos\\Saldos Insolutos {periodo}.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
saldos_insolutos_detalle['NRO_OPERACION']=saldos_insolutos_detalle['NRO_OPERACION'].astype(str).str.replace('K','').astype(float)
df_iaxis=df_iaxis.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
```

### Lectura de Bases de Datos GES

Este código se encarga de **leer** información de las bases de datos del sistema GES. A través de este proceso, se obtienen datos que serán utilizados en análisis posteriores.
```python
if base_ges==1:
```

El código se encarga de **leer datos** desde diferentes fuentes:

1. **Carga de un archivo CSV**: Se utiliza `pd.read_csv` para leer un archivo y almacenarlo en `df_ges`, especificando el separador, formato decimal, y otras configuraciones.
  
2. **Carga de hojas de Excel**: Se obtienen tres tablas desde un archivo Excel usando `tables.get_table_xlsx`, almacenándolas en `estados_ges`, `forma_pago`
```python
df_ges: pd.DataFrame=pd.read_csv(ruta_input+archivo_input_ges,sep=separador_input,decimal=decimal_input,parse_dates=cols_date_ges,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
estados_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Estados GES')
forma_pago: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Forma Pago')
planes_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Planes GES')
for col in cols_date_ges:
if df_ges[col].dtype!='datetime64[ns]': df_ges[col]=pd.to_datetime(df_ges[col],format = '%d-%m-%Y', errors='coerce')
```

La instrucción `df_ges['CTI']=0` **asigna un valor de 0** a una nueva columna llamada **CTI** en el DataFrame **df_ges**. Esto se realiza como parte de **transformaciones iniciales** en los datos.
```python
df_ges['CTI']=0
```

El código verifica si la columna **`PERIOD_TASA`** existe en el DataFrame **`df_ges`**. Si está presente, ajusta los valores de la columna **`TASA_CRED`** según el tipo de periodo:

- Si **`PERIOD_TASA`** es **`'M'`** (mensual), divide **`TASA_CRED`** entre 100.
- Si es **`'A'`** (anual
```python
if 'PERIOD_TASA' in df_ges.columns:df_ges['TASA_CRED']=np.where(df_ges['PERIOD_TASA']=='M',df_ges['TASA_CRED']/100,np.where(df_ges['PERIOD_TASA']=='A',(1+df_ges['TASA_CRED']/100)**(1/12)-1,df_ges['TASA_CRED']/100))
```

Si la variable `clasificacion_contrato` es igual a **'Cesantia PU'**, se ejecuta la función `corrige_tasas_ges` sobre el DataFrame `df_ges` utilizando los parámetros definidos en **`parameters`**. 

Esto **corrige las tasas** específicamente para los contratos de **prima única de cesantía**.
```python
if clasificacion_contrato=='Cesantia PU': df_ges=corrige_tasas_ges(df_ges, parameters)
```

El código realiza una **validación de unicidad** en un conjunto de datos llamado `df_ges`. 

1. **Identificación de duplicados**: Busca registros duplicados basándose en ciertas columnas (`campo_rut_duplicados`, `POLIZA`, `CERTIFICADO`, `NRO_OPERACION`, `COD_COB`). Si encuentra duplicados, genera un reporte indicando cuántos registros duplicados hay y los guarda en un archivo CSV.

2. **Eliminación
```python
duplicados_ges=df_ges.loc[df_ges.duplicated(subset=[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','COD_COB'],keep=False)]
if not (duplicados_ges.empty):
escribe_reporta(archivo_reporte,'El dataframe GES de entrada contiene {} registros duplicados. Revisar!'.format(duplicados_ges.shape[0]))
duplicados_ges.to_csv(ruta_output+'0. Duplicados GES.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
df_ges=df_ges.drop_duplicates()
duplicados_drop_ges=df_ges.loc[df_ges.duplicated(subset=['POLIZA','CERTIFICADO','NRO_OPERACION','COD_COB'],keep=False)]
if not (duplicados_drop_ges.empty):
escribe_reporta(archivo_reporte,'Luego de eliminar duplicados, la base input de GES contiene {} registros duplicados. Revisar!'.format(duplicados_drop_ges.shape[0]))
duplicados_drop_ges.to_csv(ruta_output+'0. Duplicados GES Drop.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
```

El código establece condiciones para crear la variable **FECHA_ANULACION** en un DataFrame llamado `df_ges`, dependiendo del tipo de contrato.

1. Si el contrato no es **'Cesantia PU'** y no es **'K-Fijo'**, se asigna a **FECHA_ANULACION** una fecha basada en **FECHA_VENCIMIENTO** dentro de un rango específico.
   
2. Si el contrato es **'K-Fijo'**, se inicializa
```python
if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'): df_ges['FECHA_ANULACION']=pd.to_datetime(np.where((df_ges['FECHA_VENCIMIENTO']>=fecha_inicio_mes)&(df_ges['FECHA_VENCIMIENTO']<=fecha_cierre),df_ges['FECHA_VENCIMIENTO'].astype(str),''), format = '%Y-%m-%d', errors='coerce')
elif contrato=='K-Fijo':
df_ges['FEC AUX NA']=0
df_ges['FEC AUX NA']=pd.to_datetime(df_ges['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
df_ges['FECHA_ANULACION']=np.where(~df_ges['FECHA_RENUNCIA'].isnull(),df_ges['FECHA_RENUNCIA'],np.where(~df_ges['FECHA_PREPAGO'].isnull(),df_ges['FECHA_PREPAGO'],np.where(df_ges['FECHA_FIN_VIGENCIA']==df_ges['FECHA_VENCIMIENTO'],df_ges['FEC AUX NA'],df_ges['FECHA_FIN_VIGENCIA'])))
df_ges=df_ges.drop(columns=['FEC AUX NA'],axis=1)
df_ges['PERIODO_CONTABILIZACION']=np.where(df_ges['FECHA_ANULACION'].isnull(),np.nan,np.maximum(df_ges['PERIODO_CONTABILIZACION'],df_ges['FECHA_ANULACION'].dt.year*100+df_ges['FECHA_ANULACION'].dt.month))
df_ges['FECHA_CONTABILIZACION_ANULACION']=pd.to_datetime(df_ges['PERIODO_CONTABILIZACION'],format='%Y%m', errors='coerce')+ MonthEnd(0)
```

Las instrucciones de código realizan **transformaciones finales** en un conjunto de datos llamado `df_ges`. 

1. **Unión de datos**: Se combina `df_ges` con otro conjunto de datos `forma_pago` usando una clave común llamada `FORMA_PAGO`.
2. **Creación de nueva columna**: Se copia el contenido de la columna `TIPO_POLIZA` a una nueva columna llamada `TIPO_POLIZA_LETRA`.
3. **Condicional**:
```python
df_ges=df_ges.merge(forma_pago,how='left',on='FORMA_PAGO')
df_ges['TIPO_POLIZA_LETRA']=df_ges['TIPO_POLIZA']
df_ges['TIPO_POLIZA']=np.where(df_ges['TIPO_POLIZA_LETRA']=='C',2,1)
df_ges['BASE']='GES'
```

El código asigna valores a las columnas `FINI_RENOV_ANUAL` y `FFIN_RENOV_ANUAL` del DataFrame `df_ges`. Estos valores se obtienen mediante la función `calculo_fechas_renovacion`, que calcula las fechas de renovación de contratos de prima recurrente utilizando varias columnas como parámetros, incluyendo `FECHA_EFECTO`, `FECHA_VENCIMIENTO`, `FECHA_ANULACION` y `FORMA_PAGO_COD
```python
df_ges['FINI_RENOV_ANUAL'],df_ges['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_ges, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo)
```

El código ajusta el valor de la columna **IPRIANU** en el DataFrame **df_ges**. Si el **tipo_contrato** es igual a **'Vida'**, multiplica **IPRIANU** por **FACTOR ANUALIZACION**. 

Esto se utiliza para la **anualización de la prima de vida GES**.
```python
if tipo_contrato=='Vida':df_ges['IPRIANU']=df_ges['IPRIANU']*df_ges['FACTOR ANUALIZACION']
```

El código realiza las siguientes acciones:

1. **Combina dos conjuntos de datos**: Utiliza `merge` para unir `df_ges` con `planes_ges` basándose en las columnas `PRODUCTO` y `PLAN_DESC`. Esto se hace de manera **izquierda** (`how='left'`), lo que significa que se conservarán todos los registros de `df_ges` y se agregarán los datos de `planes_ges` donde coincidan.

2.
```python
df_ges=df_ges.merge(planes_ges,how='left',on=['PRODUCTO','PLAN_DESC'])
df_ges['COD_PLAN']=df_ges['COD_PLAN'].fillna(0)
```

El código realiza las siguientes acciones:

1. **Fusión de datos**: Combina `df_ges` con `estados_ges`, añadiendo la columna `APLICA ESTADO` donde el `ESTADO` coincide. Esto permite identificar si un estado aplica o no.

2. **Filtrado**: Se queda solo con las filas donde `APLICA ESTADO` es igual a 1, es decir, aquellos estados que sí aplican.

3. **
```python
df_ges=df_ges.merge(estados_ges[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
df_ges=df_ges[df_ges['APLICA ESTADO']==1].copy()
if 'POLVIGENTE' in df_ges.columns: df_ges=df_ges[~df_ges['POLVIGENTE'].isin([9])]
```

El código realiza un **tratamiento específico** para los saldos insolutos en el caso de un contrato denominado **"Desgravamen No Licitado"**. 

1. Asigna los valores de la columna `POLASECFI` a una nueva columna llamada `ICAPITAL`.
2. Elimina las columnas `POLCFIORI` y `POLASECFI` del DataFrame.
3. Convierte la columna `NRO_OPERACION` a un tipo numérico,
```python
if contrato=='Desgravamen No Licitado':
df_ges['ICAPITAL']=df_ges['POLASECFI']
df_ges.drop(columns=['POLCFIORI','POLASECFI'],axis=1,inplace=True)
df_ges['NRO_OPERACION']=pd.to_numeric(df_ges['NRO_OPERACION'],errors = 'coerce')
df_ges=df_ges.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
```

El código combina diferentes conjuntos de datos (dataframes) según ciertas condiciones. 

- Si **ambas bases** (`base_iaxis` y `base_ges`) están activas (igual a 1), se **unen** los dataframes `df_iaxis` y `df_ges`.
- Si solo `base_iaxis` está activa, se utiliza únicamente `df_iaxis`.
- Si solo `base_ges` está activa, se utiliza `df_
```python
if (base_iaxis==1)&(base_ges==1):
df_0_0: pd.DataFrame=pd.concat([df_iaxis,df_ges],axis=0)
elif base_iaxis==1:
df_0_0: pd.DataFrame=df_iaxis
elif base_ges==1:
df_0_0: pd.DataFrame=df_ges
else:
return pd.DataFrame()
```

### Cálculos con las bases de GES a iAxis unidas

Se realizarán cálculos utilizando las bases de datos de GES y iAxis que han sido combinadas. Esto permitirá obtener resultados más completos y precisos a partir de la información unificada.

El código realiza **cálculos de variables adicionales** y **cambia el nombre de algunas variables** en un conjunto de datos. 

1. **Reporte de prima neta**: Se registra la suma de una columna específica en un archivo de reporte.
2. **Relleno de valores nulos**: Se reemplazan valores nulos en la columna `NRO_OPERACION` por 0.
3. **Limpieza de texto**: Se eliminan espacios en blanco de
```python
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
```

Si la columna **'FEC_NAC'** está presente en el DataFrame **df_0_1**, se registra un mensaje indicando que se está calculando la edad de ingreso. Luego, se calculan dos nuevas columnas: **'EDAD INGRESO'** y **'ISSUE EDAD INGR'**. Este cálculo se basa en el **RUT**, la **FEC_NAC** y la **FECHA_EFECTO**, entre otros parámetros.
```python
if 'FEC_NAC' in df_0_1.columns:
escribe_reporta(archivo_reporte,'Calculando edad de ingreso')
df_0_1['EDAD INGRESO'],df_0_1['ISSUE EDAD INGR']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],df_0_1['FECHA_EFECTO'],edad_casos_perdidos,108,archivo_reporte,reporta_issues=1, edad_inf = 18, aplica_edad_prom_cartera = 1)
```

El código realiza cálculos específicos de fechas según el tipo de contrato. 

- Si el contrato **no es** 'Cesantia PU' y **no es** 'K-Fijo', se calculan las fechas de renovación y se asignan a las columnas `FINI_RENOV_ANUAL` y `FFIN_RENOV_ANUAL`. Luego, se determina la fecha de fin de exposición (`FECHA FIN EXP`) considerando la fecha de anulación y la fecha de vencimiento.

- Si
```python
if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'):
df_0_1['FINI_RENOV_ANUAL'],df_0_1['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_0_1, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo,0)
df_0_1['FECHA FIN EXP']=np.where(~df_0_1['FECHA_ANULACION'].isnull(),df_0_1['FECHA_ANULACION'],np.where(df_0_1['FECHA_VENCIMIENTO'].isnull(),df_0_1['FFIN_RENOV_ANUAL'],df_0_1['FECHA_VENCIMIENTO']))
else:
df_0_1['FEC AUX NA']=0
df_0_1['FEC AUX NA']=pd.to_datetime(df_0_1['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
df_0_1['FECHA_ANULACION']=np.where(df_0_1['FECHA_ANULACION']<=fecha_cierre,df_0_1['FECHA_ANULACION'],df_0_1['FEC AUX NA'])
```

### Cálculos Genéricos para Bases de Vida Prima Recurrente

Este código se encarga de realizar **cálculos** relacionados con las bases de vida para primas recurrentes. Estos cálculos son fundamentales para determinar el costo y la viabilidad de productos de seguros que requieren pagos periódicos.
```python
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
```

#### Cálculo de Productos con Capital

Este código se encargará de calcular ciertos productos financieros que tienen en cuenta el **saldo insoluto**. Esto es especialmente relevante para aquellos productos que dependen del capital pendiente.
```python
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
```

El código realiza las siguientes acciones:

1. **Filtrado de datos**: Se crea un nuevo conjunto de datos `df_331` que contiene solo las filas donde el producto es 331, la base es 'IAXIS' y el código de cobertura es 12.

2. **Exclusión de datos**: Se genera `df_resto`, que incluye todas las filas de `df_0_5` que no están en `df_331`.

3. **S
```python
df_331 = df_0_5[(df_0_5['PRODUCTO']==331)&(df_0_5['BASE']=='IAXIS')&(df_0_5['CODIGO COBERTURA']==12)].copy()
df_resto = df_0_5[~df_0_5.index.isin(df_331.index)].copy()
df_331['PRIMA NETA ANUAL'] = df_331.groupby('SSEGURO')['PRIMA NETA ANUAL'].transform('sum')
df_331 = df_331[df_331['CODIGO COBERTURA IAXIS']==1200].copy()
df_0_6 = pd.concat([df_resto,df_331]).reset_index(drop=True)
df_final_0=df_0_6.copy()
else: df_final_0=df_0_3.copy()
```

#### Tratamiento de Saldo Insoluto para Multisocios

Este código se encarga de gestionar los saldos que no han sido pagados por los multisocios. Su objetivo es asegurar que se manejen adecuadamente las deudas pendientes, facilitando así un mejor control financiero.
```python
if contrato in ['Multisocios']:
df_0_3['FECHA_FIN_CRED']=np.where(df_0_3['BASE']=='GES',np.maximum(df_0_3['FECHA_VENCIMIENTO'],df_0_3['FECHA_FIN_CRED']),df_0_3['FECHA_VENCIMIENTO'])
df_0_3['NCUOTAS']=((df_0_3['FECHA_FIN_CRED']-df_0_3['FECHA_EFECTO']).dt.days/365*12).round(0)
df_0_3['NCUOTAS FALTANTES']=((df_0_3['FECHA_FIN_CRED']-fecha_cierre).dt.days/365*12).round(0)
df_0_3['PERIODO_EFECTO']=df_0_3['FECHA_EFECTO'].dt.year*100+df_0_3['FECHA_EFECTO'].dt.month
df_0_3=completa_campo_total(df_0_3,'TASA_CRED',[['PRODUCTO','PERIODO_EFECTO'],['PERIODO_EFECTO']], parameters)
df_0_3['SALDO INSOLUTO CALCULADO']=np.where(df_0_3['FECHA_FIN_CRED']<fecha_cierre,0,df_0_3['ICAPITAL']*(1-(1+df_0_3['TASA_CRED_FINAL'])**(-df_0_3['NCUOTAS FALTANTES']))/(1-(1+df_0_3['TASA_CRED_FINAL'])**(-df_0_3['NCUOTAS'])))
df_0_3['MONTO ASEGURADO']=np.maximum(df_0_3['SALDO INSOLUTO CALCULADO'],0)
df_final_0=df_0_3.copy()
```

### Cálculos para K-Fijo

Este segmento de código se encarga de realizar **cálculos específicos** relacionados con el concepto de K-fijo. Estos cálculos son fundamentales para el análisis y procesamiento de datos en el contexto que se esté trabajando.
```python
elif (tipo_contrato=='Vida')&(contrato=='K-Fijo'):
df_0_1['EXPOSICION MENSUAL']=1
escribe_reporta(archivo_reporte,'Calculando edad de renovacion')
df_0_1['EDAD RENOVACION'],df_0_1['ISSUE EDAD RENOV']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],fecha_inicio_mes,edad_casos_perdidos,108,archivo_reporte,reporta_issues=1, edad_inf = 18, aplica_edad_prom_cartera = 1)
df_0_1['PLAZO MESES']=np.maximum(1,round((df_0_1['FECHA_VENCIMIENTO']-df_0_1['FECHA_EFECTO']).dt.days/(365.25/12),0))
df_0_1['MONTO ASEGURADO']=df_0_1['ICAPITAL']
df_0_2=df_0_1[df_0_1['FECHA_EFECTO']<=fecha_cierre]
df_final_0=df_0_2.copy()
```

### Exportación de Edades con Problemas

Este código se encargará de **exportar** datos relacionados con las edades que presentan **problemas**. Se procesarán y organizarán las edades para facilitar su análisis y uso posterior.

El código realiza las siguientes acciones:

1. **Verifica columnas**: Comprueba si existen las columnas `'ISSUE EDAD INGR'` y `'ISSUE EDAD RENOV'` en el dataframe `df_final_0`.

2. **Filtra y exporta datos**: Si hay problemas en las edades de ingreso o renovación (valores mayores a 0), filtra los registros correspondientes y los guarda en archivos CSV:
   - Para **edades de ingreso**:
```python
if 'ISSUE EDAD INGR' in df_final_0.columns:
if sum(df_final_0['ISSUE EDAD INGR'])>0: df_final_0[df_final_0['ISSUE EDAD INGR']==1].to_csv(ruta_output+'0. Edades de Ingreso a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
if 'ISSUE EDAD RENOV' in df_final_0.columns:
if sum(df_final_0['ISSUE EDAD RENOV'])>0: df_final_0[df_final_0['ISSUE EDAD RENOV']==1].to_csv(ruta_output+'0. Edades de Renovacion a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
escribe_reporta(archivo_reporte,'El dataframe input luego de ser pre-procesado posee una prima neta de {}'.format(np.nansum(df_final_0['PRIMA NETA ANUAL'])))
```

## Últimos Filtros

Se aplicarán los **últimos filtros** a los datos para asegurar que la información sea precisa y relevante antes de su análisis final. Estos filtros ayudarán a eliminar cualquier dato no deseado o erróneo, mejorando así la calidad de los resultados.

El código crea una nueva variable llamada `df_final_1` que contiene una copia de los datos de `df_final_0`. Solo se incluyen las filas donde la columna **EXPOSICION MENSUAL** es mayor que 0. Esto permite trabajar con un subconjunto específico de los datos originales.
```python
df_final_1=df_final_0[df_final_0['EXPOSICION MENSUAL']>0].copy()
```

El código filtra un conjunto de datos para mantener solo aquellos registros que cumplen ciertas condiciones relacionadas con fechas. 

1. **Primera línea**: Se crea `df_final_2` que incluye registros de `df_final_1` donde la **fecha de vencimiento** es mayor o igual a `fecha_cierre` o es nula.
   
2. **Segunda línea**: Se crea `df_final_3` que incluye registros de `df_final_2` donde la
```python
df_final_2=df_final_1[(df_final_1['FECHA_VENCIMIENTO']>=fecha_cierre)|(df_final_1['FECHA_VENCIMIENTO'].isnull())]
df_final_3=df_final_2[(df_final_2['FECHA_ANULACION']>=fecha_cierre)|(df_final_2['FECHA_ANULACION'].isnull())]
return df_final_3
```