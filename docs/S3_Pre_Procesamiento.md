# Modulo S3_Pre_Procesamiento


Preprocesamiento de las bases de datos


# Funcion `pre_procesamiento`

La función `pre_procesamiento` se encarga de preparar los datos necesarios para el cálculo de reaseguro, ya sea de primas o siniestros. Para ello, realiza las siguientes acciones:

1. **Lectura de Datos**: Obtiene la información de dos sistemas de administración de bases de datos, GES e IAXIS.

2. **Transformación de Datos**: Antes de proceder con los cálculos de reaseguro, transforma los datos según el tipo de contrato de reaseguro. Esto incluye:
   - Diferenciar entre contratos de prima única o recurrente.
   - Aplicar un tratamiento distinto para los siniestros, que se clasifican en Vida y Generales.

3. **Parámetros y Tablas**: Utiliza parámetros y tablas proporcionados para guiar el proceso de transformación y cálculo.

4. **Salida**: Devuelve un `DataFrame` que está listo para ser utilizado en el cálculo de primas o siniestros de reaseguro, según sea necesario.

En resumen, esta función es crucial para asegurar que los datos estén en el formato adecuado antes de realizar cálculos complejos de reaseguro.

## Definición de Variables en `parameters`

Se establecen variables dentro de un objeto llamado `parameters` para facilitar su uso en una función posterior. Estas variables son de uso frecuente y permiten que la función acceda a ellas de manera eficiente y organizada. Esto mejora la legibilidad y el mantenimiento del código al centralizar la gestión de estas variables.

- **Funcionalidad**: El código extrae y almacena características específicas de un contrato desde un diccionario de parámetros.

- Se accede a un diccionario llamado `parameters` para obtener valores asociados a claves específicas.
- Se definen cuatro variables: `tipo_calculo`, `tipo_contrato`, `contrato` y `clasificacion_contrato`.
- Cada variable almacena un valor correspondiente a una característica del contrato, extraído del diccionario `parameters`.
- Estas variables permiten trabajar con las características del contrato de manera más directa y legible en el código.
```python
# Caracteristicas del contrato
tipo_calculo: str = parameters.parameters['tipo_calculo']
tipo_contrato: str = parameters.parameters['tipo_contrato']
contrato: str = parameters.parameters['contrato']
clasificacion_contrato: str = parameters.parameters['clasificacion_contrato']
```

- **Funcionalidad**: El código extrae y almacena valores de fechas y un periodo desde un conjunto de parámetros.

- Se definen tres variables: `fecha_cierre`, `fecha_inicio_mes` y `periodo`.
- Estas variables obtienen sus valores desde un diccionario llamado `parameters.parameters`.
- `fecha_cierre` y `fecha_inicio_mes` son de tipo `datetime.datetime`, mientras que `periodo` es un entero.
```python
# Fechas de cierre
fecha_cierre: datetime.datetime = parameters.parameters['fecha_cierre']
fecha_inicio_mes: datetime.datetime = parameters.parameters['fecha_inicio_mes']
periodo: int = parameters.parameters['periodo']
```

- **Funcionalidad del código**: Extrae valores de un diccionario llamado `parameters` y los asigna a variables específicas.

- Define cinco variables, cada una con un tipo de dato específico: `str`, `int`, `int`, `float`, y `Any`.
- Cada variable se inicializa con un valor obtenido de un diccionario llamado `parameters`.
- El diccionario `parameters` contiene claves que corresponden a los nombres de las variables.
- Las variables almacenan configuraciones o parámetros que pueden ser utilizados en otras partes del programa.
- El uso del tipo `Any` para `archivo_reporte` indica que puede contener cualquier tipo de dato.
```python
# Campos tecnicos mas especializados
campo_rut_duplicados: str = parameters.parameters['campo_rut_duplicados']
edad_casos_perdidos: int = parameters.parameters['edad_casos_perdidos']
dias_exposicion: int = parameters.parameters['dias_exposicion']
tdm_mensual: float = parameters.parameters['tdm_mensual']
archivo_reporte: Any = parameters.parameters['archivo_reporte']
```

- **Funcionalidad**: El código extrae valores de un diccionario para configurar parámetros relacionados con bases y archivos de entrada.

- Define dos variables enteras que representan bases específicas utilizando valores de un diccionario llamado `parameters`.
- Asigna nombres de archivos a dos variables de tipo cadena, también obtenidos del mismo diccionario.
- Utiliza un diccionario llamado `parameters` para acceder a los valores necesarios mediante claves específicas.
- Los valores extraídos se almacenan en variables para su uso posterior en el programa.
```python
# Sobre que bases considerar y cuales nombres tienen los archivos que debemos leer
base_iaxis: int = parameters.parameters['base_iaxis']
base_ges: int = parameters.parameters['base_ges']
archivo_input: str = parameters.parameters['archivo_input']
archivo_input_ges: str = parameters.parameters['archivo_input_ges']
```

- **Funcionalidad del código**: Configura los separadores y formatos decimales para la importación y exportación de datos.

- El código obtiene valores de configuración desde un objeto llamado `parameters`.
- Define el separador de campos y el símbolo decimal para los datos de entrada.
- Establece el separador de campos y el símbolo decimal para los datos de salida.
- Estos valores se almacenan en variables específicas para su uso posterior en el procesamiento de datos.
```python
# Sobre como debemos importar y exportar la data
separador_input: str = parameters.parameters['separador_input']
decimal_input: str = parameters.parameters['decimal_input']
separador_output: str = parameters.parameters['separador_output']
decimal_output: str = parameters.parameters['decimal_output']
```

Este código configura rutas de archivos y escribe mensajes en un archivo de reporte y en la consola.

- Define varias rutas de archivos utilizando un diccionario llamado `parameters`.
- Las rutas incluyen `ruta_output`, `ruta_input`, `ruta_pyme`, `ruta_otros`, `ruta_si`, y `ruta_uso_seguro`.
- Utiliza una función `escribe_reporta` para escribir un mensaje en un archivo de reporte.
- El mensaje indica el inicio de la lectura de bases de datos, incluyendo la fecha y hora actual.
- La fecha y hora se formatean usando `time.strftime` y `time.localtime`.
- Imprime en la consola el valor de una variable llamada `contrato`.
- El código parece estar relacionado con la gestión de archivos y la generación de reportes.
- La estructura sugiere que es parte de un sistema más grande que maneja datos y reportes.
```python
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
```

## Preprocesamiento de los Expuestos para Cálculo de Prima de Reaseguro

Este proceso prepara los datos de los asegurados para calcular la prima de reaseguro. Se ajustan y limpian los datos para asegurar que sean precisos y estén listos para el análisis. Esto es crucial para determinar el costo adecuado del seguro basado en el riesgo.

- **Funcionalidad**: El código verifica si una variable llamada `tipo_calculo` es igual a la cadena `'Prima de Reaseguro'`.
- **Explicación**: Comprueba si el valor de `tipo_calculo` coincide con `'Prima de Reaseguro
```python
if tipo_calculo=='Prima de Reaseguro':
```

- **Funcionalidad del código**: Carga datos desde varios archivos de texto y Excel en diferentes DataFrames de pandas.

- Se utilizan rutas de archivo dinámicas para cargar datos desde archivos de texto y Excel.
- Los archivos de texto se leen con configuraciones específicas de decimales y separadores, y no se consideran campos de fecha.
- Los datos se almacenan en objetos `DataFrame` de pandas, que son estructuras de datos tabulares.
- Se carga un archivo Excel específico en un DataFrame, seleccionando una hoja llamada 'Coberturas GES'.
- Condicionalmente, si una variable llamada `contrato` tiene un valor específico, se carga un archivo de texto adicional en otro DataFrame.
```python
# Inputs de otras fuentes
polizas_pyme: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_pyme}1. Inputs Auxiliares\\Polizas Pyme\\Polizas Pyme.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
cti: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_otros}1. Inputs Auxiliares\\Otros\\CTI.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
innominadas: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_otros}1. Inputs Auxiliares\\Otros\\polizas_innominadas.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
cobs_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Coberturas GES')
if contrato=='Complementario UC': uso_seguro_com_uc: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_uso_seguro}1. Inputs Auxiliares\\Com UC\\COM UC Uso del Seguro Hist {periodo}.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
```

- **Funcionalidad del código**: Asigna listas de campos de tipo fecha a variables basadas en el tipo y clasificación del contrato.

1. Se verifica si el contrato es de tipo "Vida" y no está en una lista específica, asignando dos listas de campos de fecha a las variables correspondientes.
2. Si el contrato es de tipo "Vida" y está en una lista diferente, se asignan listas de campos de fecha que incluyen fechas de inicio y fin de crédito.
3. Para contratos de tipo "Vida" con el nombre "K-Fijo", se asignan listas de campos de fecha que incluyen fechas de prepagos y renuncias.
4. Si la clasificación del contrato es "Cesantia PU", se asignan listas de campos de fecha similares a las de "K-Fijo".
5. Para contratos de tipo "Generales" que incluyen "Incendio y Sismo", se asignan listas de campos de fecha más reducidas.
6. Finalmente, si el contrato es de tipo "Generales" y se llama "Cesantia PR", se asigna una lista de campos de fecha a una sola variable.
```python
# Dependiendo del contrato, son diferentes los campos de tipo fecha que debemos transformar
if (tipo_contrato=='Vida')&(contrato not in ['K-Fijo','Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO']
elif (tipo_contrato=='Vida')&(contrato in ['Desgravamen No Licitado','Multisocios']): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_INICIO_CRED','FECHA_FIN_CRED']
elif (tipo_contrato=='Vida')&(contrato=='K-Fijo'): cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
elif clasificacion_contrato =='Cesantia PU': cols_date,cols_date_ges=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FECHA_CONTABILIZACION_ANULACION'],['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_PREPAGO','FECHA_RENUNCIA','FECHA_FIN_VIGENCIA']
elif (tipo_contrato=='Generales')&('Incendio y Sismo' in contrato): cols_date,cols_date_ges=['FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION'],['FECHA_EFECTO','FECHA_VENCIMIENTO']
elif (tipo_contrato=='Generales')&(contrato=='Cesantia PR'): cols_date: list[str]=['FEC_NAC','FECHA_EFECTO','FECHA_VENCIMIENTO','FINI_RENOV_ANUAL','FFIN_RENOV_ANUAL','FECHA_ANULACION']
```

### Lectura de Bases de Datos IAXIS

Este proceso se encarga de **cargar y leer datos** desde una base de datos específica llamada IAXIS. Utiliza herramientas de programación para **extraer información** de manera eficiente y organizada. El objetivo es **facilitar el acceso** a los datos almacenados para su posterior análisis o procesamiento.

- **Funcionalidad**: El código verifica si la variable `base_iaxis` es igual a 1.
- **Explicación**: Si la condición es verdadera, se ejecutará el bloque de código que sigue a esta declaración `if`.
```python
if base_iaxis==1:
```

- **Funcionalidad del código**: Carga datos desde un archivo CSV y hojas de Excel, y asegura que ciertas columnas de fechas estén en el formato correcto.

- Utiliza `pandas` para leer un archivo CSV en un DataFrame llamado `df_iaxis`, especificando separador, formato de decimales, y columnas de fecha.
- Carga dos tablas desde un archivo Excel en DataFrames separados, `estados_iaxis` y `canales_venta`, usando una función que extrae hojas específicas.
- Recorre las columnas de fecha especificadas y convierte su tipo de datos a `datetime64[ns]` si no lo están ya.
- La conversión de fechas utiliza un formato específico y maneja errores con la opción `coerce`, que convierte valores no convertibles a `NaT` (Not a Time).
- El archivo CSV se lee con codificación `latin-1` y la opción `low_memory=False` para optimizar la carga de datos.
```python
# Lectura de BBDD y tablas de parametria
df_iaxis: pd.DataFrame=pd.read_csv(ruta_input+archivo_input,sep=separador_input,decimal=decimal_input,parse_dates=cols_date,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
estados_iaxis: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Estados IAXIS')
canales_venta: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Canal Venta')
for col in cols_date:
	if df_iaxis[col].dtype!='datetime64[ns]': df_iaxis[col]=pd.to_datetime(df_iaxis[col],format = '%d-%m-%Y', errors='coerce')
```

- **Funcionalidad del código**: Realiza transformaciones y ajustes en un DataFrame llamado `df_iaxis`.

1. Redondea los valores de las columnas `IPRIANU` e `ICAPITAL` a cuatro decimales.
2. Asigna la cadena `'IAXIS'` a una nueva columna llamada `BASE`.
3. Calcula el periodo de contabilización en formato `YYYYMM` usando la columna `FECHA_CONTABILIZACION_ANULACION`, si esta existe.
4. Verifica si la columna `NRO_OPERACION` existe; si no, la crea con valor 0.
5. Si `NRO_OPERACION` ya existe, reemplaza los valores nulos con 0.
```python
# Algunas transformaciones iniciales
df_iaxis['IPRIANU']=round(df_iaxis['IPRIANU'],4)
df_iaxis['ICAPITAL']=round(df_iaxis['ICAPITAL'],4)
df_iaxis['BASE']='IAXIS'
if 'FECHA_CONTABILIZACION_ANULACION' in df_iaxis.columns:df_iaxis['PERIODO_CONTABILIZACION']=df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.year*100+df_iaxis['FECHA_CONTABILIZACION_ANULACION'].dt.month
if 'NRO_OPERACION' not in df_iaxis.columns:df_iaxis['NRO_OPERACION']=0
else: df_iaxis['NRO_OPERACION']=df_iaxis['NRO_OPERACION'].fillna(0)
```

- **Funcionalidad**: El código agrega información del nombre del canal de venta a un DataFrame si la columna correspondiente existe.
- Si el DataFrame contiene la columna 'CANAL_VENTA', se realiza una combinación con otro DataFrame que contiene los nombres de los canales de venta, utilizando
```python
# Obtenemos nombre del canal de venta
if 'CANAL_VENTA' in df_iaxis.columns: df_iaxis=df_iaxis.merge(canales_venta,how='left',on=['CANAL_VENTA'])
```

- **Funcionalidad del código**: Asigna un valor binario a una columna en un DataFrame basado en condiciones específicas relacionadas con un contrato.
- Si el contrato es "Complementario UC", se actualiza la columna 'USO SEGURO' en el DataFrame `df_
```python
# uso del seguro para el contrato Complementario UC
if contrato=='Complementario UC': df_iaxis['USO SEGURO']= np.where((df_iaxis['SSEGURO'].isin(uso_seguro_com_uc['SSEGURO']))&(df_iaxis['MOTIVO_BAJA']==306),1,0)
```

- El código ajusta la tasa de interés de los contratos en función del período especificado en el campo `PERIOD_TASA`.
- Si `PERIOD_TASA` es 12, divide la tasa de interés por 100; si es 1, convierte la tasa anual a mensual;
```python
# Tratamiento del campo PERIOD_TASA para aquellos contratos que poseen saldo insoluto
if 'PERIOD_TASA' in df_iaxis.columns:df_iaxis['TASA_CRED']=np.where(df_iaxis['PERIOD_TASA']==12,df_iaxis['TASA_CRED']/100,np.where(df_iaxis['PERIOD_TASA']==1,(1+df_iaxis['TASA_CRED']/100)**(1/12)-1,df_iaxis['TASA_CRED']/100))
```

- **Funcionalidad**: El código marca las pólizas como CTI (Colectivo de Tratamiento Individual) en un DataFrame.
- Utiliza la función `np.where` para asignar el valor 1 a la columna 'CTI' si el valor en 'PRODUCTO
```python
# Marcamos polizas CTI (Colectivo de Tratamiento Individual)
df_iaxis['CTI']=np.where(df_iaxis['PRODUCTO'].isin(list(cti['PRODUCTO'])),1,0)
```

- **Funcionalidad del código**: Verifica y maneja registros duplicados en un conjunto de datos llamado `iAxis`.

1. Define las columnas clave para identificar duplicados en función del tipo de contrato.
2. Busca registros duplicados en el DataFrame `df_iaxis` usando las columnas clave.
3. Si existen duplicados, genera un reporte indicando la cantidad de duplicados encontrados.
4. Guarda los registros duplicados en un archivo CSV para revisión.
5. Elimina los duplicados del DataFrame `df_iaxis`.
6. Verifica nuevamente si existen duplicados después de la eliminación.
7. Si aún hay duplicados, genera otro reporte indicando la cantidad restante.
8. Guarda los duplicados restantes en un segundo archivo CSV.
9. Utiliza funciones auxiliares para escribir reportes y manejar archivos.
10. El proceso asegura que los datos en `iAxis` sean únicos según las columnas especificadas.
```python
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
```

- **Funcionalidad del código**: Filtra y enriquece un DataFrame con información de estados y pólizas, asignando valores condicionales a una columna específica.

- El código comienza realizando una combinación (merge) del DataFrame `df_iaxis` con otro DataFrame llamado `estados_iaxis`, utilizando la columna 'ESTADO' como clave, para agregar la columna 'APLICA ESTADO'.
- Filtra `df_iaxis` para conservar solo las filas donde 'APLICA ESTADO' es igual a 1, asegurando que solo se mantengan los registros aplicables.
- Realiza otra combinación de `df_iaxis` con el DataFrame `polizas_pyme` usando la columna 'POLIZA' como clave, para añadir información adicional sobre pólizas.
- Asigna valores a la columna 'TIPO_POLIZA_LETRA' basándose en condiciones: si es nula, se asigna 'I' o 'C' dependiendo del valor de 'TIPO_POLIZA'; de lo contrario, conserva su valor actual.
```python
# Cruces con tablas de parametria
df_iaxis=df_iaxis.merge(estados_iaxis[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
df_iaxis=df_iaxis[df_iaxis['APLICA ESTADO']==1].copy()
df_iaxis=df_iaxis.merge(polizas_pyme,how='left',on=['POLIZA'])
df_iaxis['TIPO_POLIZA_LETRA']=np.where(df_iaxis['TIPO_POLIZA_LETRA'].isnull(),np.where(df_iaxis['TIPO_POLIZA']==1,'I','C'),df_iaxis['TIPO_POLIZA_LETRA'])
```

- **Funcionalidad del código**: Procesa y combina datos de saldos insolutos para contratos específicos de desgravamen no licitado.

- Verifica si el contrato es de tipo "Desgravamen No Licitado".
- Carga un archivo de texto que contiene detalles de saldos insolutos en un DataFrame de pandas, utilizando parámetros específicos para el formato del archivo.
- Convierte los valores de la columna 'NRO_OPERACION' a números flotantes, eliminando cualquier carácter 'K'.
- Fusiona el DataFrame de saldos insolutos con otro DataFrame llamado `df_iaxis` basándose en las columnas 'POLIZA', 'RUT' y 'NRO_OPERACION'.
```python
# Tratamiento de saldos insolutos para el contrato de desgravamen no licitado
if contrato =='Desgravamen No Licitado':
	saldos_insolutos_detalle: pd.DataFrame = tables.get_table_txt(file_path=f'{ruta_si}1. Inputs Auxiliares\\Saldos Insolutos\\Saldos Insolutos {periodo}.txt', decimal=decimal_input, separador=separador_input, campos_fecha=False)
	saldos_insolutos_detalle['NRO_OPERACION']=saldos_insolutos_detalle['NRO_OPERACION'].astype(str).str.replace('K','').astype(float)
	df_iaxis=df_iaxis.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
```

### Lectura de Bases de Datos GES

Este proceso implica la importación de datos desde una base de datos específica llamada GES. Se accede a la información almacenada en esta base de datos para su posterior análisis o manipulación. La lectura de datos es un paso fundamental para trabajar con información actualizada y relevante.

- **Funcionalidad del código**: Verifica si la variable `base_ges` es igual a 1.
- Si la condición es verdadera, se ejecutará el bloque de código que sigue a la declaración `if`.
```python
if base_ges==1:
```

- **Funcionalidad del código**: Carga datos desde archivos CSV y Excel en varios DataFrames de pandas, asegurando que ciertas columnas de fechas estén en el formato correcto.

1. Se lee un archivo CSV en un DataFrame llamado `df_ges`, especificando el separador, el formato decimal, y las columnas de fechas a parsear.
2. El archivo CSV se decodifica usando `latin-1` y se optimiza para manejar grandes volúmenes de datos con `low_memory=False`.
3. Se cargan tres tablas desde un archivo Excel en DataFrames separados: `estados_ges`, `forma_pago`, y `planes_ges`, cada uno desde una hoja específica.
4. Se verifica que las columnas de fechas en `df_ges` estén en el tipo de dato `datetime64[ns]`.
5. Si alguna columna de fechas no está en el formato correcto, se convierte usando `pd.to_datetime` con un formato específico y se manejan errores con `errors='coerce'`.
```python
# Lectura de BBDD y tablas de parametria
df_ges: pd.DataFrame=pd.read_csv(ruta_input+archivo_input_ges,sep=separador_input,decimal=decimal_input,parse_dates=cols_date_ges,date_format='%d-%m-%Y',encoding='latin-1',low_memory=False)
estados_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Estados GES')
forma_pago: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Forma Pago')
planes_ges: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Planes GES')
for col in cols_date_ges:
	if df_ges[col].dtype!='datetime64[ns]': df_ges[col]=pd.to_datetime(df_ges[col],format = '%d-%m-%Y', errors='coerce')
```

- **Funcionalidad del código**: Inicializa una columna llamada 'CTI' en un DataFrame con un valor de 0.
- Añade una nueva columna al DataFrame `df_ges` y establece su valor inicial en 0 para todas las filas.
```python
# Algunas transformaciones iniciales
df_ges['CTI']=0
```

- **Funcionalidad del código**: Ajusta la tasa de interés de los contratos según el periodo especificado en el campo `PERIOD_TASA`.
- Si el periodo es mensual ('M'), divide la tasa de interés por 100; si es anual ('A'), convierte la tasa anual
```python
# Tratamiento del campo PERIOD_TASA para aquellos contratos que poseen saldo insoluto
if 'PERIOD_TASA' in df_ges.columns:df_ges['TASA_CRED']=np.where(df_ges['PERIOD_TASA']=='M',df_ges['TASA_CRED']/100,np.where(df_ges['PERIOD_TASA']=='A',(1+df_ges['TASA_CRED']/100)**(1/12)-1,df_ges['TASA_CRED']/100))
```

- **Funcionalidad del código**: Ajusta las tasas para contratos de prima única de cesantía.
- Si el contrato está clasificado como "Cesantia PU", se aplican correcciones a las tasas en el DataFrame `df_ges` utilizando la función `corrige_t
```python
# Corrige las tasas para los contratos de prima unica de cesantia
if clasificacion_contrato=='Cesantia PU': df_ges=corrige_tasas_ges(df_ges, parameters)
```

- **Funcionalidad del código**: El código identifica y maneja registros duplicados en un DataFrame llamado `df_ges`.

1. Se busca duplicados en `df_ges` usando un conjunto específico de columnas, incluyendo un campo de identificación y otros campos relacionados.
2. Si se encuentran duplicados, se genera un reporte indicando la cantidad de registros duplicados.
3. Los registros duplicados se exportan a un archivo CSV para su revisión.
4. Se eliminan los duplicados del DataFrame `df_ges`.
5. Se verifica nuevamente si existen duplicados en `df_ges` después de la eliminación, pero con un conjunto reducido de columnas.
6. Si aún hay duplicados, se genera otro reporte con la cantidad de duplicados restantes.
7. Los duplicados restantes se exportan a un segundo archivo CSV.
8. Las funciones `escribe_reporta` y `to_csv` se utilizan para generar reportes y exportar datos, respectivamente.
9. Las variables como `archivo_reporte`, `ruta_output`, `separador_output`, y `decimal_output` configuran la salida de los reportes y archivos CSV.
```python
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
```

- **Funcionalidad del código**: El código asigna una fecha de anulación a contratos en un DataFrame basado en ciertas condiciones y actualiza el periodo de contabilización.

1. Se verifica si el contrato no es de tipo 'Cesantia PU' y no es 'K-Fijo'.
2. Si la condición anterior es verdadera, se asigna la fecha de vencimiento como fecha de anulación si está dentro de un rango específico.
3. Si el contrato es 'K-Fijo', se crea una columna auxiliar inicializada en cero.
4. La columna auxiliar se convierte a un formato de fecha.
5. La fecha de anulación se determina usando varias condiciones, como la presencia de fechas de renuncia o prepago.
6. La columna auxiliar se elimina después de su uso.
7. Se actualiza el periodo de contabilización usando la fecha de anulación si está disponible.
8. La fecha de contabilización de anulación se calcula y se ajusta al final del mes correspondiente.
```python
# Creacion de la variable FECHA_ANULACION (no existe en GES) que depende de que contrato estemos trabajando
if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'): df_ges['FECHA_ANULACION']=pd.to_datetime(np.where((df_ges['FECHA_VENCIMIENTO']>=fecha_inicio_mes)&(df_ges['FECHA_VENCIMIENTO']<=fecha_cierre),df_ges['FECHA_VENCIMIENTO'].astype(str),''), format = '%Y-%m-%d', errors='coerce')
elif contrato=='K-Fijo':
	df_ges['FEC AUX NA']=0
	df_ges['FEC AUX NA']=pd.to_datetime(df_ges['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
	df_ges['FECHA_ANULACION']=np.where(~df_ges['FECHA_RENUNCIA'].isnull(),df_ges['FECHA_RENUNCIA'],np.where(~df_ges['FECHA_PREPAGO'].isnull(),df_ges['FECHA_PREPAGO'],np.where(df_ges['FECHA_FIN_VIGENCIA']==df_ges['FECHA_VENCIMIENTO'],df_ges['FEC AUX NA'],df_ges['FECHA_FIN_VIGENCIA'])))
	df_ges=df_ges.drop(columns=['FEC AUX NA'],axis=1)
	df_ges['PERIODO_CONTABILIZACION']=np.where(df_ges['FECHA_ANULACION'].isnull(),np.nan,np.maximum(df_ges['PERIODO_CONTABILIZACION'],df_ges['FECHA_ANULACION'].dt.year*100+df_ges['FECHA_ANULACION'].dt.month))
	df_ges['FECHA_CONTABILIZACION_ANULACION']=pd.to_datetime(df_ges['PERIODO_CONTABILIZACION'],format='%Y%m', errors='coerce')+ MonthEnd(0)
```

- **Funcionalidad del código**: El código realiza transformaciones en un DataFrame `df_ges` al combinarlo con otro DataFrame y modificar algunas de sus columnas.

- El DataFrame `df_ges` se combina con el DataFrame `forma_pago` utilizando una unión a la izquierda basada en la columna `FORMA_PAGO`.
- Se crea una nueva columna `TIPO_POLIZA_LETRA` en `df_ges` que es una copia de la columna `TIPO_POLIZA`.
- La columna `TIPO_POLIZA` se actualiza para contener valores numéricos: 2 si `TIPO_POLIZA_LETRA` es 'C', y 1 en caso contrario.
- Se añade una nueva columna `BASE` al DataFrame `df_ges`, asignándole el valor constante 'GES'.
```python
# Transformaciones finales
df_ges=df_ges.merge(forma_pago,how='left',on='FORMA_PAGO')
df_ges['TIPO_POLIZA_LETRA']=df_ges['TIPO_POLIZA']
df_ges['TIPO_POLIZA']=np.where(df_ges['TIPO_POLIZA_LETRA']=='C',2,1)
df_ges['BASE']='GES'
```

- Este código calcula las fechas de renovación anual de contratos de prima recurrente.
- Utiliza una función llamada `calculo_fechas_renovacion` para determinar las fechas de inicio y fin de renovación anual, basándose en columnas específicas del DataFrame `df_ges` y un
```python
# Calculamos fechas de renovacion de los contratos de prima recurrente
df_ges['FINI_RENOV_ANUAL'],df_ges['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_ges, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo)
```

- **Funcionalidad**: El código ajusta la prima anual de un seguro de vida multiplicándola por un factor de anualización.
- Si el tipo de contrato es "Vida", se actualiza la columna 'IPRIANU' en el DataFrame `df_ges` multiplic
```python
# Anualizacion de la prima de vida GES
if tipo_contrato=='Vida':df_ges['IPRIANU']=df_ges['IPRIANU']*df_ges['FACTOR ANUALIZACION']
```

- **Funcionalidad**: El código combina dos conjuntos de datos y gestiona valores faltantes en una columna específica.

- Se realiza una combinación de datos entre `df_ges` y `planes_ges` utilizando las columnas `PRODUCTO` y `PLAN_DESC` como claves. Luego, los valores faltantes en la columna `COD_PLAN` de `df_ges` se reemplazan con 0.
```python
# traemos codigos de planes GES
df_ges=df_ges.merge(planes_ges,how='left',on=['PRODUCTO','PLAN_DESC'])
df_ges['COD_PLAN']=df_ges['COD_PLAN'].fillna(0)
```

- **Funcionalidad del código**: Filtra un DataFrame basado en ciertas condiciones relacionadas con el estado y una columna específica.

- El código realiza una combinación de dos DataFrames, añadiendo información sobre si un estado aplica o no. Luego, filtra las filas del DataFrame resultante para conservar solo aquellas donde el estado aplica. Finalmente, si existe la columna 'POLVIGENTE', elimina las filas donde su valor es 9.
```python
# Algunos estados pueden no aplicar
df_ges=df_ges.merge(estados_ges[['ESTADO','APLICA ESTADO']],how='left',on=['ESTADO'])
df_ges=df_ges[df_ges['APLICA ESTADO']==1].copy()
if 'POLVIGENTE' in df_ges.columns: df_ges=df_ges[~df_ges['POLVIGENTE'].isin([9])]
```

- **Funcionalidad del código**: Modifica un DataFrame para gestionar saldos insolutos asociados a contratos de desgravamen no licitados.

- Verifica si el contrato es de tipo "Desgravamen No Licitado".
- Asigna el valor de la columna `POLASECFI` a una nueva columna llamada `ICAPITAL` en el DataFrame `df_ges`.
- Elimina las columnas `POLCFIORI` y `POLASECFI` del DataFrame `df_ges`.
- Convierte los valores de la columna `NRO_OPERACION` a numéricos, manejando errores con un valor nulo.
- Realiza una unión (merge) del DataFrame `df_ges` con otro DataFrame llamado `saldos_insolutos_detalle` usando las columnas `POLIZA`, `RUT` y `NRO_OPERACION` como claves.
```python
# Tratamiento de saldos insolutos para el contrato de desgravamen no licitado
if contrato=='Desgravamen No Licitado':
	df_ges['ICAPITAL']=df_ges['POLASECFI']
	df_ges.drop(columns=['POLCFIORI','POLASECFI'],axis=1,inplace=True)
	df_ges['NRO_OPERACION']=pd.to_numeric(df_ges['NRO_OPERACION'],errors = 'coerce')
	df_ges=df_ges.merge(saldos_insolutos_detalle,how='left',on=['POLIZA','RUT','NRO_OPERACION'])
```

- El código combina dos bases de datos en un solo DataFrame si ambas existen, o selecciona una de ellas si solo una está disponible.

1. Verifica si ambas variables `base_iaxis` y `base_ges` son iguales a 1.
2. Si ambas son 1, concatena los DataFrames `df_iaxis` y `df_ges` verticalmente.
3. Si solo `base_iaxis` es 1, asigna `df_iaxis` a `df_0_0`.
4. Si solo `base_ges` es 1, asigna `df_ges` a `df_0_0`.
5. Si ninguna de las bases es 1, devuelve un DataFrame vacío.
6. Utiliza `pd.concat` para combinar DataFrames cuando ambas bases existen.
7. La variable `df_0_0` almacena el resultado final del proceso de combinación.
8. El código maneja la existencia de las bases de datos de manera condicional.
```python
# JUNTAMOS LAS BASES DEPENDIENDO DE CUALES EXISTEN
if (base_iaxis==1)&(base_ges==1):
	df_0_0: pd.DataFrame=pd.concat([df_iaxis,df_ges],axis=0)
elif base_iaxis==1:
	df_0_0: pd.DataFrame=df_iaxis
elif base_ges==1:
	df_0_0: pd.DataFrame=df_ges
else:
	return pd.DataFrame()
```

### Cálculos con las bases de GES unidas a iAxis

Este proceso combina las bases de datos de GES con iAxis para realizar cálculos específicos. La unión de estas bases permite analizar y extraer información relevante de manera más eficiente. Los resultados obtenidos pueden ser utilizados para mejorar la toma de decisiones en diversos contextos.

- **Funcionalidad del código**: Procesa un DataFrame para calcular variables adicionales, realizar cambios de nombre y completar datos faltantes.

1. Calcula la suma de la columna 'IPRIANU' del DataFrame `df_0_0` y escribe el resultado en un archivo de reporte.
2. Rellena los valores faltantes en la columna 'NRO_OPERACION' con ceros.
3. Elimina espacios en blanco al inicio y final de los valores en la columna 'CANAL_DESC', si existe.
4. Realiza una unión (merge) entre `df_0_0` y `cobs_ges` usando la columna 'COD_COB', añadiendo la columna 'COB_GES'.
5. Sustituye valores nulos en 'COB_GES' con los valores de 'COD_COB'.
6. Renombra varias columnas para mejorar la claridad de sus nombres.
7. Crea una nueva columna 'POL_PROD' basada en condiciones específicas de otras columnas.
8. Añade una columna 'FECHA CIERRE' con un valor constante y ajusta su tipo de dato para que coincida con 'FECHA_EFECTO'.
9. Introduce una columna 'INNOMINADA' que indica si una póliza está en una lista específica.
10. Calcula las edades y las asigna a las columnas 'EDAD' e 'ISSUE EDAD' usando una función externa.
11. Utiliza parámetros adicionales en la función de cálculo de edad para manejar casos especiales y reportar problemas.
```python
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
```

- **Funcionalidad del código**: Calcula la edad de ingreso de individuos y reporta cualquier problema encontrado durante el cálculo.

- Verifica si la columna 'FEC_NAC' está presente en el DataFrame `df_0_1`.
- Si la columna existe, escribe un mensaje en un archivo de reporte indicando que se está calculando la edad de ingreso.
- Utiliza la función `calcula_edad` para calcular la edad de ingreso y detectar problemas, almacenando los resultados en las columnas 'EDAD INGRESO' e 'ISSUE EDAD INGR' del DataFrame.
```python
# Calculo edad de ingreso
if 'FEC_NAC' in df_0_1.columns:
	escribe_reporta(archivo_reporte,'Calculando edad de ingreso')
	df_0_1['EDAD INGRESO'],df_0_1['ISSUE EDAD INGR']=calcula_edad(df_0_1['RUT'],df_0_1['FEC_NAC'],df_0_1['FECHA_EFECTO'],edad_casos_perdidos,108,archivo_reporte,reporta_issues=1, edad_inf = 18, aplica_edad_prom_cartera = 1)
```

- El código calcula fechas de inicio y fin de exposición para contratos, diferenciando entre tipos específicos de contratos.

1. **Condicional**: Verifica si el contrato no es de tipo 'Cesantia PU' ni 'K-Fijo'.
2. **Cálculo de Fechas**: Para contratos que no son 'Cesantia PU' ni 'K-Fijo', calcula fechas de renovación anual usando una función específica.
3. **Fecha de Fin de Exposición**: Determina la fecha de fin de exposición basándose en la fecha de anulación o vencimiento.
4. **Inicialización de Fecha Auxiliar**: Para contratos 'Cesantia PU' o 'K-Fijo', inicializa una columna auxiliar de fecha con ceros.
5. **Conversión de Fecha**: Convierte la columna auxiliar a un formato de fecha, manejando errores.
6. **Ajuste de Fecha de Anulación**: Actualiza la fecha de anulación si es posterior a una fecha de cierre específica.
7. **Uso de Librerías**: Utiliza las librerías `numpy` y `pandas` para operaciones de manejo de datos y fechas.
```python
# CALCULOS ESPECIFICOS POR CADA CONTRATO
# CALCULOS DE FECHAS DE INICIO/FIN DE EXPOSICION: SE DIFERENCIAN ENTRE PRIMA UNICA (CESANTIA Y K-FIJO) DEL RESTO
if (clasificacion_contrato !='Cesantia PU')&(contrato!='K-Fijo'):
	df_0_1['FINI_RENOV_ANUAL'],df_0_1['FFIN_RENOV_ANUAL']=calculo_fechas_renovacion(df_0_1, 'FECHA_EFECTO', 'FECHA_VENCIMIENTO', 'FECHA_ANULACION','FORMA_PAGO_CODIGO', periodo,0)
	df_0_1['FECHA FIN EXP']=np.where(~df_0_1['FECHA_ANULACION'].isnull(),df_0_1['FECHA_ANULACION'],np.where(df_0_1['FECHA_VENCIMIENTO'].isnull(),df_0_1['FFIN_RENOV_ANUAL'],df_0_1['FECHA_VENCIMIENTO']))
else:
	df_0_1['FEC AUX NA']=0
	df_0_1['FEC AUX NA']=pd.to_datetime(df_0_1['FEC AUX NA'],format = '%d-%m-%Y', errors='coerce')
	df_0_1['FECHA_ANULACION']=np.where(df_0_1['FECHA_ANULACION']<=fecha_cierre,df_0_1['FECHA_ANULACION'],df_0_1['FEC AUX NA'])
```

### Cálculos Genéricos para Bases de Vida Prima Recurrente

Este código realiza cálculos relacionados con seguros de vida que tienen pagos recurrentes. Utiliza fórmulas actuariales para determinar valores como primas, beneficios y otros factores financieros. Está diseñado para facilitar la gestión y análisis de pólizas de seguro de vida con pagos periódicos.

- **Funcionalidad del código**: Procesa datos de contratos de seguros de vida para calcular exposiciones mensuales, tipos de asegurados, edades de renovación y montos asegurados, integrando información de diferentes tablas.

1. Verifica si el tipo de contrato es "Vida" y no es "K-Fijo".
2. Carga datos de las hojas de Excel "Meses Renta" y "Saldo Insoluto" en dos DataFrames.
3. Calcula la exposición mensual para cada registro usando fechas específicas y actualiza el DataFrame principal.
4. Determina el tipo de asegurado como "Titular" o "Adicional" basado en la información del RUT.
5. Escribe un mensaje en un archivo de reporte indicando que se está calculando la edad de renovación.
6. Calcula la edad de renovación y posibles problemas de edad, ajustando según el tipo de contrato.
7. Fusiona el DataFrame principal con los datos de "Meses Renta" usando una clave común.
8. Calcula el monto asegurado, aplicando una fórmula específica si el contrato es "Desgravamen No Licitado".
9. Fusiona el DataFrame resultante con los datos de "Saldo Insoluto" usando múltiples claves comunes.
10. Rellena valores nulos en la columna "APLICA CALCULO SALDO INSOLUTO" con ceros.
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

###### Cálculo de Productos con Capital como Saldo Insoluto

Este código se encargará de calcular productos financieros donde el capital se considera como el saldo insoluto. El saldo insoluto es el monto pendiente de pago en un préstamo o crédito. Este cálculo es crucial para determinar los pagos futuros y el interés acumulado.

- **Funcionalidad del código**: Filtra y procesa un conjunto de datos financieros para calcular montos asegurados basados en condiciones específicas.

1. Verifica si el contrato es "Desgravamen No Licitado".
2. Filtra los datos en dos subconjuntos según si aplican cálculo de saldo insoluto.
3. Calcula la fecha de fin de crédito para un subconjunto basado en condiciones específicas.
4. Determina el número total de cuotas y las cuotas faltantes en meses.
5. Calcula el periodo de efecto en formato año-mes.
6. Completa el campo de tasa de crédito usando una función externa con parámetros dados.
7. Calcula el saldo insoluto basado en la tasa de crédito y el número de cuotas.
8. Determina el monto asegurado comparando el saldo insoluto existente y el calculado.
9. Combina los subconjuntos de datos procesados y no procesados en un solo DataFrame.
10. Restablece el índice del DataFrame combinado para mantener la continuidad de los datos.
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

- **Funcionalidad del código**: Filtra, agrupa y combina datos de un DataFrame para corregir problemas específicos relacionados con un producto.

1. Filtra un DataFrame original para seleccionar filas donde el producto es 331, la base es 'IAXIS', y el código de cobertura es 12.
2. Crea una copia de este subconjunto filtrado para trabajar con él sin modificar el original.
3. Separa el resto de los datos que no cumplen con las condiciones anteriores en otro DataFrame.
4. Agrupa el subconjunto filtrado por una columna específica y suma los valores de otra columna para corregir duplicados.
5. Filtra nuevamente el subconjunto para mantener solo las filas con un código de cobertura específico.
6. Combina el subconjunto corregido con el resto de los datos y restablece el índice del DataFrame resultante.
```python
# ! Solicitud area de productos: producto 331 se encuentra con coberturas duplicados y problemas de capitales mal asignados
df_331 = df_0_5[(df_0_5['PRODUCTO']==331)&(df_0_5['BASE']=='IAXIS')&(df_0_5['CODIGO COBERTURA']==12)].copy()
df_resto = df_0_5[~df_0_5.index.isin(df_331.index)].copy()
df_331['PRIMA NETA ANUAL'] = df_331.groupby('SSEGURO')['PRIMA NETA ANUAL'].transform('sum')
df_331 = df_331[df_331['CODIGO COBERTURA IAXIS']==1200].copy()
df_0_6 = pd.concat([df_resto,df_331]).reset_index(drop=True)
df_final_0=df_0_6.copy()
```

- **Funcionalidad**: El código copia el contenido de un DataFrame a otro si no se cumple una condición previa.
- Si la condición previa no se cumple, se crea una copia del DataFrame `df_0_3` y se asigna a `df_final_0`.
```python
else: df_final_0=df_0_3.copy()
```

#### Tratamiento de Saldo Insoluto para Multisocios

Este proceso se encarga de gestionar el saldo pendiente de pago para cuentas con múltiples socios. Se asegura de que cada socio reciba la información correcta sobre el saldo que aún no ha sido liquidado. Esto es crucial para mantener la transparencia y precisión en las finanzas compartidas.

- **Funcionalidad del código**: Calcula y actualiza varios campos financieros en un DataFrame basado en condiciones específicas para contratos de tipo 'Multisocios'.

1. Verifica si el contrato es de tipo 'Multisocios' para proceder con las operaciones.
2. Actualiza la fecha de finalización del crédito dependiendo de la base de datos, eligiendo la fecha máxima entre la fecha de vencimiento y la fecha de fin de crédito.
3. Calcula el número total de cuotas del crédito en meses, basándose en la diferencia entre la fecha de fin de crédito y la fecha de efecto.
4. Calcula el número de cuotas faltantes hasta una fecha de cierre específica.
5. Determina el período de efecto en formato YYYYMM a partir de la fecha de efecto.
6. Completa el campo de tasa de crédito utilizando una función externa, basada en combinaciones de producto y período de efecto.
7. Calcula el saldo insoluto del crédito, considerando si la fecha de fin de crédito es anterior a la fecha de cierre.
8. Establece el monto asegurado como el máximo entre el saldo insoluto calculado y cero.
9. Crea una copia del DataFrame actualizado para su uso posterior.
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

Este código realiza cálculos específicos relacionados con un valor constante denominado **K-Fijo**. Se utiliza para resolver problemas matemáticos o de ingeniería donde **K** es un parámetro fijo que influye en los resultados. La estructura del código está diseñada para manejar operaciones que dependen de este valor constante.

- **Funcionalidad del código**: Calcula y ajusta datos relacionados con contratos de seguro de vida, específicamente para contratos tipo "K-Fijo".

1. Verifica si el tipo de contrato es "Vida" y el contrato es "K-Fijo".
2. Asigna un valor de exposición mensual de 1 a los registros relevantes.
3. Llama a una función para calcular la edad de renovación y la edad de emisión, almacenando los resultados en nuevas columnas.
4. Calcula el plazo en meses entre la fecha de vencimiento y la fecha de efecto, asegurando que sea al menos 1.
5. Asigna el valor del capital asegurado a una nueva columna llamada "MONTO ASEGURADO".
6. Filtra los registros cuya fecha de efecto es anterior o igual a una fecha de cierre específica.
7. Crea una copia de los datos filtrados para su uso posterior.
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

Este proceso identifica y extrae datos relacionados con edades que presentan inconsistencias o errores. La información se prepara para ser revisada o corregida posteriormente. El objetivo es asegurar la calidad y precisión de los datos relacionados con edades.

- **Funcionalidad del código**: Exporta a archivos CSV las filas de un DataFrame que tienen problemas con las edades de ingreso o renovación, y escribe un reporte sobre la prima neta anual.

1. Verifica si el DataFrame contiene una columna llamada 'ISSUE EDAD INGR'.
2. Si la columna existe y tiene valores positivos, exporta las filas correspondientes a un archivo CSV para revisión.
3. Realiza un proceso similar para la columna 'ISSUE EDAD RENOV', exportando las filas problemáticas a otro archivo CSV.
4. Calcula la suma de la columna 'PRIMA NETA ANUAL' del DataFrame, ignorando valores nulos.
5. Escribe un reporte que incluye la suma calculada de la prima neta anual.
```python
# EXPORTO EDADES CON ISSUES
if 'ISSUE EDAD INGR' in df_final_0.columns:
	if sum(df_final_0['ISSUE EDAD INGR'])>0: df_final_0[df_final_0['ISSUE EDAD INGR']==1].to_csv(ruta_output+'0. Edades de Ingreso a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
if 'ISSUE EDAD RENOV' in df_final_0.columns:
	if sum(df_final_0['ISSUE EDAD RENOV'])>0: df_final_0[df_final_0['ISSUE EDAD RENOV']==1].to_csv(ruta_output+'0. Edades de Renovacion a Revisar.csv',sep=separador_output,decimal=decimal_output,date_format='%d-%m-%Y',index=False)
escribe_reporta(archivo_reporte,'El dataframe input luego de ser pre-procesado posee una prima neta de {}'.format(np.nansum(df_final_0['PRIMA NETA ANUAL'])))
```

## Últimos Filtros

Este bloque de código aplica los filtros finales a un conjunto de datos. Se asegura de que solo los datos que cumplen con ciertos criterios específicos sean seleccionados para su uso posterior. Esto es crucial para mantener la integridad y relevancia de la información procesada.

- **Funcionalidad del código**: Filtra un DataFrame para incluir solo las filas donde la exposición mensual es mayor que cero.
- Crea una copia del DataFrame original, `df_final_0`, seleccionando únicamente las filas con valores positivos en la columna 'EXPOSICION
```python
# Trabajamos con el df_final_0 y hacemos todas las operaciones que debamos hacer en general
df_final_1=df_final_0[df_final_0['EXPOSICION MENSUAL']>0].copy()
```

- **Funcionalidad del código**: Filtra un conjunto de datos para obtener registros de asegurados que están vigentes al final del mes.

- Se filtran los registros donde la fecha de vencimiento es posterior o igual a una fecha de cierre específica, o donde la fecha de vencimiento no está definida.
- Luego, se aplica un segundo filtro para incluir solo los registros donde la fecha de anulación es posterior o igual a la misma fecha de cierre, o donde la fecha de anulación no está definida.
- Finalmente, se devuelve el conjunto de datos resultante que cumple con ambos criterios de vigencia.
```python
# ! Solicitud Productos: Trabajar con los asegurados que estan vigentes a fin de mes
df_final_2=df_final_1[(df_final_1['FECHA_VENCIMIENTO']>=fecha_cierre)|(df_final_1['FECHA_VENCIMIENTO'].isnull())]
df_final_3=df_final_2[(df_final_2['FECHA_ANULACION']>=fecha_cierre)|(df_final_2['FECHA_ANULACION'].isnull())]
return df_final_3
```