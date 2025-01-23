# Modulo S4_Calculos_Renovacion

Modulo para calcular la renovación de reaseguro para un contrato específico.

La instrucción `warnings.simplefilter(action='ignore', category=UserWarning)` **desactiva las advertencias** de tipo `UserWarning`, evitando que se muestren en la pantalla. Esto es útil para mantener la salida del programa más limpia, como se menciona en el comentario: *"Quitamos los warning de la pantalla"*.
```python
warnings.simplefilter(action='ignore', category=UserWarning)
```

# Funcion `calculos_renovacion`

La función `calculos_renovacion` se encarga de realizar cálculos relacionados con la **renovación de reaseguro** para un contrato específico. 

### Parámetros:
- **parameters**: Incluye todos los parámetros necesarios para la ejecución.
- **tables**: Contiene las tablas de parámetros que se utilizan en el proceso.
- **ruta_salidas**: Especifica la ruta donde se guardarán los resultados principales de los cálculos.

## Importación de Parámetros y Tablas

Se obtendrán los **parámetros** y **tablas** necesarios para el funcionamiento del código. Esto permitirá que las siguientes operaciones se realicen de manera efectiva y organizada.

La instrucción `contrato: str = parameters.parameters['contrato']` asigna el valor del contrato de reaseguro a la variable `contrato`. Este valor se obtiene de un conjunto de parámetros. 

Es importante destacar que el tipo de la variable es `str`, lo que indica que se espera que el valor sea una cadena de texto.
```python
contrato: str = parameters.parameters['contrato']
```

La instrucción `nombre_prods: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Nombre Productos Renovacion')` **carga una tabla desde un archivo Excel**. 

El comentario indica que esta tabla se utiliza como una **matriz para asignar el nombre del producto**. Esto sugiere que los datos de esta tabla son importantes para identificar productos en un proceso posterior.
```python
nombre_prods: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Nombre Productos Renovacion')
```

Se crean dos **matrices** (o **dataframes**) a partir de datos de un archivo Excel. 

- `ramo_reas_otros` contiene información sobre el **contrato de reaseguro de Desgravamen No Licitado**.
- `ramo_reas_desgnl` almacena datos para **otros contratos**.

Estos datos son necesarios para el área de productos.
```python
ramo_reas_otros: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Ramo Reas Otros')
ramo_reas_desgnl: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Ramo Reas Desg NL')
```

## Cálculos Previos para el Contrato de Reaseguro

Se realizarán una serie de **cálculos previos** antes de asignar un contrato de reaseguro. Estos cálculos son fundamentales para asegurar que el proceso se lleve a cabo de manera eficiente y efectiva.

La instrucción `df: pd.DataFrame = pre_procesamiento(parameters, tables)` **realiza un preprocesamiento de datos**. Toma parámetros y tablas como entrada y aplica transformaciones necesarias a la información extraída de sistemas de administración de bases de datos. El resultado es un **DataFrame** que se utilizará para asignar contratos de reaseguro y realizar cálculos.
```python
df: pd.DataFrame = pre_procesamiento(parameters, tables)
```

El código `df = identificador_anonimo(df, ['RUT'])` se encarga de **anonimizar** el campo **RUT** en el DataFrame `df`. Esto se realiza a solicitud del área de productos, con el objetivo de proteger la **privacidad** de los datos.
```python
df = identificador_anonimo(df, ['RUT'])
```

El código realiza las siguientes acciones:

1. **Cuenta y muestra** la cantidad de registros que serán eliminados, específicamente aquellos que pertenecen a ciertos productos relacionados con "Hospitalario 100%" y "fallecimiento COVID".
   
2. **Filtra el DataFrame** `df` para eliminar los registros de esos productos, manteniendo solo aquellos que no están en la lista especificada. 

Esto responde a una solicitud del área de productos para eliminar información sensible.
```python
print(f'Se eliminarán {sum(df["PRODUCTO"].isin([88,101,193,369,370,371,372]))} registros que pertenecen a los productos de Hospitalario 100% y productos fallecimiento COVID')
df = df[~df['PRODUCTO'].isin([88,101,193,369,370,371,372])].copy()
```

Se añade una nueva columna llamada **REGISTROS** al DataFrame `df`, y se le asigna el valor **1** a cada fila. Esto se utiliza para contar la cantidad de registros más adelante.
```python
df['REGISTROS']=1
```

La instrucción `df = recargos(df, parameters, calcula_recargos=0)` se encarga de **identificar los registros** que tienen **recargos de sobreprima o extraprima** en el conjunto de datos `df`. 

El parámetro `calcula_recargos=0` sugiere que no se están calculando nuevos recargos en este momento.
```python
df = recargos(df,parameters,calcula_recargos=0)
```

## Cálculos de Asignación de Contratos de Reaseguro

Este código se encarga de realizar **cálculos** relacionados con la asignación de contratos de **reaseguro**. Su objetivo es facilitar la gestión y distribución de riesgos entre las partes involucradas, asegurando que se tomen decisiones informadas y eficientes en el proceso.

La instrucción `df = asignacion_contratos(df, parameters, tables, mantiene_na = 1)` **asigna contratos de reaseguro** a los registros en el DataFrame `df`. Utiliza los parámetros y tablas proporcionados, y el argumento `mantiene_na = 1` sugiere que se mantendrán ciertos valores nulos durante el proceso.
```python
df = asignacion_contratos(df, parameters, tables, mantiene_na = 1)
```

La instrucción `asignacion_vigencias(df, parameters, tables, mantiene_na = 1)` asigna la **vigencia del contrato** a los registros en el DataFrame `df`. 

El resultado se divide en dos partes:
- `df`: contiene los registros actualizados con la vigencia asignada.
- `df_deleted_vigencia`: almacena los registros que no tienen vigencia.

El parámetro `mantiene_na = 1` sugiere que se
```python
df,df_deleted_vigencia = asignacion_vigencias(df, parameters, tables, mantiene_na = 1)
```

## Cálculos de Cúmulos Asociados a los Contratos

Este segmento de código se encarga de realizar **cálculos** relacionados con los **cúmulos** que están vinculados a diferentes **contratos**. Estos cálculos son esenciales para entender y gestionar mejor los datos asociados a cada contrato.

La instrucción `df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO LIMITE INDIVIDUAL')` realiza un **cálculo de acumulados** sobre el monto asegurado de cada persona. 

El parámetro `campo_cumulo` especifica que se está trabajando con el **riesgo límite individual**. Esto permite analizar la información de manera más detallada y organizada.
```python
df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO LIMITE INDIVIDUAL')
```

La instrucción `df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO LIMITE CONTRATO')` **aplica una función** llamada `cumulos` sobre un conjunto de datos `df`. Esta función utiliza ciertos **parámetros** y **tablas** para calcular un **cúmulo** relacionado con el **monto asegurado** de un contrato. El campo específico que se considera para este cálculo es `'RIESGO LIMITE CONTRATO
```python
df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO LIMITE CONTRATO')
```

La instrucción `df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO RETENCION EXCEDENTE')` realiza un **cálculo de acumulados** sobre el monto asegurado en contratos de excedente. 

El resultado se almacena en `df`, que es un **DataFrame**. El parámetro `campo_cumulo` especifica el campo relacionado con el riesgo de retención excedente.
```python
df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO RETENCION EXCEDENTE')
```

## Cálculos de Capitales Cedidos y Retenidos

Este segmento de código se encarga de calcular los montos de capital que han sido cedidos y aquellos que se han retenido. Estos cálculos son esenciales para entender la distribución y el manejo de los recursos financieros en un contexto específico.
```python
df['CAPITAL CEDIDO POST EXCEDENTE'] = df['CAPITAL POST LIMITE CONTRATO'] * (1-df['PORCENTAJE RETENCION EXCEDENTE'])
df['CAPITAL RETENIDO POST QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] * (1-np.where(df['CESION QS'].isnull(),0,df['CESION QS']))
df['CAPITAL CEDIDO QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] - df['CAPITAL RETENIDO POST QS']
df['CAPITAL RETENIDO TOTAL'] = df['MONTO ASEGURADO']-(df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS'])
df['CAPITAL CEDIDO TOTAL'] = df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS']
df['PORCENTAJE CEDIDO FINAL']=np.where(df['MONTO ASEGURADO']>0,df['CAPITAL CEDIDO TOTAL']/df['MONTO ASEGURADO'],df['CESION QS']*df['PORCENTAJE LIMITE INDIVIDUAL']*df['PORCENTAJE LIMITE CONTRATO']*df['PORCENTAJE RETENCION EXCEDENTE'])
```

El código realiza una verificación del tipo de contrato. Dependiendo del tipo, se ejecuta una función llamada `cruce_left` con diferentes parámetros.

- Si el contrato es **"Desgravamen No Licitado"**, se utiliza el conjunto de datos `ramo_reas_desgnl`.
- Si el contrato es uno de los siguientes: **"Digital Klare", "K-Fijo", "AP + Urgencias Medicas"** o **"Multisocios"**,
```python
if contrato=='Desgravamen No Licitado':
df = cruce_left(df, ramo_reas_desgnl, ['COBERTURA DEL CONTRATO'], ['COBERTURA DEL CONTRATO'],parameters,name='ramo_reas_desgnl')
elif contrato in ['Digital Klare','K-Fijo','AP + Urgencias Medicas','Multisocios']:
df = cruce_left(df, ramo_reas_otros, ['POL_PROD','CODIGO COBERTURA'], ['POL_PROD','CODIGO COBERTURA'],parameters,name='ramo_reas_otros')
```

El código asigna un nombre a un producto en un DataFrame llamado `df`. Utiliza la función `cruce_left` para combinar datos basándose en las columnas `PRODUCTO` y `BASE`, utilizando parámetros específicos. Esto permite organizar y etiquetar la información de manera más clara.
```python
df = cruce_left(df, nombre_prods, ['PRODUCTO','BASE'], ['PRODUCTO','BASE'],parameters,name='nombre_prods')
```

La instrucción `if contrato == 'K-Fijo':` verifica si el valor de la variable `contrato` es igual a `'K-Fijo'`. 

Si es así, se ejecutan las acciones necesarias para **crear campos adicionales** que son necesarios, asegurando que la base de datos sea **uniforme para todos los contratos**.
```python
if contrato == 'K-Fijo':
```

La instrucción `df['MESES RENTA'] = 1` **asigna el valor 1** a una nueva columna llamada **"MESES RENTA"** en el DataFrame `df`. Esto significa que cada fila de esta columna tendrá el valor **1**. 

El comentario indica que se está **creando** esta columna para representar **meses de renta**.
```python
df['MESES RENTA'] = 1
```

Se asigna el valor **'Titular'** a la columna **'TIPO ASEGURADO'** del DataFrame **df**. Esto indica que todos los registros en esa columna serán clasificados como **Titular**.
```python
df['TIPO ASEGURADO'] = 'Titular'
```

Se definen dos listas en Python:

1. **`campos_productos`**: Contiene campos relacionados con productos, como fechas, identificadores y detalles de pólizas.

2. **`campos_renovacion`**: Incluye campos específicos para la renovación de pólizas, con información similar pero más enfocada.

Ambas listas son **selecciones de campos** para uso interno y para compartir con reaseguradores.
```python
campos_productos = ['FECHA_CIERRE','BASE','IDENTIFICADOR','RUT','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','NOMBRE_PRODUCTO','PLAN','CODIGO_COBERTURA','CODIGO_COBERTURA_IAXIS','CONTRATO_REASEGURO','COBERTURA_DEL_CONTRATO','RAMO_REAS','COB_REAS','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FEC_NAC','EDAD','SEXO','TIPO_POLIZA','FORMA_PAGO_CODIGO','MESES_RENTA','INNOMINADA','PRIMA_NETA_ANUAL','ICAPITAL','MONTO_ASEGURADO','CAPITAL_RETENIDO_TOTAL','CAPITAL_CEDIDO_TOTAL','RECARGO']
campos_renovacion = ['FECHA_CIERRE','BASE','IDENTIFICADOR','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO_COBERTURA','RAMO_REAS','COB_REAS','TIPO_POLIZA','FECHA_EFECTO','FECHA_VENCIMIENTO','FEC_NAC','EDAD','SEXO','MONTO_ASEGURADO']
```

## Reportería

Este segmento de código se encargará de generar informes y análisis sobre los datos procesados. Su objetivo es presentar la información de manera clara y comprensible, facilitando la toma de decisiones.

El código transforma los nombres de las columnas de un DataFrame, reemplazando los espacios por guiones bajos. 

- **`cols_new = []`**: Crea una lista vacía para almacenar los nuevos nombres de las columnas.
- **`for col in df.columns:`**: Itera sobre cada nombre de columna en el DataFrame.
- **`cols_new.append(col.replace(' ','_'))`**: Reemplaza los espacios en el nombre de la columna por gu
```python
cols_new = []
for col in df.columns:
cols_new.append(col.replace(' ','_'))
df.columns = cols_new
```

El código exporta datos filtrados a archivos CSV comprimidos en formato ZIP. 

- **Primera línea**: Exporta información de uso interno relacionada con contratos de reaseguro.
- **Segunda línea**: Exporta información específica para reaseguradores.

Ambas exportaciones utilizan un separador `;`, un formato decimal `.` y un formato de fecha específico.
```python
df[df['CONTRATO_REASEGURO'].notnull()][campos_productos].to_csv(ruta_salidas+f'Detalle Renovacion {contrato} Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
df[df['CONTRATO_REASEGURO'].notnull()][campos_renovacion].to_csv(ruta_salidas+f'Detalle Renovacion {contrato} Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
```

El código realiza las siguientes acciones:

1. **Crea un directorio**: Se establece una ruta para guardar resultados y se crea el directorio si no existe.
   
2. **Carga archivos de parámetros**: Se cargan varios archivos de Excel que contienen información necesaria para el proceso.

3. **Carga de parámetros**: Se obtienen y cargan parámetros específicos desde uno de los archivos.

4. **Realiza cálculos**: Se ejecuta una función que realiza cálculos
```python
if __name__=='__main__':
ruta_salidas='2 Output\\Resultados 2024-12-20\\'
Path(ruta_salidas).mkdir(parents=True, exist_ok=True)
files: Parameter_Loader = Parameter_Loader(excel_file='Inputs Archivos Excel.xlsx', open_wb=True, ruta_extensa='')
files.get_reference(reference='archivo_calculos')
files.get_reference(reference='archivo_querys')
files.get_reference(reference='archivo_parametros')
parameters: Parameter_Loader = Parameter_Loader(excel_file=files.parameters['archivo_calculos'], open_wb=True)
carga_parametros(files, parameters)
tables: Parameter_Loader = files.parameters['archivo_parametros']
calculos_renovacion(parameters, tables, ruta_salidas)
```