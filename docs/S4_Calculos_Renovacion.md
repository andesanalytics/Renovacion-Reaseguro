# Modulo S4_Calculos_Renovacion

Modulo para calcular la renovación de reaseguro para un contrato específico.


- **Funcionalidad**: El código suprime la visualización de advertencias de usuario en la salida del programa.
- Utiliza la función `simplefilter` del módulo `warnings` para ignorar las advertencias de tipo `UserWarning`.
```python
# Quitamos los warning de la pantalla
warnings.simplefilter(action='ignore', category=UserWarning)
```

# Funcion `calculos_renovacion`

La función `calculos_renovacion` se encarga de realizar cálculos relacionados con la renovación de un contrato de reaseguro específico. 

### Parámetros de entrada:

- **`parameters`**: Un objeto de tipo `Parameter_Loader` que incluye todos los parámetros necesarios para la ejecución de los cálculos.
- **`tables`**: Otro objeto de tipo `Parameter_Loader` que contiene las tablas de parámetros utilizadas durante el proceso de cálculo.
- **`ruta_salidas`**: Una cadena de texto que especifica la ruta donde se almacenarán los resultados principales de los cálculos realizados.

### Salida:

- La función no devuelve ningún valor, ya que su propósito es realizar los cálculos y guardar los resultados en la ubicación especificada por `ruta_salidas`.

## Importación de Parámetros y Tablas

Este paso implica la **carga de datos** y **configuraciones** necesarias para el funcionamiento del programa. Se obtienen parámetros específicos y tablas de datos que serán utilizados en las operaciones posteriores. Esto asegura que el código tenga acceso a la información requerida para procesar y analizar los datos correctamente.

- **Funcionalidad del código**: Carga datos de un archivo Excel en diferentes estructuras de datos para su uso en la gestión de contratos de reaseguro.

- Define una variable para almacenar el tipo de contrato de reaseguro desde un conjunto de parámetros.
- Carga una tabla de Excel que contiene los nombres de productos de renovación en un DataFrame de pandas.
- Carga dos tablas de Excel en DataFrames separados para asignar campos específicos relacionados con contratos de reaseguro, diferenciando entre contratos generales y un contrato específico de Desgravamen No Licitado.
```python
# ? Tablas
# Contrato de reaseguro
contrato: str = parameters.parameters['contrato']
# Matriz para asignar el nombre del producto
nombre_prods: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Nombre Productos Renovacion')
# Matrices para asignar los campos RAMO_REAS y COB_REAS
# ! Estos campos son solicitados por el área de productos y son dos matrices porque una es para el contrato de reaseguro de Desgravamen No Licitado, mientras que la otra matriz es para el resto de contratos
ramo_reas_otros: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Ramo Reas Otros')
ramo_reas_desgnl: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Ramo Reas Desg NL')
```

#### Cálculos Previos del Proceso

Antes de asignar un contrato de reaseguro, se realizan cálculos necesarios para evaluar los riesgos y determinar las condiciones del contrato. Estos cálculos aseguran que el contrato sea adecuado para las necesidades de cobertura y que los términos sean justos tanto para el asegurador como para el reasegurador.

- **Funcionalidad**: El código realiza transformaciones en los datos extraídos de sistemas de administración de bases de datos antes de asignar contratos de reaseguro y realizar cálculos.
- **Explicación**: Se utiliza una función llamada `pre_procesamiento` que toma
```python
# Preprocesamiento de la Data: A las querys que se extraen de los sistemas de administracion de BBDD (GES e Iaxis) se le realizan ciertas transformaciones a la data antes de asignar contratos de reaseguro y realizar los calculos
df: pd.DataFrame = pre_procesamiento(parameters, tables)
```

- **Funcionalidad del código**: El código procesa un DataFrame para anonimizar datos, eliminar ciertos productos y calcular recargos.

1. Anonimiza el campo 'RUT' en el DataFrame utilizando una función específica para proteger la identidad de los individuos.
2. Calcula y muestra la cantidad de registros que serán eliminados, correspondientes a productos específicos relacionados con hospitalización y fallecimiento por COVID.
3. Filtra el DataFrame para eliminar los registros de los productos especificados.
4. Añade una nueva columna al DataFrame para contar el número de registros, asignando un valor de 1 a cada fila.
5. Identifica y procesa los registros que tienen recargos de sobreprima o extraprima mediante una función que aplica cálculos específicos.
```python
# ? Calculos solicitados por el área de productos
# ! Solicitado por el área de productos, anonimizamos el campo RUT
df = identificador_anonimo(df, ['RUT'])
#  ! Solicitado por el área de productos, eliminamos los productos de Hospitalario 100% y productos fallecimiento COVID'
print(f'Se eliminarán {sum(df["PRODUCTO"].isin([88,101,193,369,370,371,372]))} registros que pertenecen a los productos de Hospitalario 100% y productos fallecimiento COVID')
df = df[~df['PRODUCTO'].isin([88,101,193,369,370,371,372])].copy()
# Campo para luego contar la cantidad de registros
df['REGISTROS']=1
# Identificamos los registros que poseen recargos de sobreprima o extraprima
df = recargos(df,parameters,calcula_recargos=0)
```

## Cálculos de Asignación de Contratos de Reaseguro

Este proceso se encarga de determinar cómo se distribuyen los contratos de reaseguro entre diferentes partes. Utiliza datos específicos para calcular las proporciones y asignar responsabilidades financieras. El objetivo es optimizar la cobertura de riesgos y asegurar una distribución equitativa de las obligaciones.

- **Funcionalidad del código**: Asigna contratos de reaseguro a un conjunto de registros en un DataFrame.
- Utiliza una función llamada `asignacion_contratos` que toma un DataFrame y otros parámetros para realizar la asignación, permitiendo mantener valores n
```python
# Asignamos contrato de reaseguro a los registros
df = asignacion_contratos(df, parameters, tables, mantiene_na = 1)
```

- **Funcionalidad del código**: Asigna la vigencia del contrato a los registros de un DataFrame y separa aquellos que no cumplen con ciertos criterios.

- Utiliza una función llamada `asignacion_vigencias` que toma un DataFrame y otros parámetros para asignar vig
```python
# Asignamos vigencia del contrato a la que pertenecen los registros
df,df_deleted_vigencia = asignacion_vigencias(df, parameters, tables, mantiene_na = 1)
```

## Cálculos de Cúmulos Asociados a los Contratos

Este código se encarga de realizar cálculos relacionados con los cúmulos de datos que están vinculados a contratos específicos. Los cúmulos pueden referirse a agrupaciones de datos o valores que necesitan ser procesados para obtener información relevante sobre los contratos. El objetivo es analizar y manipular estos datos para facilitar la toma de decisiones o generar reportes.

- **Funcionalidad del código**: Calcula el cúmulo sobre el monto asegurado para cada persona individualmente utilizando un DataFrame y parámetros específicos.

- Utiliza una función llamada `cumulos` que toma un DataFrame y varios parámetros para calcular y agregar información sobre el riesgo límite
```python
# Cumulo sobre el monto asegurado que proviene de cada persona individualmente
df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO LIMITE INDIVIDUAL')
```

- **Funcionalidad del código**: Calcula el cúmulo sobre el monto asegurado basado en el límite de riesgo del contrato.

- Utiliza una función llamada `cumulos` para procesar un DataFrame `df` con parámetros y tablas adicionales, especificando el campo de cú
```python
# Cumulo sobre el monto asegurado que proviene del contrato en su conjunto
df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO LIMITE CONTRATO')
```

- **Funcionalidad del código**: Aplica un cúmulo sobre el monto asegurado para contratos de excedente en un DataFrame.
- Utiliza la función `cumulos` para modificar el DataFrame `df` con parámetros y tablas dados, enfocándose en el campo específico '
```python
# Cumulo sobre el monto asegurado que aplica sobre contratos de excedente
df = cumulos(df, parameters, tables, campo_cumulo = 'RIESGO RETENCION EXCEDENTE')
```

## Cálculos de Capitales Cedidos y Retenidos

Este código se encarga de calcular los montos de capital que una entidad decide ceder a otra y los que retiene para sí misma. Utiliza fórmulas específicas para determinar estos valores basándose en ciertos criterios o condiciones predefinidas. Los resultados ayudan a entender cómo se distribuyen los recursos financieros entre diferentes partes.

- **Funcionalidad del código**: Calcula varios valores relacionados con la cesión y retención de capital en un DataFrame, basándose en diferentes porcentajes y condiciones.

1. Calcula el capital cedido después de aplicar un porcentaje de retención excedente al capital post límite de contrato.
2. Determina el capital retenido post QS ajustando el capital retenido post excedente según la cesión QS, si está presente.
3. Calcula el capital cedido QS como la diferencia entre el capital retenido post excedente y el capital retenido post QS.
4. Establece el capital retenido total restando del monto asegurado la suma del capital cedido post excedente y el capital cedido QS.
5. Calcula el capital cedido total sumando el capital cedido post excedente y el capital cedido QS.
6. Define el porcentaje cedido final, que depende del monto asegurado y otros factores, utilizando una condición para manejar casos donde el monto asegurado es mayor que cero.
```python
df['CAPITAL CEDIDO POST EXCEDENTE'] = df['CAPITAL POST LIMITE CONTRATO'] * (1-df['PORCENTAJE RETENCION EXCEDENTE'])
df['CAPITAL RETENIDO POST QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] * (1-np.where(df['CESION QS'].isnull(),0,df['CESION QS']))
df['CAPITAL CEDIDO QS'] = df['CAPITAL RETENIDO POST EXCEDENTE'] - df['CAPITAL RETENIDO POST QS']
df['CAPITAL RETENIDO TOTAL'] = df['MONTO ASEGURADO']-(df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS'])
df['CAPITAL CEDIDO TOTAL'] = df['CAPITAL CEDIDO POST EXCEDENTE'] + df['CAPITAL CEDIDO QS']
df['PORCENTAJE CEDIDO FINAL']=np.where(df['MONTO ASEGURADO']>0,df['CAPITAL CEDIDO TOTAL']/df['MONTO ASEGURADO'],df['CESION QS']*df['PORCENTAJE LIMITE INDIVIDUAL']*df['PORCENTAJE LIMITE CONTRATO']*df['PORCENTAJE RETENCION EXCEDENTE'])
```

- **Funcionalidad del código**: Realiza cruces de datos y ajustes en un DataFrame según el tipo de contrato para uniformizar la información de productos de seguros.

1. Si el contrato es "Desgravamen No Licitado", se realiza un cruce de datos con `ramo_reas_desgnl` usando la columna "COBERTURA DEL CONTRATO".
2. Para contratos como "Digital Klare", "K-Fijo", "AP + Urgencias Medicas" y "Multisocios", se realiza un cruce con `ramo_reas_otros` usando las columnas "POL_PROD" y "CODIGO COBERTURA".
3. Se asigna el nombre del producto mediante un cruce con `nombre_prods` usando las columnas "PRODUCTO" y "BASE".
4. Si el contrato es "K-Fijo", se crean las columnas "MESES RENTA" con valor 1 y "TIPO ASEGURADO" con valor "Titular" para uniformizar la base de datos.
5. Se definen dos listas de campos: `campos_productos` y `campos_renovacion`, que contienen los nombres de las columnas seleccionadas para diferentes propósitos de salida.
6. `campos_productos` incluye campos para uso interno y para entrega a reaseguradores.
7. `campos_renovacion` contiene campos específicos para procesos de renovación de contratos.
8. El código asegura que todos los contratos tengan un formato uniforme al crear campos faltantes para ciertos tipos de contratos.
9. Utiliza la función `cruce_left` para realizar los cruces de datos, lo que sugiere que se trata de una operación de combinación de DataFrames.
10. La estructura del código permite manejar diferentes tipos de contratos de manera flexible y adaptativa.
```python
# ? Cruces solicitados por el área de productos
# ! Solicitado por el área de productos
# asignamos campos RAMO_REAS y COB_REAS
if contrato=='Desgravamen No Licitado':
	df = cruce_left(df, ramo_reas_desgnl, ['COBERTURA DEL CONTRATO'], ['COBERTURA DEL CONTRATO'],parameters,name='ramo_reas_desgnl')
elif contrato in ['Digital Klare','K-Fijo','AP + Urgencias Medicas','Multisocios']:
	df = cruce_left(df, ramo_reas_otros, ['POL_PROD','CODIGO COBERTURA'], ['POL_PROD','CODIGO COBERTURA'],parameters,name='ramo_reas_otros')
# Asignamos nombre de producto
df = cruce_left(df, nombre_prods, ['PRODUCTO','BASE'], ['PRODUCTO','BASE'],parameters,name='nombre_prods')
# Contrato K-Fijo no posee estos campos, así que los creamos para que la base sea uniforme para todos los contratos
if contrato == 'K-Fijo':
	# Creamos Meses de renta igual a 1
	df['MESES RENTA'] = 1
	# Creamos tipo de asegurado como titular
	df['TIPO ASEGURADO'] = 'Titular'
# Campos seleccionados tanto para la salida de uso interno como para la salida que se entregará a los reaseguradores
campos_productos = ['FECHA_CIERRE','BASE','IDENTIFICADOR','RUT','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','NOMBRE_PRODUCTO','PLAN','CODIGO_COBERTURA','CODIGO_COBERTURA_IAXIS','CONTRATO_REASEGURO','COBERTURA_DEL_CONTRATO','RAMO_REAS','COB_REAS','FECHA_EFECTO','FECHA_VENCIMIENTO','FECHA_ANULACION','FEC_NAC','EDAD','SEXO','TIPO_POLIZA','FORMA_PAGO_CODIGO','MESES_RENTA','INNOMINADA','PRIMA_NETA_ANUAL','ICAPITAL','MONTO_ASEGURADO','CAPITAL_RETENIDO_TOTAL','CAPITAL_CEDIDO_TOTAL','RECARGO']
campos_renovacion = ['FECHA_CIERRE','BASE','IDENTIFICADOR','SSEGURO','POLIZA','CERTIFICADO','PRODUCTO','CODIGO_COBERTURA','RAMO_REAS','COB_REAS','TIPO_POLIZA','FECHA_EFECTO','FECHA_VENCIMIENTO','FEC_NAC','EDAD','SEXO','MONTO_ASEGURADO']
```

## Reportería

La sección de *reportería* se encarga de generar informes o reportes basados en los datos procesados. Estos informes pueden incluir resúmenes, estadísticas o visualizaciones que faciliten la comprensión de la información. La finalidad es proporcionar una visión clara y accesible de los datos para la toma de decisiones.

- **Funcionalidad**: El código reemplaza los espacios en los nombres de las columnas de un DataFrame por guiones bajos.

- Se crea una lista vacía para almacenar los nuevos nombres de las columnas.
- Se itera sobre cada nombre de columna en el DataFrame.
- Para cada nombre de columna, se reemplazan los espacios por guiones bajos y se añade a la lista.
- Finalmente, se actualizan los nombres de las columnas del DataFrame con los nuevos nombres de la lista.
```python
# pequeño script para que todos nuestros campos tengan guin bajo en vez de espacio
cols_new = []
for col in df.columns:
	cols_new.append(col.replace(' ','_'))
df.columns = cols_new
```

- **Funcionalidad del código**: Exporta datos filtrados a archivos comprimidos en formato CSV para diferentes propósitos.

- Filtra un DataFrame para incluir solo las filas donde la columna 'CONTRATO_REASEGURO' no es nula. Luego, exporta estos datos a dos archivos CSV comprimidos, uno para uso interno y otro para reaseguradores, utilizando diferentes conjuntos de columnas y un formato específico para fechas y números.
```python
# Exportamos la salida de uso interno y la salida para los reaseguradores
df[df['CONTRATO_REASEGURO'].notnull()][campos_productos].to_csv(ruta_salidas+f'Detalle Renovacion {contrato} Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
df[df['CONTRATO_REASEGURO'].notnull()][campos_renovacion].to_csv(ruta_salidas+f'Detalle Renovacion {contrato} Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
```