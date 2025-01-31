# Modulo S2_Funciones


Este script contiene todas las funciones externas que ayudan ya sea a preprocesar la data o a calcular el reaseguro


# Funcion `escribe_reporta`

La función `escribe_reporta` toma un archivo de texto ya abierto y una cadena de texto como parámetros. Su propósito es escribir el contenido de la cadena de texto en el archivo proporcionado.

- **Funcionalidad del código**: Escribe texto en un archivo y añade una nueva línea al final.

- El código utiliza el método `write` de un objeto de archivo llamado `reporte` para agregar contenido. Después de escribir el texto, se inserta un carácter de nueva línea para asegurar que el siguiente contenido comience en una nueva línea.
```python
reporte.write(texto)
reporte.write('\n')
```

- **Funcionalidad del código**: Muestra en pantalla el contenido de una variable llamada `texto`.
- Utiliza la función `print()` para desplegar el valor almacenado en la variable `texto` en la consola.
```python
# Igualmente mostramos en pantalla lo que escribiremos en el reporte
print(texto)
```

# Funcion `get_all_subsets`

La función `get_all_subsets` toma como entrada una lista de cadenas y devuelve una lista de conjuntos. Cada conjunto en la lista representa un subconjunto posible del conjunto original.

- **Funcionalidad**: Convierte un conjunto en una lista para mantener el orden de los elementos.
- Convierte el conjunto dado en una lista, lo que permite que los elementos tengan un orden definido.
```python
# Convert the input set into a list to ensure order of elements in subsets.
s_list = list(s)
```

- **Funcionalidad**: Genera todas las combinaciones posibles de elementos de una lista dada.

- Utiliza `chain.from_iterable` junto con `combinations` para crear combinaciones de todos los tamaños posibles, desde cero hasta el tamaño total de la lista.
```python
# Generate all possible combinations of elements in the list.
all_combinations = chain.from_iterable(combinations(s_list, r) for r in range(len(s_list) + 1))
```

- **Funcionalidad**: Convierte una lista de combinaciones en una lista de subconjuntos únicos.
- **Explicación**: Itera sobre cada combinación en `all_combinations` y transforma cada una en un conjunto para eliminar duplicados, devolviendo una lista de estos conjuntos
```python
# Convert each combination into a set to obtain subsets.
return [set(comb) for comb in all_combinations]
```

# Funcion `filtra_una_combinacion`

La función `filtra_una_combinacion` realiza las siguientes acciones:

1. **Filtrado de Datos**: Toma un DataFrame y lo filtra según las características de un tipo específico de cálculo de reaseguro. Utiliza una lista de campos y una combinación específica de valores para determinar qué filas deben ser incluidas en el DataFrame filtrado.

2. **Manejo de Cambios en el Tiempo**: Si el tipo de cálculo asignado a un producto cambia con el tiempo, la función puede realizar un `merge_asof`, que es una operación de combinación que permite unir dos DataFrames basándose en una clave y un orden temporal.

3. **Parámetros de Filtrado**: Utiliza una tabla de parámetros para guiar el proceso de filtrado y cruce. Las columnas especificadas en `cols_cruce` se utilizan para realizar el cruce entre el DataFrame original y la tabla de parámetros.

4. **Resultado**: Devuelve una tupla con tres DataFrames:
   - El primero contiene las filas que coinciden con la combinación específica.
   - El segundo contiene las filas restantes después de aplicar el filtro.
   - El tercero es la tabla de parámetros original, sin modificaciones.

Esta función es útil para gestionar y analizar datos de reaseguro que pueden variar con el tiempo y requieren un filtrado específico basado en combinaciones de valores.

## Cálculos Iniciales

Este bloque de código se encarga de realizar operaciones matemáticas o lógicas necesarias antes de ejecutar procesos más complejos. Establece valores base o condiciones iniciales que serán utilizados en etapas posteriores del programa. Es fundamental para asegurar que el sistema funcione correctamente desde el principio.

- **Funcionalidad**: Crea una copia de un DataFrame existente para futuras operaciones de cruce con otra tabla.
- **Explicación**: Se genera una copia del DataFrame `df` y se asigna a `df_filtrado` para preservar el original mientras se realizan
```python
# df_filtrado que será el que se irá cruzando con la tabla de las asignaciones
df_filtrado: pd.DataFrame=df.copy()
```

- **Funcionalidad del código**: Añade una nueva columna al DataFrame que almacena el índice original de cada fila.
- **Explicación**: La columna `'INDICE'` se crea en `df_filtrado` para preservar el orden original de los registros al guardar sus
```python
# Se guarda el indice para no perder el orden de los registros
df_filtrado['INDICE']=df_filtrado.index
```

- **Funcionalidad**: Crea una copia de un DataFrame de pandas para realizar operaciones de filtrado sin modificar el original.
- Se utiliza el método `copy()` para duplicar el DataFrame `tabla_parametros` y asignarlo a `tabla_parametros_filtrada`.
```python
# Tabla auxiliar que tendrá filtrado lo que vamos a cruzar
tabla_parametros_filtrada: pd.DataFrame=tabla_parametros.copy()
```

- **Funcionalidad**: Convierte un objeto `combinacion` en una lista.
- **Explicación**: Se utiliza la función `list()` para transformar el objeto `combinacion` en una lista llamada `combinacion_list`.
```python
# Combinacion de campos que vamos a utilizar para cruzar
combinacion_list = list(combinacion)
```

- **Funcionalidad del código**: El código identifica elementos únicos en una lista al eliminar aquellos que están presentes en otra lista.

- Utiliza la función `set` para convertir `lista_campos` en un conjunto, luego aplica `difference` para eliminar los elementos que también están en `
```python
# Campos que no vamos a cruzar
combi_out=list(set(lista_campos).difference(combinacion_list))
```

- **Funcionalidad del código**: Filtra un DataFrame eliminando filas con valores nulos en ciertas columnas y ajusta el índice.

- Se eliminan las filas con valores nulos en las columnas especificadas por `combinacion_list` de un DataFrame llamado `tabla_parametros_filtrada`.
- Se identifican y eliminan filas que tienen valores nulos en todas las columnas especificadas por `combi_out`, almacenando temporalmente estas filas en `tabla_parametros_quitar`.
- Finalmente, se actualiza `tabla_parametros_filtrada` para excluir las filas eliminadas, y se restablece el índice del DataFrame.
```python
# Creamos la variable tabla_parametros_filtrada que contendra solo los registros que vamos a cruzar en esta combinacion
tabla_parametros_filtrada = tabla_parametros_filtrada.dropna(subset=combinacion_list) # type: ignore
tabla_parametros_quitar: pd.DataFrame=tabla_parametros_filtrada.dropna(subset=combi_out,how='all')
tabla_parametros_filtrada=tabla_parametros_filtrada.loc[tabla_parametros_filtrada.index.difference(tabla_parametros_quitar.index)].reset_index(drop=True)
```

## Asignación de Contratos de Reaseguro

Este proceso asigna contratos de reaseguro utilizando una combinación específica de criterios. Se busca optimizar la cobertura de riesgos al emparejar contratos con las necesidades particulares de reaseguro. La asignación se realiza de manera sistemática para asegurar que cada contrato cumpla con los requisitos establecidos.

- **Funcionalidad del código**: Separa un DataFrame en dos partes basadas en la presencia o ausencia de valores en la columna 'INICIO DEL CONTRATO'.

- Si el DataFrame `tabla_parametros_filtrada` no está vacío, se procede con la separación.
- Se crea un nuevo DataFrame `tpf_sin_inicio` que contiene las filas donde la columna 'INICIO DEL CONTRATO' es nula.
- Otro DataFrame `tpf_con_inicio` se genera con las filas donde la columna 'INICIO DEL CONTRATO' tiene valores no nulos.
```python
# En caso de que tabla_parametros_filtrada sea no vacia comenzamos con el proceso de asignacion
if not tabla_parametros_filtrada.empty:
	tpf_sin_inicio: pd.DataFrame=tabla_parametros_filtrada[tabla_parametros_filtrada['INICIO DEL CONTRATO'].isnull()].copy()
	tpf_con_inicio: pd.DataFrame=tabla_parametros_filtrada[~tabla_parametros_filtrada['INICIO DEL CONTRATO'].isnull()].copy()
```

- **Funcionalidad del código**: El código decide si realizar un merge entre dos DataFrames basándose en ciertas condiciones y prepara estructuras de datos para el resultado.

- Verifica si el DataFrame `tpf_sin_inicio` no está vacío.
- Si `tpf_sin_inicio` tiene datos, realiza un *merge* interno con `df_filtrado` usando columnas específicas y almacena el resultado en `df_filtrado_sin_inicio`.
- Si `tpf_sin_inicio` está vacío, crea un DataFrame vacío llamado `df_filtrado_sin_inicio`.
- Comprueba si `tpf_con_inicio` no está vacío y, de ser así, elimina duplicados basándose en ciertas columnas, almacenando el resultado en `tpf_con_inicio_unicos`.
- Inicializa un DataFrame vacío llamado `df_filtrado_con_inicio` para su uso posterior.
```python
# Pregunta si necesitamos hacer un merge o merge_asof, en caso de tener cambio de contratos en el tiempo
if not tpf_sin_inicio.empty: df_filtrado_sin_inicio=df_filtrado.merge(tpf_sin_inicio[combinacion_list+cols_cruce],how='inner',on=combinacion_list)
else: df_filtrado_sin_inicio=pd.DataFrame()
if not tpf_con_inicio.empty:
	tpf_con_inicio_unicos=tpf_con_inicio[combinacion_list].drop_duplicates()
	df_filtrado_con_inicio=pd.DataFrame()
```

- **Funcionalidad del código**: Filtra y combina datos de dos dataframes basados en condiciones específicas y los concatena en un dataframe final.

1. Itera sobre cada fila de un dataframe llamado `tpf_con_inicio_unicos`.
2. Convierte cada fila en una lista de valores.
3. Crea copias de los dataframes `df_filtrado` y `tpf_con_inicio` para trabajar con ellos sin modificar los originales.
4. Filtra las copias de los dataframes usando una lista de columnas y valores correspondientes.
5. Aplica un filtro a `df_filtrado_con_inicio_aux` y `tpf_con_inicio_filtrada` para que solo contengan filas donde las columnas especificadas coincidan con los valores de la lista.
6. Realiza una combinación asimétrica (merge_asof) de los dataframes filtrados, ordenando por fechas específicas.
7. Elimina las filas con valores nulos en las columnas especificadas después de la combinación.
8. Concatena el resultado filtrado y combinado al dataframe `df_filtrado_con_inicio`.
9. El proceso se repite para cada fila del dataframe `tpf_con_inicio_unicos`.
10. El resultado es un dataframe que contiene datos filtrados y combinados de acuerdo a las condiciones dadas.
```python
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
```

- **Funcionalidad del código**: Filtra un DataFrame y devuelve tres objetos: el DataFrame filtrado, el DataFrame restante y una tabla de parámetros.

1. Si una condición no se cumple, se crea un DataFrame vacío llamado `df_filtrado_con_inicio`.
2. Se concatenan `df_filtrado_con_inicio` y `df_filtrado_sin_inicio` para formar `df_filtrado`.
3. Se define `df_a_filtrar` como las filas de `df` que no están en `df_filtrado`, utilizando la diferencia de índices.
4. Se eliminan las columnas llamadas 'INDICE' de `df_filtrado`.
5. Se asigna `tabla_parametros` a `tabla_parametros_a_filtrar`.
6. Se devuelven `df_filtrado`, `df_a_filtrar` y `tabla_parametros_a_filtrar`.
```python
else: df_filtrado_con_inicio=pd.DataFrame()
df_filtrado=pd.concat([df_filtrado_con_inicio,df_filtrado_sin_inicio])
df_a_filtrar: pd.DataFrame=df.loc[df.index.difference(df_filtrado['INDICE'])] # type: ignore
df_filtrado=df_filtrado.drop(columns=['INDICE'])
tabla_parametros_a_filtrar=tabla_parametros
return df_filtrado,df_a_filtrar,tabla_parametros_a_filtrar
```

- **Funcionalidad del código**: Retorna un DataFrame vacío junto con dos variables adicionales si una condición específica se cumple.
- Si `tabla_parametros_filtrada` está vacía, el código devuelve un DataFrame vacío y las variables `df` y `tabla_parametros`.
```python
# En caso de que tabla_parametros_filtrada sea vacia retornamos un dataframe vacio
else : return pd.DataFrame(),df,tabla_parametros
```

# Funcion `asignacion_contratos`

La función `asignacion_contratos` asigna contratos de reaseguro a un conjunto de datos de asegurados. Utiliza repetidamente una función llamada `filtra_una_combinacion` para aplicar los filtros necesarios hasta que todos los contratos relevantes hayan sido asignados. 

### Parámetros:

- **`df`**: Un `DataFrame` que contiene la información de los asegurados.
- **`parameters`**: Un objeto que carga los parámetros necesarios para el cálculo.
- **`tables`**: Un objeto que carga las tablas que facilitan el cálculo de los contratos de reaseguro.
- **`mantiene_na`**: Un valor binario opcional que determina si se mantienen los registros que no fueron asignados a un contrato de reaseguro. Por defecto, está configurado en `0`.

### Retorno:

Devuelve un `DataFrame` que es una versión ampliada del original, con campos adicionales que describen las características del contrato de reaseguro asignado.

- **Funcionalidad del código**: Carga datos de hojas de cálculo y parámetros para configurar un proceso de asignación de pólizas al reaseguro.

- Se utiliza una función para obtener datos de una hoja de cálculo llamada "Ocurrencias" y se almacena en un DataFrame llamado `ocurrencias`.
- Otra hoja de cálculo, "Matriz Contrato-Cobertura", se carga en un DataFrame llamado `tabla_parametros`.
- Se extraen varios parámetros de un objeto `parameters`, incluyendo `contrato`, `tipo_calculo`, `tipo_prima` y `archivo_reporte`.
- `contrato` es una cadena que representa el contrato específico a utilizar.
- `tipo_calculo` y `tipo_prima` son cadenas que determinan el tipo de cálculo y prima a aplicar.
- `archivo_reporte` es un parámetro que puede ser de cualquier tipo y se utiliza para especificar el archivo de reporte.
```python
# Contiene las polizas que debo asignar por ocurrencia al reaseguro
ocurrencias: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Ocurrencias')
tabla_parametros: pd.DataFrame = tables.get_table_xlsx(sheet_name = 'Matriz Contrato-Cobertura')
contrato: str = parameters.parameters['contrato']
tipo_calculo: str = parameters.parameters['tipo_calculo']
tipo_prima: str = parameters.parameters['tipo_prima']
archivo_reporte: Any = parameters.parameters['archivo_reporte']
```

- **Funcionalidad**: El código obtiene el número total de filas en un DataFrame llamado `df`.
- **Explicación**: La variable `original_rows` almacena la cantidad de filas presentes en `df` utilizando el atributo `shape`.
```python
# Definiciones preliminares del proceso
original_rows=df.shape[0]
```

- **Funcionalidad del código**: Filtra y procesa registros de contratos de reaseguro para generar combinaciones de parámetros y reportar el inicio del proceso.

1. Filtra la tabla de parámetros para incluir solo los registros que coinciden con un contrato específico si el tipo de cálculo es "Prima de Reaseguro".
2. Define una lista de columnas que no se cruzan con las columnas específicas de contrato, cobertura e inicio del contrato.
3. Genera todas las combinaciones posibles de subconjuntos de las columnas restantes.
4. Elimina el conjunto vacío de las combinaciones generadas.
5. Inicializa un DataFrame vacío para almacenar resultados finales.
6. Escribe un mensaje en un archivo de reporte indicando el inicio del proceso de asignación de contratos de reaseguro, incluyendo la fecha y hora actuales.
```python
# Solo tomo los registros asociados al proceso que estoy corriendo. Mayor info en el diccionario de contratos
if tipo_calculo == 'Prima de Reaseguro': tabla_parametros=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato]
cols_cruce=['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','INICIO DEL CONTRATO']
lista_campos: list[str]=list(set(list(tabla_parametros.columns)).difference(cols_cruce))
lista_combinaciones: list[set[str]] = get_all_subsets(lista_campos)
lista_combinaciones.remove(set())
df_final=pd.DataFrame()
escribe_reporta(archivo_reporte,'COMIENZA LA ASIGNACION DE CONTRATOS DE REASEGURO:\n{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
```

- **Funcionalidad del código**: Determina la fecha relevante para el cálculo de seguros en función del tipo de contrato y condiciones específicas.

- Combina dos conjuntos de datos basados en la columna 'POLIZA'.
- Verifica si el número de registros ha aumentado después de la combinación y genera un reporte si es así.
- Asigna 'Ocurrencia' a las entradas vacías en la columna 'PERIODO DEL CONTRATO'.
- Para contratos de tipo "Siniestros de Reaseguro", establece la fecha relevante según el tipo de periodo del contrato.
- Si el tipo de prima es "Prima Unica", utiliza la fecha de efecto como la fecha relevante.
- En otros casos, asigna la fecha de cierre como la fecha relevante.
```python
# Asigna la fecha que debemos tener en consideracion para asignar tipo_calculo
# Si el tipo_calculo es por suscripcion, se toma la fecha de inicio vigencia
# Si el tipo_calculo es por ocurrencia, se toma la fecha de cierre
df=df.merge(ocurrencias,how='left',on=['POLIZA'])
if pd.concat([df_final,df]).shape[0]>original_rows: escribe_reporta(archivo_reporte, 'El dataframe posterior a la tabla de ocurrencias tiene más registros. REVISAR!')
df['PERIODO DEL CONTRATO'] = df['PERIODO DEL CONTRATO'].fillna('Ocurrencia')
if tipo_calculo == 'Siniestros de Reaseguro': df['FECHA CRUCE VIGENCIAS']=np.where(df['PERIODO DEL CONTRATO']=='Ocurrencia',df['FECHA_SINIESTRO'],df['INICIO_VIGENCIA'])
elif tipo_prima=='Prima Unica': df['FECHA CRUCE VIGENCIAS']=df['FECHA_EFECTO']
else : df['FECHA CRUCE VIGENCIAS']=df['FECHA CIERRE']
```

- **Funcionalidad del código**: Filtra y concatena registros de un DataFrame basado en combinaciones de columnas, generando un reporte si hay discrepancias.

1. Itera sobre una lista de combinaciones de columnas.
2. Verifica si todas las columnas de cada combinación están presentes en el DataFrame.
3. Filtra el DataFrame usando una función externa y concatena el resultado en un DataFrame final.
4. Comprueba si hay columnas en `lista_campos` que no están en el DataFrame y genera un reporte si es así.
5. Compara el número de filas del DataFrame final con el original y reporta si hay un aumento inesperado.
6. Devuelve el DataFrame final concatenado con el original si una condición específica se cumple.
7. Si la condición no se cumple, devuelve solo el DataFrame final.
8. Utiliza funciones auxiliares para filtrar y reportar, lo que sugiere modularidad en el diseño del código.
```python
# Recorre el dataframe para cada registro de filtro que deba hacer, para ir concatenandolos en el df que necesitamos
for combinacion in lista_combinaciones:
	if all(x in df.columns for x in combinacion):
		df_filtrado,df,tabla_parametros=filtra_una_combinacion(df,lista_campos,tabla_parametros,combinacion,cols_cruce)
		df_final=pd.concat([df_final,df_filtrado],axis=0)
if list(set(lista_campos) - set(df.columns)) != []: escribe_reporta(archivo_reporte, f'Las siguientes columnas no se encuentran en el df para poder asignar contratos\n{list(set(lista_campos) - set(df.columns))}\n')
if pd.concat([df_final,df]).shape[0]>original_rows: escribe_reporta(archivo_reporte, 'El dataframe posterior a la asignación de contratos tiene más registros. REVISAR!')
if mantiene_na==1: return pd.concat([df_final,df],axis=0)
else: return df_final
```

# Funcion `asignacion_vigencias`

La función `asignacion_vigencias` tiene como propósito principal determinar la vigencia del reaseguro para cada registro en un conjunto de datos. Además, puede eliminar registros que, basándose en sus fechas de inicio y fin, no deberían formar parte del contrato de reaseguro.

### Parámetros de Entrada:

- **`df`**: Un `DataFrame` que contiene la información de los asegurados.
- **`parameters`**: Un objeto `Parameter_Loader` que incluye los parámetros necesarios para el cálculo.
- **`tables`**: Otro objeto `Parameter_Loader` que proporciona tablas auxiliares para el cálculo.
- **`mantiene_na`**: Un valor entero opcional (por defecto es 0) que actúa como un indicador binario para decidir si se mantienen los registros que no fueron asignados a ningún contrato de reaseguro.

### Valor de Retorno:

La función devuelve una tupla que contiene dos `DataFrames`:

1. El `DataFrame` original, pero enriquecido con campos adicionales que describen las características de la vigencia del contrato asignado.
2. Un segundo `DataFrame` que podría contener información adicional relacionada con el proceso de asignación de vigencias.

- **Funcionalidad del código**: Filtra y procesa datos relacionados con contratos de reaseguro y sus coberturas, registrando el inicio del proceso en un archivo de reporte.

1. Se obtienen parámetros de configuración como `archivo_reporte`, `contrato`, y `tipo_calculo` desde un objeto `parameters`.
2. Se carga una tabla de parámetros desde un archivo Excel, específicamente desde la hoja llamada 'Matriz Vigencias'.
3. Se escribe un mensaje en el archivo de reporte indicando el inicio del proceso de asignación de vigencias, incluyendo la fecha y hora actuales.
4. Se guarda una lista de las columnas iniciales del DataFrame `df`.
5. Se crea un DataFrame `df_nuls` que contiene las filas de `df` donde el campo 'CONTRATO REASEGURO' es nulo.
6. Se inicializa un DataFrame vacío llamado `df_final`.
7. Se verifica si el tipo de cálculo es 'Prima de Reaseguro'.
8. Si el tipo de cálculo es 'Prima de Reaseguro', se filtra la tabla de parámetros para incluir solo las filas donde el 'CONTRATO REASEGURO' coincide con el contrato especificado.
9. Se extraen las coberturas únicas del contrato de reaseguro filtrado y se almacenan en `cobs_reaseguro`.
```python
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
```

- **Funcionalidad del código**: Filtra y combina datos de dos tablas basadas en una lista de coberturas de reaseguro.

- Itera sobre una lista de coberturas de reaseguro.
- Filtra un DataFrame para obtener filas que coincidan con la cobertura actual.
- Filtra otra tabla de parámetros de manera similar.
- Combina los DataFrames filtrados usando una fusión asimétrica basada en fechas y agrega el resultado a un DataFrame final.
```python
# Ciclo que recorre cada uno de los elementos de la losta para ir filtrando y asignado vigencias
for cobertura_reaseg in cobs_reaseguro:
	df_filtrado: pd.DataFrame = df[df['COBERTURA DEL CONTRATO']==cobertura_reaseg]
	tabla_filtrada: pd.DataFrame = tabla_parametros[tabla_parametros['COBERTURA DEL CONTRATO']==cobertura_reaseg]
	df_final: pd.DataFrame = pd.concat(objs=[df_final,pd.merge_asof(df_filtrado.sort_values('FECHA CRUCE VIGENCIAS'),tabla_filtrada.sort_values('FECHA INICIO CONTRATO').drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'],axis=1),left_on=['FECHA CRUCE VIGENCIAS'],right_on='FECHA INICIO CONTRATO')],axis=0) # type: ignore
```

Este código filtra y organiza datos relacionados con contratos de reaseguro y sus coberturas específicas.

- Verifica si el tipo de cálculo es "Siniestros de Reaseguro".
- Obtiene una lista única de contratos de reaseguro desde un DataFrame.
- Itera sobre cada contrato de reaseguro único.
- Filtra una tabla de parámetros para obtener solo las filas correspondientes al contrato actual.
- Extrae las coberturas únicas asociadas al contrato de reaseguro actual.
- Filtra el DataFrame original para obtener solo las filas que corresponden al contrato de reaseguro actual.
```python
if tipo_calculo == 'Siniestros de Reaseguro':
	contratos_reaseguro = df['CONTRATO REASEGURO'].unique()
	for contrato_reaseg in contratos_reaseguro:
		tabla_parametros_contrato=tabla_parametros[tabla_parametros['CONTRATO REASEGURO']==contrato_reaseg]
		cobs_reaseguro=tabla_parametros_contrato[tabla_parametros_contrato['CONTRATO REASEGURO']==contrato_reaseg]['COBERTURA DEL CONTRATO'].unique()
		df_contrato = df[df['CONTRATO REASEGURO']==contrato_reaseg]
```

- **Funcionalidad del código**: Filtra y combina datos de contratos de reaseguro basados en coberturas y fechas para asignar vigencias.

- Itera sobre una lista de coberturas de reaseguro.
- Filtra un DataFrame de contratos para obtener solo las filas que coinciden con la cobertura actual.
- Filtra otra tabla de parámetros de contrato para la misma cobertura.
- Combina los datos filtrados de ambos DataFrames basándose en fechas, y los agrega a un DataFrame final.
```python
# Ciclo que recorre cada uno de los elementos de la losta para ir filtrando y asignado vigencias
for cobertura_reaseg in cobs_reaseguro:
	df_filtrado=df_contrato[(df_contrato['COBERTURA DEL CONTRATO']==cobertura_reaseg)]
	tabla_filtrada=tabla_parametros_contrato[tabla_parametros_contrato['COBERTURA DEL CONTRATO']==cobertura_reaseg]
	df_final=pd.concat([df_final,pd.merge_asof(df_filtrado.sort_values('FECHA CRUCE VIGENCIAS'),tabla_filtrada.sort_values('FECHA INICIO CONTRATO').drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO'],axis=1),left_on=['FECHA CRUCE VIGENCIAS'],right_on='FECHA INICIO CONTRATO')],axis=0) # type: ignore
```

- **Funcionalidad del código**: Filtra un DataFrame para eliminar registros con fechas fuera del rango de vigencia de contratos.

1. Se identifican las columnas adicionales en `df_final` que no están en `cols_iniciales`.
2. Se filtran los registros de `df_final` para conservar solo aquellos con una fecha de vigencia de contrato no nula.
3. Se realiza una segunda filtración para mantener solo los registros donde la fecha de cruce de vigencias es menor o igual a la fecha de fin del contrato.
4. Se almacenan en `df_deleted` los registros que no cumplen con las condiciones de vigencia, es decir, aquellos con fecha de vigencia nula o con fecha de cruce de vigencias mayor a la fecha de fin del contrato.
5. Se utilizan copias de los DataFrames para evitar modificar los datos originales.
```python
# Eliminamos registros con fecha posterior o anterior a los contratos de vigencia establecidos
cols_finales=list(df_final.columns)
cols_extra=[x for x in cols_finales if not x in cols_iniciales]
df_final_01=df_final[~df_final['VIGENCIA CONTRATO'].isnull()].copy()
df_final_02=df_final_01[df_final_01['FECHA CRUCE VIGENCIAS']<=df_final_01['FECHA FIN CONTRATO']].copy()
df_deleted=df_final[(df_final['VIGENCIA CONTRATO'].isnull())|(df_final['FECHA CRUCE VIGENCIAS']>df_final['FECHA FIN CONTRATO'])].copy()
```

- **Funcionalidad del código**: El código verifica y reporta la eliminación de registros en un DataFrame basándose en criterios de fechas y devuelve los DataFrames resultantes.

1. Calcula cuántos registros tienen valores nulos en la columna 'VIGENCIA CONTRATO' y los cuenta.
2. Calcula cuántos registros tienen una 'FECHA CRUCE VIGENCIAS' posterior a 'FECHA FIN CONTRATO' y los cuenta.
3. Si hay registros eliminados por fechas anteriores, escribe un reporte indicando cuántos y el tipo de cálculo de reaseguro.
4. Si hay registros eliminados por fechas posteriores, escribe un reporte similar al anterior.
5. Si la variable `mantiene_na` es igual a 1, concatena y devuelve un DataFrame combinado con registros eliminados y nulos.
6. Si `mantiene_na` no es igual a 1, devuelve solo los DataFrames `df_final_02` y `df_deleted`.
```python
# Revisamos cuantos elementos se eliminaron por temas de fechas
reg_elim_ant=sum(df_final['VIGENCIA CONTRATO'].isnull())
reg_elim_post=sum(df_final_01['FECHA CRUCE VIGENCIAS']>df_final_01['FECHA FIN CONTRATO'])
if reg_elim_ant>0:escribe_reporta(archivo_reporte,f'Se eliminaron {reg_elim_ant} registros cuya fecha es anterior al primer {tipo_calculo} de reaseguro establecido')
if reg_elim_post>0:escribe_reporta(archivo_reporte,f'Se eliminaron {reg_elim_post} registros cuya fecha es posterior al ultimo {tipo_calculo} de reaseguro establecido')
if mantiene_na==1: return pd.concat([df_final_02,df_deleted.drop(['CONTRATO REASEGURO','COBERTURA DEL CONTRATO','INICIO DEL CONTRATO']+cols_extra,axis=1),df_nuls],axis=0),df_deleted
else: return df_final_02,df_deleted
```

# Funcion `cumulo_riesgo`

La función `cumulo_riesgo` procesa un conjunto de registros para calcular la suma de riesgos (cúmulos) y determinar los límites o retenciones aplicables sobre el capital. Utiliza un DataFrame que contiene información de pólizas y capital, junto con parámetros y tablas auxiliares que guían el cálculo.

### Parámetros principales:
- **DataFrame (`df`)**: Contiene los datos necesarios para el cálculo, como pólizas y capital.
- **Parámetros (`parameters`)**: Incluyen configuraciones como tipo de cálculo y contrato de reaseguro.
- **Tablas (`tables`)**: Proveen información adicional sobre retenciones o límites.
- **Riesgo de cúmulo (`riesgo_cumulo`)**: Especifica el riesgo que se está procesando.
- **Campos de agrupación (`campos_str`)**: Nombres de campos para agrupar registros y calcular cúmulos.
- **Límite o retención (`limite_retencion`)**: Puede ser un valor fijo o una referencia a una tabla con valores variables.
- **Tipo de cúmulo (`tipo_cumulo`)**: Columna que se evalúa para filtrar registros específicos.
- **Columna de capital (`columna_capital`)**: Indica el capital sobre el cual se aplican retenciones o límites.

### Resultado:
Devuelve un DataFrame con:
- **CUMULO**: Suma del capital agrupado.
- **CUMULO_PAGADOS** (opcional): Suma del capital para siniestros pagados.
- **PORCENTAJE**: Porcentaje aplicado para ajustar el capital.
- **CAPITAL POSTERIOR**: Capital ajustado tras aplicar retenciones o límites.
- **PORCENTAJE PAGADOS y PORCENTAJE PENDIENTES** (opcional): Calculados si se incluyen siniestros, unificados en la columna `PORCENTAJE`.

### Notas adicionales:
- Filtra registros según el contrato de reaseguro y tipo de cúmulo.
- Agrupa registros para sumar el capital.
- Puede cargar tablas de retenciones si `limite_retencion` es un nombre de tabla.
- Separa siniestros pagados para aplicar diferentes porcentajes.
- Genera reportes de advertencia si hay discrepancias en los datos.

- **Funcionalidad**: El código extrae valores de un diccionario anidado dentro de un objeto `parameters` y los asigna a variables específicas.

- Se accede a un diccionario llamado `parameters` para obtener valores asociados a las claves `'tipo_calculo'`, `'contrato'` y `'archivo_reporte'`.
- Estos valores se asignan a las variables `tipo_calculo`, `contrato` y `archivo_reporte`, respectivamente.
- También se extrae el valor de `ruta_extensa` directamente del objeto `parameters` y se asigna a la variable `ruta_extensa`.
```python
tipo_calculo: str = parameters.parameters['tipo_calculo']
contrato: str = parameters.parameters['contrato']
archivo_reporte: Any = parameters.parameters['archivo_reporte']
ruta_extensa: str = parameters.ruta_extensa
```

## Cálculo de Cúmulo para Tipo de Cálculo y Riesgo Particular

Esta función se encarga de calcular el cúmulo, que es una acumulación de valores, para un tipo específico de cálculo y un riesgo particular. Utiliza parámetros de entrada que definen el tipo de cálculo y el riesgo, y devuelve un resultado que representa el cúmulo calculado. Es útil para evaluar riesgos y tomar decisiones basadas en datos acumulados.

- **Funcionalidad**: Filtra un DataFrame para obtener filas que coincidan con un contrato de reaseguro específico y un riesgo de cúmulo determinado.
- Utiliza condiciones lógicas para seleccionar solo las filas donde el valor en la columna 'CONTRATO REASEGU
```python
# Filtro el df por el tipo_calculo y el riesgo de cumulo correspondiente
df_filter=df[(df['CONTRATO REASEGURO']==contrato) & (df[tipo_cumulo]==riesgo_cumulo)]
```

- **Funcionalidad del código**: Verifica si un DataFrame filtrado está vacío y, de ser así, crea columnas específicas en él.
- Si el DataFrame filtrado no contiene datos, se añaden las columnas necesarias para asegurar que el DataFrame tenga la estructura adecuada.
```python
# Si el df filtrado es vacio (pensado en que la funcion cumulos recorre todos los registros de su tabla de cumulos), entonces crea en el df las columnas que se crearán en caso de no ser vacio
if df_filter.empty:
```

- **Funcionalidad del código**: El código imprime un mensaje formateado y devuelve un DataFrame filtrado.
- Utiliza la función `print` para mostrar un mensaje que indica que el DataFrame está vacío para un tipo específico de cálculo de reaseguro y riesgo cúm
```python
#print('dataframe vacio para tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato,riesgo_cumulo))
return df_filter
```

- **Funcionalidad del código**: Agrupa un DataFrame por campos especificados y calcula la suma de una columna, generando reportes si los campos no existen.

1. Se define una lista de campos a partir de una cadena de texto separada por comas.
2. Se verifica si todos los campos de la lista existen en las columnas del DataFrame.
3. Si todos los campos existen, el DataFrame filtrado se agrupa por estos campos y se suma la columna especificada.
4. El resultado del agrupamiento se renombra para que la columna de suma se llame "CUMULO".
5. Si el tipo de cálculo incluye "Siniestro", se realiza un agrupamiento adicional para los registros con estado "PAGADO", renombrando la columna de suma a "CUMULO_PAGADOS".
6. Si algún campo no existe en el DataFrame, se genera un reporte indicando el campo faltante.
7. El reporte también incluye información sobre el tipo de cálculo de reaseguro y el riesgo cúmulo.
8. La función `escribe_reporta` se utiliza para escribir el reporte en un archivo especificado.
```python
# Hacemos groupby por la lista de campos que entregamos. Tomamos como agregacion la columna de cumulos que indicamos en los parametros
lista_campos: list[str] = campos_str.split(',')
if sum([0 if x in df.columns else 1 for x in lista_campos])==0:
	df_grouped = df_filter.groupby(lista_campos)[columna_capital].sum().reset_index().rename(columns={columna_capital:'CUMULO'})
	if 'Siniestro' in tipo_calculo: df_grouped_pagados=df_filter[df_filter['ESTADO SINIESTRO']=='PAGADO'].groupby(lista_campos)[columna_capital].sum().reset_index().rename(columns={columna_capital:'CUMULO_PAGADOS'})
else:
	for campo in lista_campos:
		if campo not in df.columns:
			escribe_reporta(archivo_reporte,'El campo {} para hacer cumulo no se encuentra dentro del dataframe, para tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(campo,contrato,riesgo_cumulo))
```

- Este código verifica si la variable `limite_retencion` es un texto que indica el nombre de una tabla.
- Utiliza la función `isinstance` para comprobar si `limite_retencion` es de tipo cadena de caracteres (`str`).
```python
# Calculo de retencion:
# Si la retencion no es igual para todos registros, se debe inrgesar el nombre de la tabla que parametriza aquello
if isinstance(limite_retencion, str):
```

- **Funcionalidad del código**: Intenta obtener una tabla específica de un archivo Excel y reporta un error si no existe.

- Utiliza un bloque `try-except` para manejar posibles errores al intentar obtener una tabla de un archivo Excel.
- Define una variable para almacenar la tabla obtenida, utilizando una función que busca la hoja especificada.
- Si la hoja no se encuentra, se captura la excepción y se llama a una función para escribir un mensaje de error en un archivo de reporte.
- El mensaje de error incluye detalles sobre el tipo de cálculo de reaseguro y el riesgo cúmulo involucrado.
```python
# Busca la tabla del nombre que pusimos dentro de la tabla de cumulos
try:
	tabla_cumulos: pd.DataFrame = tables.get_table_xlsx(sheet_name = limite_retencion)
except:
	escribe_reporta(archivo_reporte,f'la tabla de retenciones especificada no existe para el tipo_calculo de reaseguro {contrato} y riesgo cumulo {riesgo_cumulo}')
```

- **Funcionalidad**: Elimina un campo específico de una lista de nombres de columnas de una tabla.

- Se crea una lista con los nombres de las columnas de `tabla_cumulos`. Luego, se elimina de esta lista el campo que tiene el nombre `'LIMITE O RETENCION'`.
```python
# Revisa el nombre de los campos que tiene, y quita el que contiene el limite
campos = list(tabla_cumulos.columns)
campos.remove('LIMITE O RETENCION')
```

- **Funcionalidad del código**: Combina dos conjuntos de datos en función de campos comunes.
- Utiliza la función `merge` para realizar una unión interna entre `df_grouped` y `tabla_cumulos`, emparejando las filas basadas en los valores de las
```python
# Cruza con el df de acuerdo al resto de campos dentro de la tabla
df_grouped=df_grouped.merge(tabla_cumulos,how='inner', left_on=campos, right_on=campos,suffixes=['','_x']) # type: ignore
```

Claro, por favor proporciona el código que deseas que explique.
```python
else:
```

- **Funcionalidad**: Asigna un valor constante a una columna específica de un DataFrame agrupado.
- **Explicación**: La columna 'LIMITE O RETENCION' del DataFrame agrupado se rellena con el valor almacenado en la variable `limite
```python
# Si la retencion es igual para todos los registros, rellena con ese valor dentro del df agrupado
	df_grouped['LIMITE O RETENCION']=limite_retencion
```

- El código calcula porcentajes basados en condiciones específicas de los datos agrupados en un DataFrame.

1. Verifica si la palabra 'Siniestro' no está presente en la variable `tipo_calculo`.
2. Si 'Siniestro' no está presente, calcula un porcentaje dividiendo 'LIMITE O RETENCION' por 'CUMULO', asegurando que el resultado no supere 1, y asigna 0 si 'CUMULO' es 0.
3. Si 'Siniestro' está presente, realiza una combinación de datos con otro DataFrame llamado `df_grouped_pagados`.
4. Rellena los valores nulos en la columna 'CUMULO_PAGADOS' con 0.
5. Calcula 'PORCENTAJE PAGADOS' de manera similar, usando 'CUMULO_PAGADOS' en lugar de 'CUMULO'.
6. Calcula 'PORCENTAJE PENDIENTES' considerando la diferencia entre 'LIMITE O RETENCION' y el mínimo entre 'LIMITE O RETENCION' y 'CUMULO_PAGADOS', dividido por la diferencia entre 'CUMULO' y 'CUMULO_PAGADOS'.
7. Utiliza la función `np.where` para aplicar condiciones y asegurar que los porcentajes no excedan 1 y maneja casos donde los denominadores son 0.
```python
# Crea la columna porcentaje
if 'Siniestro' not in tipo_calculo:
	df_grouped['PORCENTAJE'] = np.where(df_grouped['CUMULO'] == 0, 0, np.minimum( 1, df_grouped['LIMITE O RETENCION'] / df_grouped['CUMULO']))
else:
	df_grouped=df_grouped.merge(df_grouped_pagados,how='left',on=lista_campos)
	df_grouped['CUMULO_PAGADOS']=df_grouped['CUMULO_PAGADOS'].fillna(0)
	df_grouped['PORCENTAJE PAGADOS'] = np.where(df_grouped['CUMULO_PAGADOS'] == 0, 0, np.minimum( 1, df_grouped['LIMITE O RETENCION'] / df_grouped['CUMULO_PAGADOS']))
	df_grouped['PORCENTAJE PENDIENTES'] = np.where(df_grouped['CUMULO_PAGADOS'] == df_grouped['CUMULO'], 0, np.minimum( 1, ( df_grouped['LIMITE O RETENCION'] - np.minimum( df_grouped['LIMITE O RETENCION'], df_grouped['CUMULO_PAGADOS'])) / ( df_grouped['CUMULO'] - df_grouped['CUMULO_PAGADOS'])))
```

- **Funcionalidad del código**: Compara el número de registros entre un DataFrame original filtrado y uno agrupado, reportando si hay discrepancias.

- Se realiza una combinación interna (*inner join*) entre un DataFrame filtrado y uno agrupado, utilizando una lista de campos comunes para el cruce.
- Se verifica si el número de registros en el DataFrame resultante es mayor, menor o igual al del DataFrame filtrado original.
- Si el número de registros es mayor, se genera un reporte indicando que el cruce produjo más registros.
- Si el número de registros es menor, se genera un reporte indicando que el cruce produjo menos registros.
- Los reportes incluyen información específica sobre el tipo de cálculo de reaseguro y el riesgo cúmulo involucrados.
```python
# Finalmente, cruza el df original y filtrado con el df agrupado
df_final=df_filter.merge(df_grouped,how='inner', left_on=lista_campos, right_on=lista_campos,suffixes=['','_x']) # type: ignore
if df_final.shape[0] > df_filter.shape[0]:
	escribe_reporta(archivo_reporte,'la tabla agrupada con los limites y retenciones cruzó más registros que el df original, para el tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato,riesgo_cumulo))
elif df_final.shape[0] < df_filter.shape[0]:
	escribe_reporta(archivo_reporte,'la tabla agrupada con los limites y retenciones cruzó menos registros que el df original, para el tipo_calculo de reaseguro {} y riesgo cumulo {}'.format(contrato,riesgo_cumulo))
```

- **Funcionalidad del código**: Calcula el capital posterior basado en el estado de un siniestro y ajusta el DataFrame en consecuencia.

- Si el tipo de cálculo incluye "Siniestro", se asigna un porcentaje basado en si el estado del siniestro es "PAGADO" o no.
- Se calcula el capital posterior multiplicando el porcentaje asignado por el valor de una columna específica de capital.
- Si el tipo de cálculo incluye "Siniestro", se eliminan las columnas de porcentajes pagados y pendientes del DataFrame.
- Finalmente, el DataFrame modificado se devuelve.
```python
# Columna que calcula el capital posterior a establecer limite o retencion, segun corresponda
if 'Siniestro' in tipo_calculo: df_final['PORCENTAJE']=np.where(df_final['ESTADO SINIESTRO']=='PAGADO',df_final['PORCENTAJE PAGADOS'],df_final['PORCENTAJE PENDIENTES'])
df_final['CAPITAL POSTERIOR']=df_final['PORCENTAJE']*df_final[columna_capital]
if 'Siniestro' in tipo_calculo: df_final.drop(columns=['PORCENTAJE PAGADOS', 'PORCENTAJE PENDIENTES'],inplace=True)
return df_final
```

# Funcion `cumulos`

La función `cumulos` procesa un conjunto de datos en un `DataFrame` para calcular y asignar límites o retenciones de cúmulo de reaseguros. Utiliza parámetros de configuración y tablas específicas para definir los tipos de cúmulos. El resultado es un `DataFrame` actualizado que incluye columnas adicionales o modificadas para reflejar los cálculos de cúmulo, como límites, retenciones, porcentajes aplicados y capital ajustado.

- **Funcionalidad del código**: Calcula y registra cúmulos financieros basados en diferentes configuraciones y tipos de riesgo.

1. Se obtienen tablas de configuraciones de cúmulos desde un archivo Excel.
2. Se recuperan parámetros relevantes como el archivo de reporte y el tipo de cálculo.
3. Se define un diccionario que mapea tipos de cúmulos a sus configuraciones específicas.
4. Se registra el inicio del proceso de cálculo de cúmulos en un archivo de reporte.
5. Se selecciona la tabla de cúmulos correspondiente al tipo especificado.
6. Se verifica que los campos requeridos existan en el DataFrame.
7. Se filtran registros con valores nulos en el campo de cúmulo.
8. Se inicializan campos en el DataFrame con valores por defecto.
9. Si el cálculo es de siniestros, se calcula el cúmulo de pagados para registros con estado "PAGADO".
10. Se establece un porcentaje inicial de 1 para los cálculos.
11. Se asigna el valor del campo base al campo de capital posterior.
12. Se itera sobre cada configuración en la tabla de cúmulos.
13. Se aplica la función `cumulo_riesgo` a cada configuración y se concatenan los resultados.
14. Se renombran las columnas del DataFrame para reflejar el tipo de cúmulo aplicado.
15. El DataFrame resultante se devuelve con las configuraciones de cúmulo aplicadas.
```python
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
```

# Funcion `calcula_edad`

La función `calcula_edad` determina la edad de cada registro utilizando las fechas de nacimiento y una fecha de corte. Además, implementa reglas para manejar datos faltantes o inválidos y puede generar un reporte sobre estos problemas. 

### Parámetros:

- **`rut_series`**: Serie con identificadores únicos de cada registro.
- **`fec_nac_series`**: Serie con las fechas de nacimiento.
- **`fec_corte_series`**: Fecha o serie de fechas para calcular la edad.
- **`edad_perdidos`**: Edad asignada a registros con fechas de nacimiento inválidas, si no se usa la edad promedio.
- **`edad_tope`**: Edad máxima permitida; las edades superiores se ajustan a este valor si no se usa la edad promedio.
- **`archivo_reporte`**: Archivo donde se escriben los problemas detectados.
- **`reporta_issues`**: Indica si se debe retornar un arreglo con los registros problemáticos (por defecto, no se reportan).
- **`edad_inf`**: Edad mínima permitida; las edades inferiores se ajustan a este valor si no se usa la edad promedio.
- **`aplica_edad_prom_cartera`**: Indica si se debe usar la edad promedio para reemplazar edades perdidas o fuera de rango.

### Retorno:

- **`NDArray[np.int_]`**: Arreglo con la edad calculada para cada registro.
- **`Any`**: Opcionalmente, un arreglo que indica los registros problemáticos si `reporta_issues` es 1.

- **Funcionalidad del código**: Calcula la edad promedio de una cartera de personas, manejando fechas de nacimiento duplicadas o incorrectas.

1. Se crea un DataFrame inicial con columnas de RUT y fecha de nacimiento.
2. Se genera un segundo DataFrame para obtener la fecha de nacimiento mínima por cada RUT.
3. Se realiza una combinación de ambos DataFrames para asignar a cada RUT su fecha de nacimiento mínima.
4. La serie de fechas de nacimiento se actualiza con estos valores mínimos.
5. Se identifican fechas de nacimiento nulas o iguales a "1900-01-01" como incorrectas.
6. Se extraen el año y la combinación de mes/día de las fechas de nacimiento.
7. Se verifica si `fec_corte_series` es una serie o una fecha específica.
8. Dependiendo del tipo, se calculan el año y la combinación de mes/día de `fec_corte_series`.
9. Se calcula la edad promedio de la cartera, ignorando las fechas incorrectas.
10. Si no se aplica la edad promedio de la cartera, se asigna un valor específico a las fechas incorrectas.
```python
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
	(fec_nac_series.isnull()) | (fec_nac_series == datetime.datetime(1900, 1, 1)),1,0)
# Se obtienen año y combinación de mes/día a partir de la fecha de nacimiento.
serie_year = pd.DatetimeIndex(fec_nac_series).year  # type: ignore
serie_monthday = (
	pd.DatetimeIndex(fec_nac_series).month * 100 # type: ignore
	+ pd.DatetimeIndex(fec_nac_series).day # type: ignore
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
```

- **Funcionalidad del código**: Este código ajusta y valida edades en una serie de datos, reemplazando valores incorrectos y generando un reporte de problemas detectados.

1. Verifica si se debe aplicar la edad promedio de la cartera.
2. Si se aplica, reemplaza las edades incorrectas con el promedio.
3. Calcula la edad basada en la diferencia entre el año de corte y el año de la serie.
4. Ajusta el cálculo según el día y mes de corte.
5. Identifica registros con problemas de edad.
6. Marca registros con fechas incorrectas.
7. Marca edades que superan un límite superior.
8. Marca edades por debajo de un límite inferior.
9. Cuenta cuántas fechas son incorrectas.
10. Cuenta cuántas edades exceden el límite superior.
11. Ajusta las edades finales según los límites establecidos.
12. Si no se aplica el promedio, limita las edades al rango permitido.
13. Si se aplica el promedio, reemplaza edades fuera de rango con el promedio.
14. Escribe en un archivo de reporte si hay fechas incorrectas.
15. Informa la cantidad de registros con fechas incorrectas.
16. Escribe en el reporte si hay edades que exceden el límite superior.
17. Informa cuántos registros tienen edades mayores al límite.
18. Retorna la serie final de edades ajustadas.
19. Opcionalmente, también retorna un array de problemas detectados.
```python
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
cont_fecnac_tope = sum(edad_series > edad_tope) # type: ignore
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
	return edad_series_final # type: ignore
else:
	return edad_series_final, registros_issues
```

# Funcion `calcula_exposicion`

La función `calcula_exposicion` determina la exposición de cada registro en un DataFrame basado en fechas de inicio y fin. Ajusta estas fechas a un período específico definido por `fec_bop` (fecha de inicio del período) y `fec_eop` (fecha de fin del período). El resultado es un arreglo de NumPy que contiene la proporción de días expuestos para cada registro, calculada en relación a un número estándar de días de exposición, especificado por `exp_days`.

- **Funcionalidad del código**: Calcula la exposición temporal de registros en un DataFrame basado en fechas de inicio y fin, ajustadas a un período específico.

1. Se crea una copia del DataFrame original para preservar los datos originales.
2. Se añaden dos columnas al DataFrame: 'INICIO MES' y 'FIN MES'.
3. 'INICIO MES' se establece con la fecha de inicio del período.
4. 'FIN MES' se establece con la fecha de fin del período más un día.
5. Se calcula la fecha de inicio real para cada registro como el máximo entre 'INICIO MES' y la fecha de inicio del registro.
6. Se calcula la fecha de fin real para cada registro como el mínimo entre 'FIN MES' y la fecha de fin del registro.
7. Se determina la exposición de cada registro.
8. Si la fecha de inicio del registro es posterior a 'fec_eop', la exposición es 0.
9. Si la fecha de fin del registro es anterior a 'fec_bop', la exposición es 0.
10. Si la fecha de inicio del registro es mayor que su fecha de fin, la exposición es 0.
11. En otros casos, se calcula la diferencia en días entre la fecha de fin real y la fecha de inicio real.
12. La diferencia de días se normaliza dividiendo por `exp_days`.
13. La serie resultante de exposiciones se devuelve como resultado.
```python
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
```

# Funcion `completa_campo`

La función `completa_campo` se encarga de llenar los valores faltantes en una columna específica de un DataFrame. Para lograr esto, calcula el promedio de dicha columna agrupando por otras columnas especificadas. Luego, utiliza estos promedios para completar los valores nulos en la columna objetivo. Además, la función exporta el DataFrame resultante de la agrupación a un archivo CSV, utilizando parámetros de exportación proporcionados por un objeto `Parameter_Loader`. 

**Notas importantes:**

- Si no hay valores no nulos en la columna a rellenar, los valores faltantes no se completarán.
- El archivo CSV exportado incluye el prefijo `"0. Tabla Agrup"` en su nombre, seguido del nombre de la columna y los campos de agrupación.
- El parámetro `campo_cero` no afecta la lógica actual de la función.

- **Funcionalidad del código**: Rellena los valores faltantes en un DataFrame utilizando el promedio de grupos definidos por ciertas columnas y exporta los resultados agrupados a un archivo CSV.

1. Separa el DataFrame en dos: uno con valores nulos en una columna específica y otro sin valores nulos.
2. Elimina la columna con valores nulos del primer DataFrame.
3. Agrupa el segundo DataFrame por las columnas especificadas y calcula el promedio de la columna con valores no nulos.
4. Fusiona el DataFrame con valores nulos con el DataFrame agrupado para rellenar los valores faltantes.
5. Combina ambos DataFrames (el original sin valores nulos y el modificado con valores rellenados) en uno solo.
6. Exporta el DataFrame agrupado a un archivo CSV con un nombre basado en las columnas utilizadas para agrupar.
7. Devuelve el DataFrame final con los valores rellenados.
```python
# if campo_cero==True: df_sin_valores,df_con_valores=df[df[campo_rellenar].isnull()].copy(),df[~df[campo_rellenar].isnull()].copy()
# elif campo_cero==False: df_sin_valores,df_con_valores=df[(df[campo_rellenar].isnull())|(df[campo_rellenar]==0)].copy(),df[(~df[campo_rellenar].isnull())&(df[campo_rellenar]>0)].copy()
df_sin_valores,df_con_valores=df[df[campo_rellenar].isnull()].copy(),df[~df[campo_rellenar].isnull()].copy()
df_sin_valores.drop(columns=[campo_rellenar],axis=1,inplace=True)
df_agrupado=df_con_valores[[campo_rellenar]+campos_agrupar].groupby(campos_agrupar, dropna=False).agg('mean').reset_index()
df_sin_valores=df_sin_valores.merge(df_agrupado,how='left',on=campos_agrupar)
df_final=pd.concat([df_con_valores,df_sin_valores],axis=0)
df_agrupado.to_csv(parameters.parameters['ruta_output']+'0. Tabla Agrup '+campo_rellenar+' campos '+'_'.join(campos_agrupar)+'.csv',sep=parameters.parameters['separador_output'],decimal=parameters.parameters['decimal_output'],date_format='%d-%m-%Y',index=False)
return df_final
```

# Funcion `completa_campo_total`

La función `completa_campo_total` se encarga de **rellenar valores nulos o no válidos** en un campo específico de un DataFrame. Para lograr esto, realiza una serie de **agregaciones sucesivas** basadas en combinaciones de campos proporcionadas. Si después de estas agregaciones aún quedan valores nulos, estos se completan utilizando un **promedio global** previamente calculado.

### Parámetros:

- **`df`**: Es el DataFrame que contiene los datos donde se desea completar el campo.
- **`campo_completar`**: Es el nombre del campo que se va a completar.
- **`listas_campos_agrupar`**: Lista de campos que se usarán para agrupar los datos en cada paso de la agregación.
- **`parameters`**: Objeto que contiene configuraciones adicionales necesarias para la función.
- **`campo_cero`**: Indica si el valor cero debe considerarse válido al calcular el promedio. Si es `True`, el cero se considera válido; si es `False`, se trata como un valor a reemplazar.

### Retorno:

La función devuelve un DataFrame con una nueva columna llamada `<campo_completar>_FINAL`, que contiene los valores finales del campo completado.

### Notas adicionales:

- La función trabaja sobre una copia del DataFrame original para evitar modificar los datos de entrada directamente.
- El cálculo del promedio global puede incluir o excluir ceros, dependiendo del valor de `campo_cero`.
- Es necesario que la función `completa_campo` esté disponible en el entorno para que `completa_campo_total` funcione correctamente.

- **Funcionalidad**: El código crea una copia de un DataFrame y calcula un promedio basado en una condición.

- Se genera una copia del DataFrame original para preservar sus datos. Luego, si la variable `campo_cero` es verdadera, se procede a calcular un promedio general y se establecen valores iniciales en una columna específica.
```python
# Se crea una copia del DataFrame original para no modificarlo directamente.
df_aux = df.copy()
# Dependiendo del valor de 'campo_cero', se calcula el promedio general
# y se definen los valores iniciales de la columna '_FINAL'.
if campo_cero is True:
```

- **Funcionalidad**: Calcula el promedio de valores no nulos en una columna de un DataFrame y crea una nueva columna con los mismos valores.

- Se filtran los valores no nulos de una columna específica para calcular su promedio. Luego, se copia esta columna a una nueva columna con un nombre modificado.
```python
# Cuando 'campo_cero' es True, se consideran todos los valores distintos de nulo.
promedio_general = df_aux[~df_aux[campo_completar].isnull()][campo_completar].mean()
df_aux[campo_completar + '_FINAL'] = df_aux[campo_completar]
```

Claro, por favor proporciona el código que deseas que explique.
```python
else:
```

- **Funcionalidad del código**: Calcula el promedio de valores positivos y no nulos de una columna y asigna `NaN` a los valores no positivos en una nueva columna.

- Filtra un DataFrame para excluir valores nulos y ceros de una columna específica.
- Calcula el promedio de los valores restantes de esa columna.
- Crea una nueva columna en el DataFrame.
- En la nueva columna, copia los valores positivos de la columna original.
- Asigna `NaN` a los valores que son cero o negativos en la nueva columna.
- Utiliza la función `np.where` para realizar la asignación condicional.
- La nueva columna tiene el mismo nombre que la original, con '_FINAL' añadido al final.
- El proceso prepara los datos para un posible llenado posterior de los valores `NaN`.
```python
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
```

- **Funcionalidad del código**: Completa una columna en un DataFrame agrupando por campos específicos y rellena valores nulos con un promedio general.

- El código itera sobre una lista de campos de agrupación, aplicando una función para completar una columna específica del DataFrame.
- La función `completa_campo` se utiliza para llenar la columna objetivo basada en los grupos definidos por cada combinación de campos.
- Después de completar los valores por grupos, los valores nulos restantes en la columna se rellenan con un valor promedio general.
- Finalmente, el DataFrame modificado se devuelve con la columna completada.
```python
# Para cada campo (o combinación de campos) en la lista de agrupaciones,
# se realiza la función 'completa_campo' que completa la columna '_FINAL'.
for lista in listas_campos_agrupar:
	df_aux = df_aux=completa_campo(df_aux,campo_completar+'_FINAL',lista,parameters,campo_cero)
# Luego de completar por grupos, se rellena con el promedio_general
# los valores que aún estén nulos.
df_aux[campo_completar + '_FINAL'] = df_aux[campo_completar + '_FINAL'].fillna(promedio_general)
# Se retorna el DataFrame resultante con la nueva columna completada.
return df_aux
```

# Funcion `corrige_tasas_ges`

La función `corrige_tasas_ges` ajusta las tasas en un DataFrame de la siguiente manera:

1. **Identificación de Duplicados**: Detecta registros duplicados utilizando un campo de RUT y otros identificadores relevantes.

2. **Cálculo de Tasa Promedio**: Para los registros duplicados, calcula una tasa promedio.

3. **Eliminación y Asignación**: Elimina las tasas originales de los duplicados y asigna la nueva tasa promedio a los registros únicos.

4. **Establecimiento de Periodicidad**: Define la periodicidad de los registros como 'M' (mensual).

El resultado es un DataFrame corregido, donde los duplicados tienen tasas ajustadas y una periodicidad uniforme.

- **Funcionalidad del código**: Identifica y separa registros duplicados y no duplicados en un DataFrame basado en ciertos campos.

- Se define una variable que contiene el nombre del campo utilizado para identificar duplicados, obteniéndolo de un conjunto de parámetros.
- Se filtran los registros duplicados del DataFrame `df` considerando un conjunto específico de columnas, y se almacenan en una nueva variable.
- Los registros que no son duplicados se almacenan en otra variable, asegurando que no compartan índices con los duplicados.
```python
# Defino registros duplicados y no duplicados
campo_rut_duplicados: str = parameters.parameters['campo_rut_duplicados']
duplicados=df.loc[df.duplicated(subset=[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','COD_COB'],keep=False)].copy()
no_duplicados_ges=df[~df.index.isin(duplicados.index)].copy()
```

- **Funcionalidad del código**: Calcula la tasa promedio de registros duplicados agrupados por ciertos campos.
- Utiliza la función `groupby` para agrupar los datos por los campos especificados y luego aplica `mean` para obtener la tasa promedio de cada grupo.
```python
# Creo las tasas agrupadas de los registros duplicados (tomando la tasa promedio)
tasas_promedio=duplicados[[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION','TASA_CRED']].groupby([campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION']).agg('mean').reset_index()
```

- **Funcionalidad**: El código elimina ciertas columnas y luego elimina filas duplicadas de un DataFrame.

- Se eliminan las columnas especificadas, 'PERIOD_TASA' y 'TASA_CRED', del DataFrame llamado `duplicados`. Posteriormente, se eliminan las filas duplicadas restantes en el DataFrame.
```python
# Elimino la tasa y periodicidad de los duplicados, y luego elimino duplicados
duplicados=duplicados.drop(columns=['PERIOD_TASA','TASA_CRED'],axis=1)
duplicados=duplicados.drop_duplicates()
```

- **Funcionalidad del código**: Combina dos conjuntos de datos y ajusta un campo antes de unirlos en un solo DataFrame.

- El código realiza una fusión de los datos en `duplicados` con `tasas_promedio` utilizando una combinación a la izquierda basada en campos específicos.
- Se establece el valor `'M'` en la columna `PERIOD_CRED` para todos los registros en el DataFrame `duplicados`.
- Combina los DataFrames `no_duplicados_ges` y `duplicados` en un solo DataFrame llamado `df_final`.
- Devuelve el DataFrame `df_final` que contiene los datos combinados.
```python
# Cruzo ahora con las tasas promedio
duplicados=duplicados.merge(tasas_promedio,how='left',on=[campo_rut_duplicados,'POLIZA','CERTIFICADO','NRO_OPERACION'])
duplicados['PERIOD_CRED']='M'
df_final=pd.concat([no_duplicados_ges,duplicados],axis=0)
return df_final
```

# Funcion `recargos`

La función `recargos` procesa un *DataFrame* que contiene información sobre asegurados y calcula los recargos en la prima de reaseguro para aquellos que tienen una sobreprima o extraprima. Además, ofrece la opción de solo identificar los registros que requieren un recargo sin realizar el cálculo. Los parámetros de entrada incluyen el *DataFrame* de asegurados, un objeto que proporciona los parámetros necesarios para el cálculo, y un indicador binario que determina si se deben calcular los recargos o solo marcarlos. La función devuelve un *DataFrame* actualizado con la información de los recargos.

- **Funcionalidad**: El código carga parámetros de configuración necesarios para un proceso específico.

- Se define una variable para la ruta de los recargos utilizando un valor de un diccionario de parámetros.
- Se establece un separador de entrada para los datos, también obtenido del mismo diccionario.
- Se configura el símbolo decimal que se usará en los datos de entrada.
- Se define la ruta de salida donde se almacenarán los resultados o datos procesados.
- Se especifica la fecha de inicio del mes, probablemente para operaciones relacionadas con fechas.
```python
# Cargo parametros que voy a utilizar
ruta_recargos: str = parameters.parameters['ruta_recargos']
separador_input: str = parameters.parameters['separador_input']
decimal_input: str = parameters.parameters['decimal_input']
ruta_output: str = parameters.parameters['ruta_output']
fecha_inicio_mes: str = parameters.parameters['fecha_inicio_mes']
```

- **Funcionalidad**: El código renombra una columna en un DataFrame y actualiza la lista de columnas.

- Cambia el nombre de la columna 'PRIMA REASEGURO' a 'PRIMA REASEGURO SIN RECARGO' en el DataFrame `df`. Luego, crea una lista `cols_df_final` que contiene los nombres de las columnas actuales del DataFrame más una columna adicional llamada 'RECARGO'.
```python
# Redefino el nombre de la prima de reaseguro, si e sque ya viene
df.rename(columns={'PRIMA REASEGURO':'PRIMA REASEGURO SIN RECARGO'},inplace=True)
cols_df_final=list(df.columns)+['RECARGO']
```

## Cálculos de Recargos

Este proceso se encarga de calcular los recargos aplicables a los registros obtenidos de la base de datos de iAxis. Los recargos se determinan en función de criterios específicos que se aplican a cada registro. El objetivo es asegurar que todos los registros reflejen los costos adicionales correspondientes.

- **Funcionalidad del código**: Define una lista que contiene el nombre de una columna relacionada con fechas en un conjunto de datos de recargos.
- Se crea una lista llamada `cols_date_iaxis` que incluye el nombre de la columna `FECHA_INICIO_RECARGO`.
```python
# columnas de fecha dentro de la data de recargos
cols_date_iaxis=['FECHA_INICIO_RECARGO']
```

- **Funcionalidad**: El código carga un archivo de texto que contiene datos de recargos en un DataFrame de pandas.
- Utiliza la función `read_csv` de pandas para leer el archivo especificado, configurando separadores, formato de fecha y codificación de caracteres.
```python
# Cargamos base de recargos
recargos_iaxis=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos iAxis.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False,date_format='%d-%m-%Y',parse_dates=cols_date_iaxis)
```

- **Funcionalidad del código**: Filtra y separa datos de recargos en dos categorías: "Extraprima" y "Sobreprima".

- El código utiliza condiciones para filtrar un DataFrame llamado `recargos_iaxis`, seleccionando filas donde el tipo de recargo es "Extraprima (tanto por mil)" o "Sobreprima (%)" y el valor del recargo es mayor que cero. Luego, extrae columnas específicas para cada categoría y almacena los resultados en dos nuevos DataFrames: `extraprima_iaxis` y
```python
# Separamos la data anterior en data de sobreprima y data de extraprima
extraprima_iaxis=recargos_iaxis[(recargos_iaxis['TIPO_RECARGO']=='Extraprima (tanto por mil)')&(recargos_iaxis['VALOR_RECARGO']>0)][['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS','VALOR_RECARGO']]
sobreprima_iaxis=recargos_iaxis[(recargos_iaxis['TIPO_RECARGO']=='Sobreprima (%)')&(recargos_iaxis['VALOR_RECARGO']>0)][['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS','VALOR_RECARGO']]
```

- **Funcionalidad**: El código renombra columnas específicas en dos DataFrames de pandas.

- Cambia el nombre de la columna `'VALOR_RECARGO'` a `'VALOR_RECARGO_EXTRAPRIMA'` en el DataFrame `extraprima_iaxis`. De manera similar, renombra la misma columna a `'VALOR_RECARGO_SOBREPRIMA'` en el DataFrame `sobreprima_iaxis`.
```python
# Algunos cambios de columnas
extraprima_iaxis.rename(columns={'VALOR_RECARGO':'VALOR_RECARGO_EXTRAPRIMA'},inplace=True)
sobreprima_iaxis.rename(columns={'VALOR_RECARGO':'VALOR_RECARGO_SOBREPRIMA'},inplace=True)
```

- **Funcionalidad**: Filtra y copia registros de un DataFrame que cumplen una condición específica.
- Crea una copia de los registros del DataFrame original donde la columna 'BASE' tiene el valor 'IAXIS'.
```python
# Separamos los registros de expuestos provenientes de iAxis
df_iaxis=df[df['BASE']=='IAXIS'].copy()
```

- El código combina datos de sobreprima y extraprima con un DataFrame existente.
- Utiliza la función `merge` de pandas para unir el DataFrame `df_iaxis` con los DataFrames `extraprima_iaxis` y `sobreprima_iaxis` mediante una unión a la izquierda (*left join*), basándose en las columnas comunes: `SSEGURO`, `NRIESGO` y `CODIGO COBERTURA IAXIS`.
```python
# Cruces con data de sobreprima y extraprima
df_iaxis=df_iaxis.merge(extraprima_iaxis,how='left',on=['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS'])
df_iaxis=df_iaxis.merge(sobreprima_iaxis,how='left',on=['SSEGURO','NRIESGO','CODIGO COBERTURA IAXIS'])
```

- **Funcionalidad del código**: Calcula y guarda los recargos de reaseguro en un archivo CSV si es necesario.

1. Verifica si se deben calcular los recargos comprobando el valor de la variable `calcula_recargos`.
2. Si `calcula_recargos` es igual a 1, calcula el recargo usando valores de sobreprima y extraprima, ajustados por la participación del reasegurador.
3. Los recargos calculados se almacenan en una columna llamada `RECARGO` dentro del DataFrame `df_iaxis`.
4. Filtra las filas donde el recargo es mayor a cero y guarda estos datos en un archivo CSV con un nombre específico.
5. Si no se deben calcular los recargos, simplemente asigna un valor basado en la sobreprima al campo `RECARGO`.
```python
# Acá está la opción de calcular o no los recargos, dependiendo si la data ya viene con el calculo de la prima de reaseguro
if calcula_recargos==1:
	df_iaxis['RECARGO']=(df_iaxis['VALOR_RECARGO_SOBREPRIMA'].fillna(0)/100*df_iaxis['PRIMA REASEGURO SIN RECARGO']+df_iaxis['VALOR_RECARGO_EXTRAPRIMA'].fillna(0)/1000*df_iaxis['CAPITAL CEDIDO TOTAL']*1/12)*df_iaxis['PARTICIPACION DEL REASEGURADOR']
	df_iaxis[df_iaxis['RECARGO']>0].to_csv(ruta_output+'3. Recargos iAxis Detalle.csv',sep=';')
else:
	df_iaxis['RECARGO']=df_iaxis['VALOR_RECARGO_SOBREPRIMA'].fillna(0)/100
```

## Cálculos de Recargos para Registros de GES

Este proceso calcula los recargos aplicables a los registros obtenidos de la base de datos de GES. Los recargos se determinan en función de criterios específicos asociados a cada registro. El objetivo es ajustar los valores de acuerdo a las políticas establecidas.

- **Funcionalidad del código**: Carga datos desde archivos de texto en dos DataFrames de pandas.

- Utiliza la función `read_csv` de pandas para leer dos archivos de texto, especificando el separador, el formato decimal y la codificación de caracteres. Los datos se almacenan en dos variables diferentes para su posterior procesamiento.
```python
# Lectura de datas (ind=individuales - cr=credit related)
recargos_ges_cr=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos GES Credit.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
recargos_ges_ind=pd.read_csv(ruta_recargos+'1. Inputs Auxiliares\\Recargos\\'+'Recargos GES Individuales.txt',sep=separador_input,decimal=decimal_input,encoding='latin-1',low_memory=False)
```

- **Funcionalidad del código**: Filtra y copia registros de un DataFrame que pertenecen a la categoría 'GES'.
- Crea un nuevo DataFrame que contiene solo las filas donde la columna 'BASE' tiene el valor 'GES'.
```python
# Separamos los registros de expuestos provenientes de GES
df_ges=df[df['BASE']=='GES'].copy()
```

- El código combina datos de dos fuentes diferentes en un solo DataFrame utilizando coincidencias basadas en columnas específicas.
- Se realizan dos operaciones de fusión (merge) en el DataFrame `df_ges`, primero con `recargos_ges_cr` y luego con `recargos_ges_ind`, uniendo los datos según las columnas de póliza, RUT, certificado y código de cobertura.
```python
# Cruces con data de ind y cr
df_ges=df_ges.merge(recargos_ges_cr,how='left',left_on=['POLIZA','RUT','CERTIFICADO','CODIGO COBERTURA'],right_on=['POLIZA_T0057','RUT_T0057','SECUENCIAL','CODIGO_COBERTURA'],suffixes=['', '_x'])
df_ges=df_ges.merge(recargos_ges_ind,how='left',left_on=['POLIZA','RUT','CERTIFICADO','CODIGO COBERTURA'],right_on=['POLIZA','RUT','SECUENCIAL','CODIGO_COBERTURA'],suffixes=['', '_x'])
```

- **Funcionalidad del código**: Calcula y guarda los recargos de reaseguro en un archivo CSV si es necesario.

1. Verifica si se deben calcular los recargos mediante una variable de control.
2. Si los recargos deben calcularse, se crea una nueva columna llamada 'RECARGO' en un DataFrame.
3. Calcula los recargos basándose en varias condiciones relacionadas con fechas y porcentajes específicos.
4. Utiliza la función `np.where` para aplicar condiciones y cálculos a las columnas del DataFrame.
5. Los cálculos consideran diferentes tipos de sobreprimas y un porcentaje de recargo general.
6. Multiplica el resultado por la participación del reasegurador para obtener el valor final del recargo.
7. Filtra las filas donde el recargo es mayor que cero.
8. Guarda el resultado filtrado en un archivo CSV en una ruta especificada.
```python
# Acá está la opción de calcular o no los recargos, dependiendo si la data ya viene con el calculo de la prima de reaseguro
if calcula_recargos==1:
	df_ges['RECARGO'] = (np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_ACTIVIDAD'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['SOBREPRIMA_ACTIVIDAD'].fillna(0)/100)+\
					np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_MEDICO'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['SOBREPRIMA_MEDICO'].fillna(0)/100)+\
					np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_DEPORTE'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['SOBREPRIMA_DEPORTE'].fillna(0)/100)+\
					df_ges['PRIMA REASEGURO SIN RECARGO']*df_ges['PORCENTAJE_RECARGO'].fillna(0)/100+\
					np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_EXTRAPRIMA'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['CAPITAL CEDIDO TOTAL']*df_ges['EXTRAPRIMA'].fillna(0)/1000)*1/12)*df_ges['PARTICIPACION DEL REASEGURADOR']
	df_ges[df_ges['RECARGO']>0].to_csv(ruta_output+'3. Recargos GES Detalle.csv',sep=';')
else:
```

- **Funcionalidad del código**: Calcula el valor de la columna 'RECARGO' en un DataFrame basado en condiciones específicas relacionadas con fechas y porcentajes de recargo.

- Se utiliza la función `np.where` para evaluar condiciones y asignar valores a la columna 'RECARGO'.
- La primera condición verifica si las columnas 'ORIGEN' y 'PORCENTAJE_RECARGO' son nulas, asignando un valor de 0 o 1 en consecuencia.
- Las siguientes condiciones calculan recargos adicionales basados en fechas y porcentajes de sobreprima para actividad, médico y deporte, comparando con una fecha de inicio de mes.
- Finalmente, se suma el porcentaje de recargo general, asegurando que los valores nulos se consideren como 0.
```python
# df_ges['RECARGO']=np.where((df_ges['ORIGEN'].isnull())&(df_ges['PORCENTAJE_RECARGO'].isnull()),0,1)
df_ges['RECARGO'] = np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_ACTIVIDAD'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['SOBREPRIMA_ACTIVIDAD'].fillna(0)/100)+\
				np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_MEDICO'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['SOBREPRIMA_MEDICO'].fillna(0)/100)+\
				np.where((df_ges['FECHA_EFECTO'].dt.to_period('M') + df_ges['MESES_SOBREPRIMA_DEPORTE'].fillna(0).astype(int)).dt.to_timestamp()<fecha_inicio_mes,0,df_ges['SOBREPRIMA_DEPORTE'].fillna(0)/100)+\
				df_ges['PORCENTAJE_RECARGO'].fillna(0)/100
```

- **Funcionalidad del código**: Combina dos dataframes en uno solo.
- Utiliza la función `pd.concat` para unir los dataframes `df_iaxis` y `df_ges` verticalmente, seleccionando solo las columnas especificadas en `cols_df_final`.
```python
# Unimos los dataframes de GES e Iaxis
df_final=pd.concat([df_iaxis[cols_df_final],df_ges[cols_df_final]],axis=0)
```

- **Funcionalidad**: Ajusta la prima de reaseguro sumando un recargo si se cumple una condición específica.
- Si la variable `calcula_recargos` es igual a 1, se actualiza la columna de prima de reaseguro en un DataFrame sumando el recargo correspondiente. Finalmente, el DataFrame modificado se devuelve.
```python
# Calculamos la nueva prima de reaseguro en caso de que el parametro calcula_recargos sea igual a 1. Luego retornamos
if calcula_recargos==1: df_final['PRIMA REASEGURO']=df_final['PRIMA REASEGURO SIN RECARGO']+df_final['RECARGO']
return df_final
```

# Funcion `cruce_left`

La función `cruce_left` realiza un *merge* de tipo *left* entre dos *DataFrames* de pandas. Esto significa que combina las filas de ambos *DataFrames* basándose en columnas específicas, manteniendo todas las filas del primer *DataFrame* y añadiendo las correspondientes del segundo *DataFrame* donde haya coincidencias.

### Parámetros:

- **df_1**: Primer *DataFrame* a combinar.
- **df_2**: Segundo *DataFrame* a combinar.
- **left_on**: Lista de nombres de columnas del primer *DataFrame* que se usarán para el cruce.
- **right_on**: Lista de nombres de columnas del segundo *DataFrame* que se usarán para el cruce.
- **parameters**: Objeto que contiene parámetros necesarios para cálculos específicos.
- **suffixes**: Tupla opcional que define los sufijos para las columnas en caso de que haya nombres duplicados, por defecto `('_df1', '_df2')`.
- **informa_no_cruces**: Valor binario opcional que indica si se debe informar sobre las filas del primer *DataFrame* que no encontraron coincidencias en el segundo, por defecto `1` (sí informar).
- **name**: Nombre opcional del archivo que contendrá los datos que no cruzaron, por defecto es una cadena vacía.

### Retorno:

Devuelve un *DataFrame* resultante del cruce, que contiene todas las filas del primer *DataFrame* y las filas coincidentes del segundo *DataFrame*.

- **Funcionalidad**: El código extrae configuraciones específicas para la exportación de datos desde un diccionario de parámetros.

- Se accede a un diccionario llamado `parameters` para obtener valores asociados a las claves `ruta_output`, `separador_output` y `decimal_output`.
- Estos valores se asignan a variables que definen la ruta de salida, el separador de datos y el formato decimal para la exportación.
- Las variables extraídas se utilizan posteriormente para configurar cómo se exportarán los datos.
```python
# Extraemos parametros de rutas de salida y separadores y decimales de exportacion
ruta_output: str = parameters.parameters['ruta_output']
separador_output: str = parameters.parameters['separador_output']
decimal_output: str = parameters.parameters['decimal_output']
```

- **Funcionalidad del código**: Fusiona dos DataFrames de pandas y verifica si hay registros en el primer DataFrame que no tienen correspondencia en el segundo.

- El código utiliza la función `merge` de pandas para combinar dos DataFrames, `df_1` y `df_2`, basándose en columnas específicas, y añade sufijos para diferenciar las columnas originales de cada DataFrame.
- Crea un nuevo DataFrame que contiene solo los registros de `df_1` que no tienen coincidencias en `df_2`.
- Si una variable de control indica que se debe informar y existen registros sin coincidencias, se procede a realizar una acción (como escribir en un archivo).
```python
# Realizar merge, especificando las columnas de fusión con left_on y right_on
# Los sufijos _df1 y _df2 se agregan a los nombres de las columnas para diferenciar las columnas originales de cada DataFrame.
merged_df = pd.merge(df_1, df_2, how='left', right_on=right_on, left_on=left_on, suffixes=('_df1', '_df2'), indicator='origen')
# Encuentra los registros en df_1 que no cruzaron con df_2
no_cruces = merged_df[merged_df['origen'] == 'left_only']
# Informar en archivo.txt si es necesario
if informa_no_cruces == 1 and not no_cruces.empty:
```

- **Funcionalidad del código**: Guarda registros únicos que no cumplen cierta condición en un archivo de texto tabular.

- Muestra la cantidad de registros que no cumplen la condición especificada.
- Imprime los registros únicos basados en una columna específica.
- Guarda estos registros en un archivo de texto, utilizando tabuladores como separadores.
```python
# Guarda esos registros en un archivo no_cruces.txt en formato de texto tabular (separado por tabuladores).
print(f'Una cantidad de {no_cruces.shape[0]} registros no cruzaron')
print(no_cruces[left_on].drop_duplicates(keep='first'))
no_cruces.to_csv(f'{ruta_output}{name} no cruces.txt', index=False,sep=separador_output,decimal=decimal_output)
```

- **Funcionalidad del código**: Verifica si hay filas duplicadas en un DataFrame después de realizar una operación de combinación (merge).

- Compara la longitud del DataFrame resultante de una combinación con el DataFrame original para identificar si se han añadido filas duplicadas.
```python
# Informa en caso de tener dobles cruces en el dataframe de la izquierda
if len(merged_df) > len(df_1) :
```

- **Funcionalidad**: El código identifica registros duplicados en un DataFrame específico basándose en una clave determinada.
- Se filtran los registros duplicados de `df_2` utilizando una clave específica, almacenándolos en `duplicados_df_2`.
```python
# Identificar duplicados en merged_df que no estaban en df_1
# Definimos duplicados_df_2 como los registros del df_2 que tienen duplicada su llave right_on
duplicados_df_2 =df_2[df_2.duplicated(subset=right_on, keep=False)]
```

- **Funcionalidad del código**: Elimina duplicados de un DataFrame en pandas, conservando solo la primera aparición de cada entrada duplicada.
- Utiliza el método `drop_duplicates` de pandas para crear un nuevo DataFrame sin duplicados, manteniendo la primera ocurrencia de
```python
# definimos df_1_sin_duplicados quitando todos los duplicados del df_1
df_1_sin_duplicados=df_1.drop_duplicates(keep='first')
```

- **Funcionalidad del código**: Identifica registros problemáticos comunes entre dos conjuntos de datos.
- Realiza una intersección entre dos DataFrames para encontrar registros duplicados compartidos.
```python
# Definimos duplicados_comunes como el cruce entre duplicados_df_2 y df_1_sin_duplicados
# Acá obtenemos los registros que nos estan causando problemas a traves de un cruce de tipo inner
duplicados_comunes = duplicados_df_2.merge(df_1_sin_duplicados, how='inner', left_on=right_on, right_on=left_on)
```

- **Funcionalidad**: El código exporta un conjunto de datos duplicados a un archivo de texto para su posterior revisión.
- **Explicación**: Utiliza la función `to_csv` para guardar los registros duplicados en un archivo de texto, especificando el separador y
```python
# Exportamos los registros para observarlos posteriormente
duplicados_comunes[df_2.columns].to_csv(f'{ruta_output}{name} duplicados.txt',sep=separador_output,decimal=decimal_output)
```

- **Funcionalidad**: Muestra los registros duplicados de un conjunto de datos específico.
- Utiliza una función de impresión formateada para mostrar los registros duplicados de un DataFrame, filtrando por las columnas de `df_2`.
```python
# Print informativo
print(f"Registros duplicados:\n{duplicados_comunes[df_2.columns]}")
```

- **Funcionalidad del código**: El código elimina duplicados de un DataFrame y devuelve el resultado sin una columna específica.

- Se verifica si hay duplicados adicionales en un DataFrame llamado `df_1`.
- Si no hay duplicados adicionales, se imprime un mensaje con el tamaño original de `df_1`.
- Se elimina la columna llamada `origen` del DataFrame `merged_df`.
- Finalmente, se devuelve el DataFrame `merged_df` modificado.
```python
else:
	print(f"No hay duplicados adicionales. Tamaño original de df_1: {len(df_1)}")
merged_df = merged_df.drop(['origen'], axis = 1)
return merged_df
```

# Funcion `identificador_anonimo`

La función `identificador_anonimo` toma un *DataFrame* de pandas y una lista de nombres de campos. Su propósito es **anonimizar** los datos en los campos especificados dentro del *DataFrame*. Devuelve un nuevo *DataFrame* donde los campos indicados han sido anonimizados, mientras que el resto de los datos se mantienen sin cambios.

- **Funcionalidad**: El código crea un conjunto único de combinaciones de identificadores y cuenta cuántas hay.

- Se utiliza un DataFrame para eliminar duplicados de ciertas columnas especificadas, y luego se calcula la cantidad de combinaciones únicas resultantes.
```python
# Crear un DataFrame con combinaciones únicas de los identificadores
identificadores_unicos = df[campos].drop_duplicates()
nro_ruts = len(identificadores_unicos)
```

- **Funcionalidad del código**: Genera identificadores únicos aleatorios para una serie de elementos.

- Se establece una semilla para asegurar que los resultados sean reproducibles en futuras ejecuciones.
- Se generan números aleatorios únicos dentro de un rango específico y se asignan a una variable.
- Estos números se almacenan en una estructura de datos bajo la columna 'IDENTIFICADOR'.
```python
# Generar números aleatorios para las combinaciones únicas
np.random.seed(1000)  # Fijar semilla para reproducibilidad
valores_aleatorios = np.random.choice(range(1000000, 9999999),size=nro_ruts,replace=False)
identificadores_unicos['IDENTIFICADOR'] = valores_aleatorios
```

## Verificación de Identificadores Únicos

Este proceso compara el número de identificadores únicos generados con el número de elementos únicos en el conjunto de datos original. Si ambos números coinciden, se confirma que cada elemento del conjunto de datos tiene un identificador único correspondiente. Esto asegura la integridad y unicidad de los datos.

- **Funcionalidad**: El código realiza una fusión de datos para asignar valores anonimizados a un DataFrame original si se cumple una condición específica.

- Si el número de identificadores únicos es igual a un valor esperado, se realiza una combinación del DataFrame original con otro DataFrame que contiene identificadores anonimizados, utilizando una clave común.
```python
# Hacer un merge para asignar los valores anonimizados al DataFrame original
if len(identificadores_unicos['IDENTIFICADOR'].drop_duplicates()) ==nro_ruts:
	df = df.merge(identificadores_unicos, on=campos, how='left')
```

- **Funcionalidad del código**: Muestra un mensaje de error si no se cumple una condición específica y devuelve un DataFrame.

- Si una condición no se cumple, se imprime un mensaje que indica que los identificadores únicos no fueron asignados correctamente.  
- Después de imprimir el mensaje, el código devuelve un objeto `df`, que es un DataFrame.
```python
# En caso de no cumplirse la condicion
else:
	print('Revisar los identificadores unicos. No fueron bien asignados')
return df
```

# Funcion `calculo_fechas_renovacion`

La función `calculo_fechas_renovacion` se encarga de calcular las fechas de renovación para contratos de reaseguro con prima anual renovable. 

### Parámetros:

- **`df`**: Es un *DataFrame* que contiene la información de los asegurados expuestos.
- **`campo_inicio`**: Nombre del campo que indica el inicio de vigencia de los registros.
- **`campo_fin`**: Nombre del campo que indica el fin de vigencia de los registros.
- **`campo_anulacion`**: Nombre del campo que indica la fecha de anulación de los asegurados.
- **`campo_periodicidad`**: Indica la periodicidad del contrato. Si es prima única, no se realiza ningún cálculo.
- **`periodo_cierre`**: Define el periodo de cierre.
- **`ajuste_pu`**: Es un valor binario opcional que indica si se deben realizar cambios a los asegurados de prima única. Por defecto, está establecido en 1.

### Retorno:

La función devuelve una *tupla* que contiene dos vectores:
1. Un vector con las fechas de inicio de renovación.
2. Un vector con las fechas de fin de renovación.

Estos vectores permiten identificar los periodos de renovación de los contratos de reaseguro.

- **Funcionalidad**: Crea una copia del DataFrame original para preservar su estado inicial.
- **Explicación**: Se genera un nuevo DataFrame llamado `df_aux` que es una copia exacta del DataFrame `df`, permitiendo realizar modificaciones en `df_aux`
```python
# Crea df auxiliar para no realizar cambios al df original
df_aux=df.copy()
```

- **Funcionalidad del código**: Extrae el mes y el año de un valor numérico que representa un periodo de cierre.

- Calcula el mes tomando el residuo de la división del valor `periodo_cierre` entre 100. Calcula el año dividiendo `periodo_cierre` entre 100 y convirtiendo el resultado a un número entero.
```python
# Crea variables cierre_month y cierre_year
cierre_month=periodo_cierre%100
cierre_year=int(periodo_cierre/100)
```

- **Funcionalidad del código**: Calcula la fecha de inicio de renovación ajustando el año, mes y día según ciertas condiciones.

- Se ajusta el año restando uno si el mes de inicio es posterior al mes de cierre o si la fecha de anulación es anterior a la fecha de inicio, siempre que la fecha de anulación no sea nula.
- El mes se establece directamente a partir del mes de la fecha de inicio.
- El día se ajusta a 28 si es 29 de febrero y el año no es bisiesto; de lo contrario, se mantiene el día original.
- La fecha de inicio de renovación se calcula como la máxima entre la fecha ajustada y la fecha de inicio original.
```python
# Defino los campos de dia, mes y año para posteriormente calcular la fecha de inicio de renovacion
df_aux['year']=cierre_year-np.where((df_aux[campo_inicio].dt.month>cierre_month)|((df_aux[campo_anulacion].dt.month*100+df_aux[campo_anulacion].dt.day<df_aux[campo_inicio].dt.month*100+df_aux[campo_inicio].dt.day)&(~df_aux[campo_anulacion].isnull())),1,0)
df_aux['month']=df_aux[campo_inicio].dt.month
df_aux['day']=np.where((df_aux[campo_inicio].dt.day==29)&(df_aux['month']==2)&(df_aux['year']%4>0),28,df_aux[campo_inicio].dt.day)
df_aux['INICIO RENOVACION']=np.maximum(pd.to_datetime(df_aux[['year','month','day']]),df_aux[campo_inicio])
```

- **Funcionalidad del código**: Calcula la fecha de fin de renovación para un conjunto de datos, ajustando para años no bisiestos.

- Se define el año de fin de renovación sumando uno al año de inicio de renovación.
- El día se ajusta a 28 si el día de inicio es 29 de febrero y el año de fin no es bisiesto.
- La fecha de fin de renovación se determina como la menor entre la fecha calculada y un campo de fecha de fin existente, si está presente.
```python
# Defino los campos de dia, mes y año para posteriormente calcular la fecha de fin de renovacion
df_aux['year']=df_aux['INICIO RENOVACION'].dt.year+1
df_aux['day']=np.where((df_aux[campo_inicio].dt.day==29)&(df_aux['month']==2)&(df_aux['year']%4>0),28,df_aux[campo_inicio].dt.day)
df_aux['FIN RENOVACION']=np.where(df_aux[campo_fin].isnull(),pd.to_datetime(df_aux[['year','month','day']]),np.minimum(pd.to_datetime(df_aux[['year','month','day']]),df_aux[campo_fin]))
```

- **Funcionalidad del código**: Ajusta las fechas de inicio y fin de una serie de datos basándose en una condición específica.

- Si la variable `ajuste_pu` es igual a 1, se evalúa la periodicidad de cada fila en el DataFrame `df_aux`.
- Para las filas donde la periodicidad es 0, se asigna el valor de `campo_inicio` a `series_inicio` y `campo_fin` a `series_fin`.
- En caso contrario, se asignan los valores de 'INICIO RENOVACION' y 'FIN RENOVACION' a `series_inicio` y `series_fin`, respectivamente.
- Si `ajuste_pu` no es igual a 1, `series_inicio` y `series_fin` se asignan directamente con los valores de 'INICIO RENOVACION' y 'FIN RENOVACION'.
- Utiliza la función `np.where` de NumPy para realizar asignaciones condicionales de manera eficiente.
```python
# Ajusto en caso de primas unicas
if ajuste_pu==1:
	series_inicio=np.where(df_aux[campo_periodicidad]==0,df_aux[campo_inicio],df_aux['INICIO RENOVACION'])
	series_fin=np.where(df_aux[campo_periodicidad]==0,df_aux[campo_fin],df_aux['FIN RENOVACION'])
else:
	series_inicio=df_aux['INICIO RENOVACION']
	series_fin=df_aux['FIN RENOVACION']
```

- **Funcionalidad del código**: El código devuelve dos series relacionadas con el inicio y fin de una renovación.
- **Explicación**: Se retornan dos variables, `series_inicio` y `series_fin`, que representan las series de datos correspondientes al inicio y fin de un proceso
```python
# Devuelvo dos series, la de inicio y fin de renovacion
return series_inicio,series_fin
```

# Funcion `automatizacion_querys`

La función `automatizacion_querys` ejecuta consultas para extraer datos necesarios para un proceso específico. Recibe un parámetro llamado `files`, que es una instancia de `Parameter_Loader`. Este parámetro contiene la información sobre el archivo de consultas que se debe utilizar durante el proceso.

- **Funcionalidad del código**: Inicializa un objeto para cargar parámetros desde un archivo Excel relacionado con consultas.

- Se crea una instancia de la clase `Parameter_Loader` utilizando un archivo Excel especificado en un diccionario de parámetros, permitiendo la gestión de parámetros asociados a consultas.
```python
# Creamos la varible querys que es del tipo Parameter_Loader. Acá guardaremos todos los parametros asociados al proceso de querys
querys: Parameter_Loader = Parameter_Loader(excel_file=files.parameters['archivo_querys'], open_wb=True, ruta_extensa='')
```

- **Funcionalidad del código**: El código carga parámetros desde un archivo Excel y los almacena en variables para su posterior uso.

1. Se obtiene una ruta de archivo desde un objeto llamado `files` y se almacena en una variable de tipo cadena.
2. Se extraen dos valores enteros, `periodo_inicio` y `periodo_fin`, utilizando un método que accede a referencias específicas.
3. Se cierra un libro de trabajo (workbook) asociado al objeto `querys` para liberar recursos.
4. Se carga una tabla desde una hoja de Excel llamada 'Split Querys' en un DataFrame de pandas, reemplazando valores nulos con cadenas vacías.
5. Se carga otra tabla desde una hoja de Excel llamada 'Diccionario Querys' en otro DataFrame de pandas, también reemplazando valores nulos con cadenas vacías.
6. Los DataFrames resultantes contienen los datos necesarios para realizar operaciones posteriores en el programa.
```python
# Cargamos parametros dentro de la variables querys
ruta_extensa: str = files.ruta_extensa
periodo_inicio: int = querys.get_reference(reference='periodo_inicio')
periodo_fin: int = querys.get_reference(reference='periodo_fin')
querys.wb.close()
parametros_split: pd.DataFrame = querys.get_table_xlsx(sheet_name = 'Split Querys').replace(np.nan, '', regex=True)
parametros_querys: pd.DataFrame = querys.get_table_xlsx(sheet_name = 'Diccionario Querys').replace(np.nan, '', regex=True)
```

- **Funcionalidad**: Convierte un DataFrame en un diccionario donde las claves son los valores de una columna específica.
- Utiliza un DataFrame llamado `parametros_querys`, establece la columna 'QUERY' como índice y convierte el resultado en un diccionario.
```python
# Creamos diccionario_querys
diccionario_querys: dict[Hashable, Any]=parametros_querys.set_index('QUERY').to_dict()
```

## Ejecución de Consultas Activas

Este proceso recorre una lista de consultas (querys) que han sido marcadas como activas para su ejecución. Cada consulta activa se ejecuta de manera secuencial. Esto permite automatizar la ejecución de múltiples consultas sin intervención manual.

- **Funcionalidad del código**: Itera sobre una lista de consultas almacenadas en un diccionario para realizar operaciones con cada consulta.

- El código utiliza un bucle `for` para recorrer cada elemento en la lista asociada a la clave `'QUERY'` dentro del diccionario `
```python
for consulta in parametros_querys['QUERY']:
```

- **Funcionalidad**: El código verifica si una consulta debe ejecutarse y, de ser así, la ejecuta con los parámetros proporcionados.

- Se accede a un diccionario para determinar si la consulta especificada está marcada para ejecución.
- Si el valor asociado es `1`, se llama a una función para ejecutar la consulta con los parámetros dados.
- La función `ejecuta_query` utiliza varios parámetros, incluyendo el periodo de tiempo y configuraciones adicionales.
```python
# obtiene del diccionario si la query debe ejecutarse o no
aplica=diccionario_querys['APLICA'][consulta]
if aplica==1:
	ejecuta_query(consulta,periodo_inicio,periodo_fin,diccionario_querys,parametros_split, ruta_extensa)
```

# Funcion `ejecuta_query`

La función `ejecuta_query` se encarga de ejecutar una consulta específica. 

### Parámetros:

- **`consulta`**: Es el nombre de la consulta que se desea ejecutar.
- **`periodo_inicio`** y **`periodo_fin`**: Definen el rango de tiempo para la consulta, si es necesario.
- **`diccionario_querys`**: Un diccionario que contiene información relevante sobre todas las consultas disponibles.
- **`parametros_split`**: Un DataFrame que proporciona información adicional cuando una consulta necesita ser dividida o detallada.
- **`ruta_extensa`**: Se utiliza para agregar detalles a la ruta de exportación de los resultados.
- **`name_file`**: Es opcional y permite especificar un nombre para el archivo de salida; si no se proporciona, su valor por defecto es `None`.

Esta función no devuelve ningún valor, ya que su propósito principal es ejecutar la consulta y posiblemente exportar los resultados.

- **Funcionalidad del código**: Configura y calcula parámetros de consulta basados en un diccionario de configuraciones y ajusta fechas de inicio y fin de un periodo.

1. Registra el tiempo de inicio de la ejecución.
2. Muestra en pantalla el nombre de la consulta que se está realizando.
3. Extrae los nombres de las columnas de la consulta desde un diccionario y los separa por comas.
4. Obtiene las columnas de fechas si están definidas, separándolas por comas.
5. Recupera el sistema asociado a la consulta desde el diccionario.
6. Obtiene el desfase en meses para ajustar las fechas.
7. Determina el tipo de exportación requerido para la consulta.
8. Define la carpeta y subcarpeta donde se almacenarán los resultados.
9. Identifica el tipo de cálculo que se realizará.
10. Calcula la fecha de inicio del periodo, ajustando por el desfase de meses.
11. Ajusta la fecha de inicio al primer día del mes correspondiente.
12. Calcula la fecha de fin del periodo, también ajustando por el desfase de meses.
13. Ajusta la fecha de fin al último día del mes correspondiente.
14. Convierte la fecha de fin en un formato de periodo (año y mes concatenados).
15. Convierte la fecha de inicio en el mismo formato de periodo.
16. El código está preparado para manejar configuraciones de consultas y ajustar periodos de tiempo según los parámetros definidos.
```python
# Tiempo inicial
start_time = time.time()
# Mostramos en pantalla que query estamos realizando
print('Realizando consulta {}'.format(consulta))
# Calculos sobre el diccionario de querys
columnas=diccionario_querys['CAMPOS QUERY'][consulta].split(',')
cols_date = diccionario_querys['CAMPOS FECHAS'][consulta].split(',') if diccionario_querys['CAMPOS FECHAS'][consulta] else []
sistema=diccionario_querys['SISTEMA'][consulta]
desfase_meses=diccionario_querys['DESFASE'][consulta]
tipo_exportar=diccionario_querys['TIPO EXPORTAR'][consulta]
carpeta=diccionario_querys['CARPETA'][consulta]
subcarpeta=diccionario_querys['SUBCARPETA'][consulta]
tipo_calculo=diccionario_querys['TIPO CALCULO'][consulta]
# Calculo de fechas y periodos
fecha_inicio=datetime.datetime(int(periodo_inicio/100),periodo_inicio%100,1)
fecha_inicio=fecha_inicio-pd.offsets.MonthEnd(desfase_meses+1)+datetime.timedelta(days=1)
fecha_fin=datetime.datetime(int(periodo_fin/100),periodo_fin%100,1)
fecha_fin=fecha_fin-pd.offsets.MonthEnd(desfase_meses)
periodo_fin=fecha_fin.year*100+fecha_fin.month
periodo_inicio=fecha_inicio.year*100+fecha_inicio.month
```

- **Funcionalidad**: El código construye una ruta de exportación basada en varias variables de entrada.

- Si la variable `carpeta` tiene un valor, la ruta incluye esta carpeta como parte del camino; de lo contrario, la ruta se construye sin ella. La ruta se forma concatenando varias partes predefinidas, como `ruta_extensa`, `tipo_calculo`, y `subcarpeta`.
```python
# Calculo de rutas de exportacion
if carpeta: ruta_exportar_query=f'{ruta_extensa}1 Input\\{tipo_calculo}\\{subcarpeta}\\{carpeta}\\'
else: ruta_exportar_query=f'{ruta_extensa}1 Input\\{tipo_calculo}\\{subcarpeta}\\'
```

- **Funcionalidad del código**: Lee y modifica una consulta SQL desde un archivo, luego establece una conexión a una base de datos Oracle según el sistema especificado.

- El código abre un archivo SQL, reemplaza ciertas palabras clave con valores específicos de fecha y periodo, y almacena el contenido modificado en una variable.
- Dependiendo del valor de la variable `sistema`, se conecta a una base de datos Oracle utilizando credenciales y detalles de conexión específicos.
- Utiliza la biblioteca `cx_Oracle` para manejar la conexión a la base de datos, asegurando que la codificación sea UTF-8.
```python
# Traemos el archivo de la query, que viene en txt y le realizamos cambios de acuerdo al periodo de ejecucion de la query
# No todas las querys tienen dependencia de los periodos de inicio y fin
with open(ruta_extensa+'0 Querys Automaticas\\'+consulta+'.sql', 'r') as query_txt: query = query_txt.read().replace('\n',' ').replace('fecha_inicio',str(fecha_inicio)[0:10]).replace('fecha_fin',str(fecha_fin)[0:10]).replace('periodo_fin',str(periodo_fin)[0:10]).replace('año_proceso',str(fecha_fin.year)).replace('mes_proceso',str(fecha_fin.month))
# Conexion sql
if sistema=='GES': connection = cx_Oracle.connect(user="USU_BCATALDO", password="SAmu3l.20204*",dsn="prod_zs.santanderseguros.cl.bsch:1526/gesvida",encoding="UTF-8")
if sistema=='IAXIS':connection = cx_Oracle.connect(user="USR_ZS_BCATALDO", password="SAturn0.20204*",dsn="zsiaxisbd.santanderseguros.cl.bsch:1521/praxis",encoding="UTF-8")
```

- **Funcionalidad del código**: Ejecuta una consulta en una base de datos y convierte los resultados en un DataFrame de pandas.

1. Se crea un cursor a partir de una conexión de base de datos existente.
2. Se imprime un mensaje para indicar que se está ejecutando una consulta.
3. El cursor ejecuta la consulta SQL proporcionada.
4. Se imprime un mensaje para señalar que se están obteniendo los resultados de la consulta.
5. Los resultados de la consulta se recuperan y almacenan en una variable.
6. Los resultados se convierten en un DataFrame de pandas para facilitar su manipulación y análisis.
```python
# Lectura de datos y pasamos a dataframe, junto con algunos print que informan en que paso estamos
cursor = connection.cursor()
print('Ejecutando query')
cursor.execute(query)
print('Pegando resultados de la query')
resultado_query = cursor.fetchall()
df=pd.DataFrame(list(resultado_query))
```

- **Funcionalidad del código**: Verifica si un DataFrame está vacío y emite una advertencia si no contiene registros.

- El código comprueba si el DataFrame `df` no tiene registros utilizando la propiedad `empty`.
- Si el DataFrame está vacío, imprime un mensaje de advertencia que incluye el contenido de la variable `consulta`.
- Después de imprimir el mensaje, el flujo del programa se detiene con una instrucción `return`.
```python
# Acá realizamos warning en caso de que la consulta no arroje ningun registro
if df.empty:
	print(f'La consulta - {consulta} - arrojó 0 registros como resultado. REVISAR!')
	return
```

- **Funcionalidad del código**: Cambia los nombres de las columnas de un DataFrame según un diccionario de consultas.
- Asigna nuevos nombres a las columnas de un DataFrame utilizando una lista llamada `columnas`.
```python
# Asignamos nombres de columnas de acuerdo a lo indicado en el diccionario de querys
df.columns=columnas
```

- **Funcionalidad del código**: Convierte columnas de un DataFrame a formato de fecha y determina el nombre de un archivo de salida basado en ciertas condiciones.

1. Verifica si hay columnas especificadas para conversión a fechas.
2. Itera sobre cada columna en la lista de columnas de fecha.
3. Convierte las columnas de tipo `object` a formato de fecha usando `pd.to_datetime`.
4. Imprime un mensaje indicando el número de registros en el DataFrame.
5. Define la extensión del nombre del archivo de salida según el tipo de exportación.
6. Si el tipo es 'historico', la extensión es '.txt'.
7. Si el tipo es 'periodo' y las fechas de inicio y fin son iguales, la extensión incluye la fecha.
8. Si el tipo es 'periodo' y las fechas de inicio y fin son diferentes, la extensión incluye el rango de fechas.
9. Si el tipo es 'fecha', la extensión incluye la fecha actual.
```python
# Conversión a fechas de las columnas
if len(cols_date)>0:
	for col in cols_date:
		if df[col].dtype==object: df[col]=pd.to_datetime(df[col],format='%Y-%m-%d', errors='coerce')
# Exportamos la data
print('Exportando datos de la query. La consulta tiene {} registros'.format(df.shape[0]))
# Acá definimos la terminacion del nombnre que tendra el archivo de acuerdo a diversas caracteristicas
if tipo_exportar=='historico':terminacion_archivo='.txt'
if (tipo_exportar=='periodo')&(periodo_fin==periodo_inicio):terminacion_archivo=' '+str(periodo_fin)+'.txt'
if (tipo_exportar=='periodo')&(periodo_fin!=periodo_inicio):terminacion_archivo=' '+str(periodo_inicio)+'-'+str(periodo_fin)+'.txt'
if tipo_exportar=='fecha':terminacion_archivo=' ('+str(datetime.datetime.now())[0:10]+').txt'
nombre_archivo_salida=consulta+terminacion_archivo
```

- **Funcionalidad del código**: Guarda un DataFrame en un archivo CSV en una carpeta específica si se indica que debe hacerlo.

- Si la variable `carpeta` es verdadera, se crea una carpeta en la ruta especificada si no existe.
- El DataFrame `df` se guarda como un archivo CSV en la ruta especificada, utilizando un punto y coma como separador y un formato de fecha específico.
- El archivo CSV se guarda con codificación UTF-8 y sin incluir el índice del DataFrame.
```python
# La variable carpeta indica si debemos guardar la query ejecutada directamente o si debemos hacerle split en distintos dataframes
if carpeta:
	Path(ruta_exportar_query).mkdir(parents=True, exist_ok=True)
	df.to_csv(ruta_exportar_query+nombre_archivo_salida,sep=';',decimal='.',encoding='UTF-8',date_format='%d-%m-%Y',index=False)
```

- **Funcionalidad del código**: Filtra una tabla para verificar si una consulta específica debe ser particionada.
- Utiliza un filtro en un DataFrame llamado `parametros_split` para seleccionar filas donde la columna 'QUERY' coincide con el valor de `consulta`, almacenando el
```python
# Revisamos en la tabla de splits si la query conrresponde que se particione o no
parametros_split_filter=parametros_split[parametros_split['QUERY']==consulta]
```

- **Funcionalidad**: El código verifica si un conjunto de parámetros está vacío y, si no lo está, llama a una función para dividir consultas y exportarlas.
- Si la variable `parametros_split_filter` no está vacía, se ejecuta la función `split_querys`
```python
# En caso de aplicar el split, llamamos a la funcion split_querys
if not parametros_split_filter.empty : split_querys(df,parametros_split_filter,ruta_exportar_query,terminacion_archivo,sistema)
```

- **Funcionalidad**: Calcula y muestra el tiempo total de ejecución de un proceso en minutos.

- Resta el tiempo de inicio del tiempo actual, convierte el resultado a minutos, lo redondea a dos decimales y lo imprime.
```python
# Mido tiempo de ejecucion e imprimo en pantalla
total_time = round((time.time()-start_time)/60, 2)
print('El tiempo total de ejecución fue de %s minutos' % total_time)
```

# Funcion `split_querys`

La función `split_querys` se encarga de dividir una consulta original, representada como un DataFrame, en partes más pequeñas si es necesario. Utiliza información adicional proporcionada en otro DataFrame para determinar cómo realizar esta división. Luego, exporta los resultados a una ubicación especificada, utilizando un formato de archivo determinado por la terminación proporcionada. Además, tiene en cuenta el sistema de administración de bases de datos que se está utilizando, ya sea GES o IAXIS.

- **Funcionalidad**: Crea una copia de un DataFrame para preservar el original.
- Se utiliza el método `copy()` para duplicar el DataFrame original y asignarlo a una nueva variable llamada `df_aux`.
```python
# Creamos df auxiliar para no hacer cambios al df original
df_aux=df.copy()
```

- **Funcionalidad del código**: Itera sobre cada fila de un DataFrame llamado `parametros_split_filter`.
- Utiliza un bucle para acceder a cada fila del DataFrame, permitiendo realizar operaciones con los datos de cada fila individualmente.
```python
# Iteracion sobre todas las particiones que se deben realizar en el df
for index,row in parametros_split_filter.iterrows():
```

- **Funcionalidad del código**: Extrae y transforma datos de un diccionario `row` en variables específicas para su posterior uso.

1. La variable `contrato` se asigna con el valor asociado a la clave `'CONTRATO'` del diccionario `row`.
2. La variable `productos` se convierte en una lista de enteros a partir de una cadena separada por guiones de la clave `'PRODUCTOS CONTRATO'`, o se asigna una cadena vacía si no hay datos.
3. La variable `polizas` se transforma en una lista de enteros de manera similar a `productos`, utilizando la clave `'POLIZAS CONTRATO'`, o se asigna una cadena vacía si no hay datos.
4. La variable `tipo_condicion` se asigna con el valor de la clave `'TIPO CONDICION'` del diccionario `row`.
5. La variable `aplica_split` se asigna con el valor de la clave `'APLICA'` del diccionario `row`.
```python
# Definimos variables de la particion que se encuentran en la matriz de particiones (parametros_split_filter)
contrato=row['CONTRATO']
productos=list(map(int,row['PRODUCTOS CONTRATO'].split('-'))) if row['PRODUCTOS CONTRATO'] else ''
polizas=list(map(int,row['POLIZAS CONTRATO'].split('-'))) if row['POLIZAS CONTRATO'] else ''
tipo_condicion=row['TIPO CONDICION']
aplica_split=row['APLICA']
```

- **Funcionalidad del código**: Genera dos series booleanas para determinar si cada fila de un DataFrame cumple con ciertas condiciones basadas en productos y pólizas.

- Se crean dos series de booleanos: una para productos y otra para pólizas. Cada serie evalúa si los elementos de las columnas correspondientes están dentro de listas específicas, dependiendo de un tipo de condición dado.
```python
# Creamos dos series de booleanos que indican si aplica la condicion de producto o poliza especificada para cada particion
cond_prods=pd.Series(np.full(df_aux.shape[0],True)) if not productos else df_aux['PRODUCTO'].isin(productos) if tipo_condicion==1 else ~df_aux['PRODUCTO'].isin(productos)
cond_pols=pd.Series(np.full(df_aux.shape[0],True)) if not polizas else df_aux['POLIZA'].isin(polizas) if tipo_condicion==1 else ~df_aux['POLIZA'].isin(polizas)
```

- **Funcionalidad del código**: Filtra un DataFrame existente para crear uno nuevo que solo contenga registros que cumplan con ciertas condiciones.

- Utiliza dos condiciones predefinidas para seleccionar filas de un DataFrame y crea una copia de las filas que cumplen ambas condiciones.
```python
# Creamos df_export a partir de los registros del df_aux que cumplen con las condiciones de las dos series previamente creadas
df_export=df_aux[cond_prods&cond_pols].copy()
```

- **Funcionalidad del código**: Filtra un DataFrame para excluir registros que ya han sido exportados a otra partición.

- Utiliza el método `difference` para identificar y mantener solo los registros en `df_aux` cuyos índices no están presentes en `df_export`, y luego
```python
# Redefinimos df_aux como los registros que no se fueron hacia esa particion
df_aux=df_aux.loc[df_aux.index.difference(df_export.index)].reset_index(drop=True)
```

- **Funcionalidad**: Determina la terminación de un archivo según el sistema especificado.
- Si el sistema es `'GES'`, la variable `terminacion_ges` se establece como `' GES'`; de lo contrario, se establece como una cadena vacía.
```python
# Revisamos si la terminacion del archivo es de tipo GES o IAXIS
terminacion_ges=' GES' if sistema=='GES' else ''
```

- **Funcionalidad del código**: Crea un directorio y exporta un DataFrame a un archivo CSV si se cumplen ciertas condiciones.

- Se asegura de que exista un directorio específico utilizando la ruta proporcionada, creando cualquier directorio intermedio si es necesario. Luego, verifica si el DataFrame no está vacío y si una condición específica es verdadera, para exportarlo a un archivo CSV con un formato y codificación definidos.
```python
# Creamos la ruta a exportar y luego exportamos
Path(f'{ruta_exportar_query}{contrato}\\').mkdir(parents=True, exist_ok=True)
if (not df_export.empty)&(aplica_split==1): df_export.to_csv(f'{ruta_exportar_query}{contrato}\\Expuestos {contrato}{terminacion_ges}{terminacion_archivo}',sep=';',decimal='.',encoding='UTF-8',date_format='%d-%m-%Y',index=False)
```