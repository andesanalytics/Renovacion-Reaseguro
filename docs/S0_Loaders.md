# Modulo S0_Loaders

# Funcion ` Parameter_Loader`

La clase `Parameter_Loader` está diseñada para **cargar parámetros y tablas** desde archivos en formato Excel (.xlsx) y archivos de texto como .txt o .csv.

# Funcion `__init__`

La función `__init__` es el constructor de la clase `Parameter_Loader`. Su propósito es inicializar una nueva instancia de esta clase con los siguientes parámetros:

- **`excel_file`**: Es un parámetro obligatorio que representa la ruta del archivo Excel (.xlsx) que se utilizará para cargar parámetros.
- **`open_wb`**: Es un parámetro opcional de tipo booleano. Indica si el archivo Excel debe abrirse utilizando la biblioteca `openpyxl` al crear la instancia. Su valor predeterminado es `False`.
- **`ruta_extensa`**: Es un parámetro opcional que permite almacenar rutas de archivos de texto o para uso genérico. Su valor predeterminado es una cadena vacía (`''`).

- **Funcionalidad**: El código define un atributo para almacenar el nombre de un archivo Excel.

- **Explicación**: Se crea un atributo llamado `excel_file` de tipo cadena de texto para guardar el nombre del archivo Excel proporcionado.
```python
# Definimos atributo excel_file
self.excel_file: str = excel_file
```

- **Funcionalidad del código**: Inicializa un objeto con un diccionario para almacenamiento en caché, una ruta de archivo y opcionalmente carga un archivo Excel.

- Se define un diccionario para almacenar datos que ya han sido procesados, lo que ayuda a evitar cálculos repetidos.
- Se establece una variable para almacenar una ruta de archivo, útil para operaciones que requieren una ubicación de archivo específica.
- Si se especifica, se carga un archivo Excel utilizando la biblioteca `openpyxl`, lo que permite manipular el contenido del archivo dentro del programa.
```python
# Diccionario para almacenar tablas o valores que ya han sido cargados (cache).
self.parameters: dict[str, Any] = {}
# Ruta adicional o extensiva, en caso de necesitar guardar/elaborar alguna ruta completa.
self.ruta_extensa: str = ruta_extensa
# Si se indica open_wb como True, se abre el archivo Excel con openpyxl y se guarda el objeto Workbook.
if open_wb:
	self.wb: openpyxl.Workbook = openpyxl.load_workbook(excel_file)
```

# Funcion `get_table_xlsx`

La función `get_table_xlsx` devuelve un *DataFrame* que contiene los datos de una hoja específica de un archivo Excel. Si la hoja ya ha sido cargada previamente, reutiliza la versión almacenada en memoria para optimizar el rendimiento. El nombre de la hoja se proporciona como argumento a la función.

- **Funcionalidad del código**: Carga una hoja de Excel en un DataFrame solo si no ha sido cargada previamente y la almacena para su reutilización.

- Verifica si el nombre de la hoja de Excel ya está presente en un diccionario llamado `parameters`.
- Si la hoja no está cargada, utiliza `pandas` para leer la hoja de Excel y la almacena en el diccionario bajo el nombre de la hoja.
- Imprime un mensaje indicando que la hoja ha sido cargada exitosamente.
- Devuelve el DataFrame correspondiente a la hoja solicitada.
```python
# Carga la hoja de Excel solo si no ha sido cargada antes.
if sheet_name not in self.parameters:
	self.parameters[sheet_name] = pd.read_excel(self.excel_file, sheet_name=sheet_name)
	print(f'Se ha cargado la tabla "{sheet_name}" del archivo "{self.excel_file}".')
# Retorna el DataFrame correspondiente.
return self.parameters[sheet_name]
```

# Funcion `get_table_txt`

La función `get_table_txt` carga un archivo de texto (como CSV o TXT) en un DataFrame. Si el archivo ya ha sido cargado previamente, reutiliza la versión almacenada en memoria para optimizar el rendimiento. Los parámetros que recibe son:

- `file_path`: la ruta del archivo de texto que se desea cargar.
- `decimal`: el carácter que se utiliza como separador decimal en el archivo.
- `separador`: el carácter que separa los campos dentro del archivo.
- `campos_fecha`: columnas que deben ser tratadas como fechas, este parámetro es opcional.

El resultado es un DataFrame que contiene los datos del archivo especificado.

- **Funcionalidad del código**: Carga un archivo de texto en un DataFrame de pandas solo si no ha sido cargado previamente, y lo retorna.

- Verifica si el archivo ya ha sido cargado revisando si su ruta está en `self.parameters`.
- Si el archivo no está cargado, procede a leerlo usando `pandas.read_csv`.
- Define el archivo a cargar mediante la variable `file_path`.
- Especifica el carácter decimal y el separador de columnas a través de las variables `decimal` y `separador`.
- Configura el formato de fecha como 'día-mes-año' para los campos especificados en `campos_fecha`.
- Utiliza la codificación 'latin-1' para leer el archivo.
- Establece `low_memory=False` para manejar archivos grandes sin problemas de memoria.
- Al cargar el archivo, almacena el DataFrame resultante en `self.parameters` usando `file_path` como clave.
- Imprime un mensaje confirmando que el archivo ha sido cargado.
- Finalmente, retorna el DataFrame asociado al archivo cargado.
```python
# Carga la tabla de texto solo si no ha sido cargada antes.
if file_path not in self.parameters:
	self.parameters[file_path]: pd.DataFrame = pd.read_csv(
		file_path,
		decimal=decimal,
		sep=separador,
		date_format='%d-%m-%Y',
		parse_dates=campos_fecha,
		encoding='latin-1',
		low_memory=False  # Evita problemas con archivos muy grandes
	)
	print(f'Se ha cargado la tabla desde el archivo "{file_path}".')
# Retorna el DataFrame correspondiente.
return self.parameters[file_path]
```

# Funcion `get_reference`

La función `get_reference` busca y devuelve el valor de una celda en un archivo Excel. Esta celda está identificada por un nombre definido en el libro de Excel. Utiliza la biblioteca `openpyxl` para acceder a la propiedad `defined_names`. Si el valor de la celda ya ha sido cargado previamente, no lo vuelve a cargar.

- **Funcionalidad del código**: Carga un valor de una celda de Excel en un diccionario si no está previamente almacenado.

- Verifica si una referencia específica no está presente en el diccionario `self.parameters`.
- Si la referencia no está almacenada, busca su ubicación en el archivo Excel.
- Utiliza `self.wb.defined_names` para encontrar las coordenadas (fila y columna) de la referencia.
- Accede al valor de la celda en la hoja de cálculo usando las coordenadas obtenidas.
- Almacena el valor de la celda en el diccionario `self.parameters` bajo la clave de la referencia.
- Imprime un mensaje indicando que la variable ha sido cargada desde el archivo Excel.
- El archivo Excel del que se carga la variable está especificado por `self.excel_file`.
```python
# ? Buscamos la referencia en caso de no estar almacenada
if reference not in self.parameters:
	# Toma la dirección (fila, columna) de la referencia y obtiene el valor de la celda.
	self.parameters[reference] = self.wb[
		next(self.wb.defined_names[reference].destinations)[0]
	][
		next(self.wb.defined_names[reference].destinations)[1]
	].value  # type: ignore
	print(f'Se ha cargado la variable "{reference}" del archivo "{self.excel_file}".')
```

- **Funcionalidad**: El código devuelve un valor específico de un diccionario o estructura similar utilizando una clave proporcionada.
- Utiliza la clave `reference` para acceder y retornar el valor asociado en `self.parameters`.
```python
# ? Retorna el valor correspondiente.
return self.parameters[reference]
```