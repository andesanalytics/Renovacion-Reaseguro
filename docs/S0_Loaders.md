# Modulo S0_Loaders

# Funcion ` Parameter_Loader`

La función `Parameter_Loader` se encarga de **cargar parámetros y tablas** desde archivos de diferentes formatos, específicamente **Excel** (.xlsx) y **archivos de texto** como .txt y .csv.

# Funcion `__init__`

La función `__init__` es el **constructor** de la clase `Parameter_Loader`. Su propósito es inicializar una nueva instancia de la clase con los siguientes parámetros:

- **`excel_file`**: Especifica la ruta del archivo Excel que se utilizará para cargar parámetros.
- **`open_wb`**: Indica si se debe abrir el archivo Excel con la biblioteca `openpyxl` al crear la instancia.
- **`ruta_extensa`**:

Se **define un atributo** llamado `excel_file` que es de tipo **cadena de texto** (`str`). Este atributo almacenará el archivo de Excel proporcionado.
```python
self.excel_file: str = excel_file
```

Se define un **diccionario** llamado `parameters`, que se utiliza para **almacenar tablas o valores** que ya han sido cargados, actuando como un **cache**. Este diccionario puede contener claves de tipo `str` y valores de cualquier tipo (`Any`).
```python
self.parameters: dict[str, Any] = {}
```

La instrucción `self.ruta_extensa: str = ruta_extensa` asigna un valor a la variable `ruta_extensa`, que se utiliza para almacenar una **ruta adicional o extensiva**. Esto es útil cuando se necesita guardar o elaborar una **ruta completa** en el contexto del programa.
```python
self.ruta_extensa: str = ruta_extensa
```

Si `open_wb` es **True**, se abre un archivo Excel utilizando **openpyxl** y se guarda el objeto **Workbook** en `self.wb`.
```python
if open_wb:
self.wb: openpyxl.Workbook = openpyxl.load_workbook(excel_file)
```

# Funcion `get_table_xlsx`

La función `get_table_xlsx` **retorna un DataFrame** que contiene los datos de una hoja específica de un archivo Excel. Si los datos ya están cargados en memoria, se reutilizan en lugar de volver a cargarlos. 

- **Parámetro**: `sheet_name` - Nombre de la hoja en el archivo Excel.
- **Retorno**: Un DataFrame con el contenido de la hoja indicada.

El código verifica si el nombre de una hoja de Excel (`sheet_name`) ya ha sido cargado en `self.parameters`. Si no ha sido cargada, se importa la hoja desde el archivo Excel especificado y se almacena en `self.parameters`. Además, se imprime un mensaje confirmando la carga de la tabla. 

**Importante:** Solo se carga la hoja si no ha sido cargada previamente.
```python
if sheet_name not in self.parameters:
self.parameters[sheet_name] = pd.read_excel(self.excel_file, sheet_name=sheet_name)
print(f'Se ha cargado la tabla "{sheet_name}" del archivo "{self.excel_file}".')
```

La instrucción `return self.parameters[sheet_name]` **devuelve** un **DataFrame** asociado al nombre de la hoja especificada por `sheet_name`. Esto permite acceder a los datos almacenados en esa hoja de manera sencilla.
```python
return self.parameters[sheet_name]
```

# Funcion `get_table_txt`

La función **get_table_txt** carga un **DataFrame** desde un archivo de texto, como un archivo CSV o TXT. Si el **DataFrame** ya está en memoria, lo reutiliza. 

**Parámetros**:
- `file_path`: Ruta del archivo a cargar.
- `decimal`: Carácter que indica el separador decimal.
- `separador`: Separador de campos en el archivo.
- `campos_fecha`: Columnas a interpretar como fechas (op

El código verifica si un archivo específico (`file_path`) ya ha sido cargado. Si no lo ha sido, carga los datos del archivo en un **DataFrame** de pandas utilizando varias configuraciones, como el formato de fecha y la codificación. Luego, imprime un mensaje confirmando que la tabla ha sido cargada. 

**Importante**: Solo se carga el archivo si no se ha hecho previamente.
```python
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
```

La instrucción `return self.parameters[file_path]` **devuelve** un **DataFrame** que está asociado a la ruta de archivo especificada por `file_path`. Esto permite acceder a los datos almacenados en esa ubicación.
```python
return self.parameters[file_path]
```

# Funcion `get_reference`

La función `get_reference` obtiene el **valor de una celda** en un archivo Excel, utilizando un **nombre definido** que apunta a esa celda. Recibe como parámetro el nombre definido y devuelve el valor correspondiente de la celda.

La instrucción `if reference not in self.parameters:` verifica si una **referencia** no ha sido previamente cargada en **parameters**. Si no está presente, se procede a cargarla. Esto asegura que la referencia se cargue solo una vez.
```python
if reference not in self.parameters:
```

El código asigna un valor a `self.parameters[reference]` utilizando la dirección de una celda definida en un archivo de Excel. 

1. **Obtiene la dirección** de la celda a partir de `self.wb.defined_names[reference].destinations`.
2. **Accede al valor** de esa celda.
3. Imprime un mensaje confirmando que se ha cargado la variable correspondiente desde el archivo Excel.

**Importante:** Se utiliza un comentario para
```python
self.parameters[reference] = self.wb[
next(self.wb.defined_names[reference].destinations)[0]
][
next(self.wb.defined_names[reference].destinations)[1]
].value  # type: ignore
print(f'Se ha cargado la variable "{reference}" del archivo "{self.excel_file}".')
```

La instrucción `return self.parameters[reference]` **devuelve un valor** específico de una colección llamada `parameters`, utilizando un identificador llamado `reference`. 

El comentario indica que esta acción **retorna el valor correspondiente** a esa referencia.
```python
return self.parameters[reference]
```