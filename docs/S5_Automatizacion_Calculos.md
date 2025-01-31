# Modulo S5_Automatizacion_Calculos


Modulo que realiza los calculos de renovacion para cada uno de los contratos solicitados, para luego consolidar en los formatos establecidos.


# Carga de Variables Iniciales

Este proceso establece los valores de partida necesarios para que el programa funcione correctamente. Las variables iniciales pueden incluir configuraciones predeterminadas, datos de entrada o parámetros que el programa utilizará más adelante. Asegurar que estas variables estén correctamente definidas es crucial para evitar errores durante la ejecución del código.

- **Funcionalidad**: El código carga un conjunto de archivos Excel que contienen parámetros necesarios para un proceso específico.

- Se crea una instancia de la clase `Parameter_Loader`, que se inicializa con un archivo Excel llamado 'Inputs Archivos Excel.xlsx'.
- La instancia se configura para abrir el libro de trabajo de Excel inmediatamente al ser creada.
- Se utilizan métodos para obtener referencias específicas a tres archivos: 'archivo_calculos', 'archivo_querys' y 'archivo_parametros'.
- Estas referencias probablemente se utilizan para acceder a datos o configuraciones específicas dentro de los archivos Excel mencionados.
```python
# Cargamos el Parameter_Loader que contendrá la informacion de los excel de parametros a utilizar durante el proceso
files: Parameter_Loader = Parameter_Loader(excel_file='Inputs Archivos Excel.xlsx', open_wb=True, ruta_extensa='')
files.get_reference(reference='archivo_calculos')
files.get_reference(reference='archivo_querys')
files.get_reference(reference='archivo_parametros')
```

- **Funcionalidad del código**: Crea un directorio específico si no existe ya.

- Define una ruta de directorio como una cadena de texto y utiliza el método `mkdir` de la clase `Path` para crear el directorio, asegurando que todos los directorios padres necesarios también se creen y que no se genere un error si el directorio ya existe.
```python
# Rutas
ruta_salidas: str = '2 Output\\Resultados Validacion V1\\'
Path(ruta_salidas).mkdir(parents=True, exist_ok=True)
```

- El código define dos listas de contratos relacionados con un reporte de CAT XL.
- Ambas listas contienen los mismos nombres de contratos, almacenados como cadenas de texto.
```python
# Contratos a ejecutar y contratos a consolidar en el reporte de CAT XL
contratos_ejecutar: list[str] = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']
contratos_consolidar_catxl: list[str] = ['AP + Urgencias Medicas','Digital Klare','K-Fijo','Multisocios','Desgravamen No Licitado']
```

- **Funcionalidad del código**: El script interactúa con el usuario para decidir si ejecutar una extracción de datos mediante consultas SQL y luego carga parámetros desde un archivo Excel.

1. El script se ejecuta solo si es el archivo principal, no cuando es importado como módulo.
2. Se inicia un bucle que solicita al usuario si desea ejecutar una extracción de consultas SQL.
3. El usuario debe ingresar "Y" para sí o "N" para no.
4. Si el usuario elige "Y", se llama a una función para automatizar las consultas y el bucle termina.
5. Si el usuario elige "N", se imprime un mensaje indicando que no se realizarán extracciones y el bucle termina.
6. Si el usuario ingresa una opción inválida, se le solicita que elija nuevamente.
7. Después de salir del bucle, se crea una instancia de `Parameter_Loader`.
8. Esta instancia carga tablas de parámetros desde un archivo Excel especificado.
9. El archivo Excel se abre con la opción `open_wb=True`.
10. La ruta del archivo Excel se obtiene de un diccionario de parámetros llamado `files`.
11. El archivo de parámetros se identifica con la clave `'archivo_parametros'`.
12. La ruta extensa se establece como una cadena vacía.
```python
# ? Control: Solo ejecutará este script si es ejecutado desde aqui y no es llamado desde otro script
if __name__=='__main__':
	# Ciclo para elegir si extraemos o no la data de expuestos conectandose a SQL
	while True:
		opcion: str = input("Desea Ejecutar Extracción de Querys (Y/N)")
		if opcion == "Y":
			automatizacion_querys(files)
			break
		elif opcion == "N":
			print('No se ejecutan extracciones de querys')
			break
		else:
			print("\nOpción no válida. Por favor, elige una opcion correcta (Y/N).")
	# Cargamos una instancia de Parameter_Loader que contendrá las tablas de parametros
	tables: Parameter_Loader = Parameter_Loader(excel_file=files.parameters['archivo_parametros'], open_wb=True, ruta_extensa='')
```

- **Funcionalidad del código**: Ejecuta contratos de renovación actualizando parámetros en un archivo Excel y realizando cálculos asociados.

1. Itera sobre una lista de contratos a ejecutar.
2. Imprime un mensaje indicando el contrato que se está ejecutando.
3. Abre un archivo Excel que contiene parámetros de cálculo.
4. Actualiza un valor específico en la hoja de parámetros del archivo Excel con el contrato actual.
5. Guarda y cierra el archivo Excel después de la actualización.
6. Crea una instancia de `Parameter_Loader` para cargar los parámetros de cálculo desde el archivo Excel.
7. Llama a una función para cargar tablas de parámetros.
8. Ejecuta cálculos de renovación utilizando los parámetros y tablas cargados.
9. Los resultados de los cálculos se guardan en una ruta de salida especificada.
```python
# ? Ciclo que ejecuta los contratos de renovacion
for contrato_ejecutar in contratos_ejecutar:
	print(f'Ejecutando contrato {contrato_ejecutar}')
	# Cambia en el excel de parametros de calculo los datos del contrato que ejecutaremos
	excel_parametros = openpyxl.load_workbook(files.parameters['archivo_calculos'])
	excel_parametros['Parametros'].cell(7,2).value=contrato_ejecutar
	excel_parametros.save(files.parameters['archivo_calculos'])
	excel_parametros.close()
	# Una vez cambiado eso, genera una isntancia Parameter_Loader que contendrá los parametros de calculo
	parameters: Parameter_Loader = Parameter_Loader(excel_file=files.parameters['archivo_calculos'], open_wb=True)
	# cargamos tablas y ejecutamos los calculos de renovacion
	carga_parametros(files, parameters)
	calculos_renovacion(parameters, tables, ruta_salidas)
```

- **Funcionalidad del código**: Consolida y exporta reportes de renovación de contratos para uso interno y para reaseguradores.

1. Inicia el proceso de consolidación de reportes con un mensaje en la consola.
2. Crea dos DataFrames vacíos para almacenar los datos consolidados de uso interno y reaseguradores.
3. Itera sobre una lista de contratos que necesitan ser consolidados.
4. Para cada contrato, imprime un mensaje indicando que se está leyendo la data correspondiente.
5. Lee el archivo de datos de uso interno del contrato actual desde un archivo comprimido.
6. Lee el archivo de datos para reaseguradores del contrato actual desde un archivo comprimido.
7. Agrega los datos leídos de uso interno al DataFrame consolidado de uso interno.
8. Agrega los datos leídos para reaseguradores al DataFrame consolidado de reaseguradores.
9. Imprime un mensaje indicando que los reportes finales están siendo exportados.
10. Exporta el DataFrame consolidado de uso interno a un archivo comprimido.
11. Exporta el DataFrame consolidado de reaseguradores a un archivo comprimido.
```python
# ? Ciclo que consolida los contratos de renovacion para el reporte CAT XL
print('Comienza el proceso de consolidación de reportes')
df_catxl_uso_interno = pd.DataFrame()
df_catxl_reaseguradores= pd.DataFrame()
# Por cada contrato extraemos el reporte de uso interno y el reporte que va a los reaseguradores, y los vamos consolidando
for contrato_consolidar in contratos_consolidar_catxl:
	print(f'Leyendo data del contrato {contrato_consolidar}')
	df_uso_interno: pd.DataFrame = pd.read_csv(f'{ruta_salidas}Detalle Renovacion {contrato_consolidar} Uso Interno.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
	df_reaseguradores: pd.DataFrame = pd.read_csv(f'{ruta_salidas}Detalle Renovacion {contrato_consolidar} Reaseguradores.txt.zip',sep=';',decimal='.',encoding='latin-1',low_memory=False)
	df_catxl_uso_interno = pd.concat([df_catxl_uso_interno,df_uso_interno])
	df_catxl_reaseguradores = pd.concat([df_catxl_reaseguradores,df_reaseguradores])
print('Exportamos Reportes Finales!')
# exportaciones finales
df_catxl_uso_interno.to_csv(f'{ruta_salidas}Detalle Renovacion Cat XL Uso Interno.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
df_catxl_reaseguradores.to_csv(f'{ruta_salidas}Detalle Renovacion Cat XL Reaseguradores.txt.zip',sep=';',decimal='.',date_format='%d-%m-%Y',index=False)
```