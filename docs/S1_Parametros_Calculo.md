# Modulo S1_Parametros_Calculo


Script que carga todos los parametros del archivo de parametros de calculo para que sean recogidos en los procesos posteriores


# Funcion `carga_parametros`

La función `carga_parametros` se encarga de cargar parámetros necesarios para un proceso específico. Recibe dos argumentos:

1. **`files`**: Un objeto de la clase `Parameter_Loader` que contiene información sobre los archivos Excel que se utilizarán en el proceso.
2. **`parameter_loader`**: Otro objeto de la clase `Parameter_Loader` que contiene información sobre los parámetros específicos de la ejecución, enfocado en un contrato de reaseguro particular.

Esta función no devuelve ningún valor.

## Carga de Variables con Funciones de Clase

Este código utiliza funciones definidas dentro de una clase para inicializar y cargar ciertas variables. Estas funciones encapsulan la lógica necesaria para asignar valores a las variables, asegurando que se mantenga la integridad y consistencia de los datos. Al emplear métodos de clase, se facilita la reutilización del código y se mejora su organización.

- **Funcionalidad del código**: Carga y organiza parámetros y configuraciones desde un archivo para su uso en un proceso de cálculo.

1. Utiliza `parameter_loader` para obtener referencias de varios parámetros necesarios.
2. Almacena la tasa de descuento de mensualidades como un número decimal.
3. Define varias subcarpetas como cadenas de texto para organizar archivos de entrada y salida.
4. Recupera configuraciones relacionadas con el formato de datos, como separadores y decimales.
5. Obtiene el tipo de base de datos expuestos y una variable adicional relacionada.
6. Carga una hoja de cálculo específica llamada "Diccionario Contratos".
7. Cierra el libro de trabajo de Excel después de cargar los datos necesarios.
8. Convierte la hoja "Diccionario Contratos" en un diccionario indexado por el campo "CONTRATO".
9. Asigna el diccionario resultante a una variable para su uso posterior.
10. Define variables para el contrato, tipo de cálculo y fecha de cierre a partir de los parámetros cargados.
11. Construye una ruta de entrada utilizando la ruta base, el tipo de cálculo y una estructura de carpetas predefinida.
12. Calcula el período en formato `YYYYMM` a partir de la fecha de cierre.
```python
parameter_loader.get_reference(reference='tipo_calculo')
parameter_loader.get_reference(reference='contrato')
parameter_loader.get_reference(reference='fecha_cierre')
tasa_dscto_mensualidades: float = parameter_loader.get_reference(reference='tasa_dscto_mensualidades')
parameter_loader.get_reference(reference='edad_casos_perdidos')
subcarpeta_input: str = parameter_loader.get_reference(reference='subcarpeta_input')
subcarpeta_historico: str = parameter_loader.get_reference(reference='subcarpeta_historico')
subcarpeta_pyme: str = parameter_loader.get_reference(reference='subcarpeta_pyme')
subcarpeta_recargos: str = parameter_loader.get_reference(reference='subcarpeta_recargos')
subcarpeta_regiones: str = parameter_loader.get_reference(reference='subcarpeta_regiones')
subcarpeta_si: str = parameter_loader.get_reference(reference='subcarpeta_si')
subcarpeta_otros: str = parameter_loader.get_reference(reference='subcarpeta_otros')
subcarpeta_lob: str = parameter_loader.get_reference(reference='subcarpeta_lob')
subcarpeta_uso_seguro: str = parameter_loader.get_reference(reference='subcarpeta_uso_seguro')
subcarpeta_output: str = parameter_loader.get_reference(reference='subcarpeta_output')
parameter_loader.get_reference(reference='separador_input')
parameter_loader.get_reference(reference='decimal_input')
parameter_loader.get_reference(reference='separador_fechas_input')
parameter_loader.get_reference(reference='separador_output')
parameter_loader.get_reference(reference='decimal_output')
parameter_loader.get_reference(reference='periodo_historico')
parameter_loader.get_reference(reference='aplica_check_parametros')
parameter_loader.get_reference(reference='uso_fecha_anulacion_historico')
tipo_base_expuestos: str = parameter_loader.get_reference(reference='tipo_base_expuestos')
add_base_expuestos: Any = parameter_loader.get_reference(reference='add_base_expuestos')
parameter_loader.get_reference(reference='tipo_proceso')
parameter_loader.get_reference(reference='base_input_siniestros_generales')
parameter_loader.get_table_xlsx(sheet_name = 'Diccionario Contratos')
parameter_loader.wb.close()
# Definimos algunas variables que utilizaremos frecuentemente en el guardado de informacion dentro de la clase
parameter_loader.parameters['diccionario_contratos'] = parameter_loader.parameters['Diccionario Contratos'].set_index('CONTRATO').to_dict()
diccionario_contratos: dict[str,Any] = parameter_loader.parameters['diccionario_contratos']
contrato: str = parameter_loader.parameters['contrato']
tipo_calculo: str = parameter_loader.parameters['tipo_calculo']
fecha_cierre: datetime.datetime = parameter_loader.parameters['fecha_cierre']
ruta_inputs: str = f'{parameter_loader.ruta_extensa}1 Input\\{tipo_calculo}\\'
periodo: int = fecha_cierre.year*100+fecha_cierre.month
```

## Cálculos sobre la Variable `diccionario_contratos`

La estructura de datos `diccionario_contratos` se utiliza para almacenar información relacionada con contratos. Se realizarán cálculos específicos sobre esta variable para extraer, analizar o modificar datos relevantes. Estos cálculos pueden incluir operaciones como sumar valores, filtrar información o actualizar registros dentro del diccionario.

- **Funcionalidad del código**: Asigna valores de un diccionario a las propiedades de un objeto y a una variable.

1. Se accede a un diccionario llamado `diccionario_contratos` utilizando una clave `contrato`.
2. Se extraen valores asociados a diferentes claves dentro del diccionario.
3. Estos valores se asignan a las propiedades del objeto `parameter_loader.parameters`.
4. Las propiedades del objeto incluyen `tipo_contrato`, `tipo_prima`, `clasificacion_contrato`, `base_ges`, `base_iaxis`, `cap_expuestos`, y `pivotea_df`.
5. Además, se asigna un valor del diccionario a una variable llamada `nombre_base`.
6. La clave `contrato` se utiliza para acceder a los valores específicos dentro de cada categoría del diccionario.
7. El código organiza y almacena información de contratos en un objeto y una variable.
8. Esto facilita el acceso y manipulación de datos relacionados con contratos en el programa.
```python
parameter_loader.parameters['tipo_contrato']=diccionario_contratos['TIPO CONTRATO'][contrato]
parameter_loader.parameters['tipo_prima']=diccionario_contratos['TIPO PRIMA'][contrato]
parameter_loader.parameters['clasificacion_contrato']=diccionario_contratos['CLASIFICACION CONTRATO'][contrato]
parameter_loader.parameters['base_ges']=diccionario_contratos['BASE GES'][contrato]
parameter_loader.parameters['base_iaxis']=diccionario_contratos['BASE IAXIS'][contrato]
parameter_loader.parameters['cap_expuestos']=diccionario_contratos['CAPS EXPUESTOS'][contrato]
parameter_loader.parameters['pivotea_df']=diccionario_contratos['PIVOTEA CONTRATO'][contrato]
nombre_base: str = diccionario_contratos['NOMBRE BASE'][contrato]
```

## Cálculos de Fechas para el Cierre

Este segmento del código se encarga de realizar operaciones relacionadas con fechas, específicamente para determinar el momento adecuado para cerrar un proceso o actividad. Utiliza funciones y métodos que permiten calcular intervalos de tiempo, ajustar fechas y verificar condiciones temporales. Esto es útil para asegurar que las acciones se realicen en el momento correcto según un calendario o cronograma predefinido.

- **Funcionalidad del código**: Calcula y almacena parámetros relacionados con fechas y periodos basados en una fecha de cierre dada.

1. Define la fecha de inicio del mes usando el año y mes de la fecha de cierre, estableciendo el día como el primero del mes.
2. Calcula la fecha de cierre del mes anterior restando un día a la fecha de inicio del mes actual.
3. Determina el número de días de exposición desde el inicio del mes hasta la fecha de cierre, incluyendo ambos días.
4. Almacena el periodo actual en los parámetros.
5. Calcula el periodo anterior restando 1 al periodo actual, ajustando por meses de enero.
6. Guarda el año de la fecha de cierre en los parámetros.
```python
parameter_loader.parameters['fecha_inicio_mes'] = datetime.datetime(fecha_cierre.year,fecha_cierre.month,1)
parameter_loader.parameters['fecha_cierre_mes_anterior'] = parameter_loader.parameters['fecha_inicio_mes']-datetime.timedelta(days=1)
parameter_loader.parameters['dias_exposicion'] = (fecha_cierre-parameter_loader.parameters['fecha_inicio_mes']).days+1
parameter_loader.parameters['periodo'] = periodo
parameter_loader.parameters['periodo_anterior'] = parameter_loader.parameters['periodo'] - (1 if parameter_loader.parameters['periodo']%100>1 else 89)
parameter_loader.parameters['año_cierre'] = fecha_cierre.year
```

## Cálculo sobre las Rutas de Entrada y de Salida

Este código se encarga de analizar y procesar las rutas de entrada y salida en un sistema determinado. Utiliza algoritmos para calcular eficientemente las trayectorias óptimas o necesarias. El objetivo es optimizar el flujo y mejorar la gestión de las rutas.

- **Funcionalidad del código**: Configura rutas de acceso para diferentes tipos de datos y salidas en un objeto `parameter_loader`.

1. Define rutas de entrada para varios tipos de datos, como históricos, pymes, recargos, regiones, y otros.
2. Utiliza variables predefinidas para construir las rutas de entrada, combinando directorios base y subcarpetas específicas.
3. Asigna cada ruta construida a una clave correspondiente en el diccionario `parameters` del objeto `parameter_loader`.
4. Define una ruta de salida principal utilizando una combinación de variables que incluyen tipo de cálculo, periodo, contrato y subcarpeta de salida.
5. Asigna la ruta de salida principal a la clave `ruta_output` en el diccionario `parameters`.
6. Define una ruta específica para el cruce histórico de duplicados, basada en la ruta de salida principal.
7. Asigna esta ruta de cruce histórico a la clave `ruta_historico_output` en el diccionario `parameters`.
8. Las rutas se construyen utilizando cadenas formateadas (`f-strings`) para facilitar la inserción de variables.
9. El código organiza y centraliza la gestión de rutas, facilitando el acceso a diferentes tipos de datos y resultados.
10. La estructura del código permite una fácil modificación de rutas al cambiar las variables base.
11. El uso de un diccionario para almacenar las rutas permite un acceso eficiente y organizado a las mismas.
```python
parameter_loader.parameters['ruta_input'] = f'{ruta_inputs}{subcarpeta_input}\\{nombre_base}\\'
parameter_loader.parameters['ruta_historico_input'] = f'{ruta_inputs}{subcarpeta_historico}\\'
parameter_loader.parameters['ruta_pyme'] = f'{ruta_inputs}{subcarpeta_pyme}\\'
parameter_loader.parameters['ruta_recargos'] = f'{ruta_inputs}{subcarpeta_recargos}\\'
parameter_loader.parameters['ruta_regiones'] = f'{ruta_inputs}{subcarpeta_regiones}\\'
parameter_loader.parameters['ruta_si'] = f'{ruta_inputs}{subcarpeta_si}\\'
parameter_loader.parameters['ruta_otros'] = f'{ruta_inputs}{subcarpeta_otros}\\'
parameter_loader.parameters['ruta_lob'] = f'{ruta_inputs}{subcarpeta_lob}\\'
parameter_loader.parameters['ruta_uso_seguro'] = f'{ruta_inputs}{subcarpeta_uso_seguro}\\'
parameter_loader.parameters['ruta_output'] = f'{parameter_loader.ruta_extensa}2 Output\\{tipo_calculo}\\{periodo}\\{contrato}\\{subcarpeta_output}\\'
parameter_loader.parameters['ruta_historico_output'] = f'{parameter_loader.parameters["ruta_output"]}Duplicados Cruce Historico'
```

## Otros Cálculos

Este apartado del código se encargará de realizar operaciones adicionales que complementan el procesamiento principal. Estas operaciones pueden incluir cálculos matemáticos, transformaciones de datos o cualquier otra tarea necesaria para completar el flujo de trabajo. Su propósito es asegurar que todos los aspectos del problema estén abordados adecuadamente.

- **Funcionalidad del código**: Calcula la tasa de descuento mensual a partir de una tasa de descuento anual para determinar el monto asegurado en coberturas de rentas.

- La tasa de descuento mensual se obtiene elevando la suma de uno más la tasa de descuento anual a la
```python
# Tasa de descuento para calculo de monto asegurado para coberturas de rentas
parameter_loader.parameters['tdm_mensual'] = (1+tasa_dscto_mensualidades)**(1/12)-1
```

- **Funcionalidad**: El código selecciona un campo específico para revisar duplicados basado en el tipo de contrato y su contenido.
- Si el tipo de contrato es "Generales" y contiene "Incendio y Sismo", se asigna 'RUT_CONTRATANTE';
```python
# Campo de rut para revisar duplicados
parameter_loader.parameters['campo_rut_duplicados'] = 'RUT_CONTRATANTE' if (parameter_loader.parameters['tipo_contrato']=='Generales')&('Incendio y Sismo' in contrato) else 'RUT'
```

- **Funcionalidad del código**: Asigna un valor a la variable `nombre_tipo_base` basado en la condición de otra variable llamada `tipo_calculo`.

- Si `tipo_calculo` es igual a `'Prima de Reaseguro'`, entonces `nombre_tipo_base` se
```python
# Campo del tipo de base que sirve para ir a buscar los archivos de expuestos
nombre_tipo_base: str = 'Expuestos ' if tipo_calculo=='Prima de Reaseguro' else 'Siniestros '
```

- El código genera nombres de archivos basados en el tipo de base de datos y otros parámetros.

1. Se define una variable que indica el tipo de base de datos, `tipo_base_expuestos`.
2. Si el tipo es 'Mensual', se crean dos nombres de archivo usando el nombre de la base y el periodo.
3. Para el tipo 'Anual', los nombres de archivo incluyen el año de cierre obtenido de un cargador de parámetros.
4. En el caso 'Historico', los nombres de archivo no incluyen fechas específicas.
5. Si el tipo es 'Fecha', se utiliza una fecha específica extraída de `add_base_expuestos`.
6. Para el tipo 'Periodos', se usa un periodo específico de `add_base_expuestos` en los nombres de archivo.
7. Si el tipo no coincide con ninguno de los casos anteriores, los nombres de archivo se establecen como cadenas vacías.
8. Los nombres de archivo generados se almacenan en las variables `archivo_input` y `archivo_input_ges`.
9. La variable `archivo_input` contiene el nombre del archivo estándar.
10. La variable `archivo_input_ges` contiene el nombre del archivo con el sufijo 'GES'.
11. Se utiliza la interpolación de cadenas para construir los nombres de archivo.
12. Los nombres de archivo se construyen en formato `.txt`.
13. La estructura `if-elif-else` se utiliza para manejar diferentes tipos de bases de datos.
14. La lógica del código permite flexibilidad en la generación de nombres de archivo según diferentes criterios temporales.
15. El código depende de variables externas como `nombre_tipo_base`, `nombre_base`, `periodo`, y `add_base_expuestos`.
16. La variable `parameter_loader.parameters` se utiliza para acceder al año de cierre en el caso anual.
17. La función `str()` se utiliza para convertir fechas a cadenas de texto.
18. El código es útil para automatizar la generación de nombres de archivo en diferentes contextos temporales.
```python
# Calculo de ruta y nombres de archivos de expuestos que iremos a buscar
if tipo_base_expuestos=='Mensual':
	archivo_input: str = f'{nombre_tipo_base}{nombre_base} {periodo}.txt'
	archivo_input_ges: str = f'{nombre_tipo_base}{nombre_base} GES {periodo}.txt'
elif tipo_base_expuestos=='Anual':
	archivo_input: str = f'{nombre_tipo_base}{nombre_base} {parameter_loader.parameters["año_cierre"]}.txt'
	archivo_input_ges: str = f'{nombre_tipo_base}{nombre_base} GES {parameter_loader.parameters["año_cierre"]}.txt'
elif tipo_base_expuestos=='Historico':
	archivo_input: str = f'{nombre_tipo_base}{nombre_base}.txt'
	archivo_input_ges: str = f'{nombre_tipo_base}{nombre_base} GES.txt'
elif tipo_base_expuestos=='Fecha':
	archivo_input: str = f'{nombre_tipo_base}{nombre_base} {str(add_base_expuestos)[0:10]}.txt'
	archivo_input_ges: str = f'{nombre_tipo_base}{nombre_base} GES {str(add_base_expuestos)[0:10]}.txt'
elif tipo_base_expuestos=='Periodos':
	archivo_input: str = f'{nombre_tipo_base}{nombre_base} {add_base_expuestos}.txt'
	archivo_input_ges: str = f'{nombre_tipo_base}{nombre_base} GES {add_base_expuestos}.txt'
else:
	archivo_input: str = ''
	archivo_input_ges: str = ''
```

- **Funcionalidad del código**: Configura rutas de directorios, prepara un archivo de reporte y copia archivos de parámetros para un sistema de administración de bases de datos.

1. Asigna nombres de archivos de entrada a parámetros específicos en un objeto `parameter_loader`.
2. Define una lista de rutas de directorios necesarias para el proceso.
3. Crea cada directorio especificado en la lista si no existe, asegurando que la estructura de carpetas esté lista.
4. Verifica si el tipo de prima es "Prima Unica" y, de ser así, crea un directorio adicional para el historial de salidas.
5. Abre un archivo de texto para escribir un reporte de errores en la ruta de salida especificada.
6. Escribe una línea inicial en el archivo de reporte que incluye detalles del cálculo, contrato y periodo.
7. Copia un archivo de cálculos desde una ubicación de parámetros a la ruta de salida.
8. Copia un archivo de parámetros desde su ubicación original a la ruta de salida, asegurando que todos los archivos necesarios estén en el lugar correcto para el proceso.
```python
# Nombre de la base de expuestos de los sistemas de administracion de BBDD GES e iAxis
parameter_loader.parameters['archivo_input'] = archivo_input
parameter_loader.parameters['archivo_input_ges'] = archivo_input_ges
# Crea las rutas de salidas
rutas: list[str]=['ruta_input', 'ruta_historico_input', 'ruta_pyme', 'ruta_recargos', 'ruta_regiones', 'ruta_si', 'ruta_otros', 'ruta_output']
for ruta in rutas:
	Path(parameter_loader.parameters[ruta]).mkdir(parents=True, exist_ok=True)
if parameter_loader.parameters['tipo_prima'] == 'Prima Unica' : Path(parameter_loader.parameters['ruta_historico_output']).mkdir(parents=True, exist_ok=True)
# Comienzo a escribir en el archivo de reporte
parameter_loader.parameters['archivo_reporte'] = open(f'{parameter_loader.parameters["ruta_output"]}0. Reporte Errores.txt','w')
parameter_loader.parameters['archivo_reporte'].write(f'Comienzo de reporte de errores de {tipo_calculo} - {contrato} al periodo {periodo}\n\n')
# Copia del archivo de parametros en la ruta del output
shutil.copyfile(files.parameters['archivo_calculos'], f'{parameter_loader.parameters["ruta_output"]}{files.parameters["archivo_calculos"]}')
shutil.copyfile(files.parameters['archivo_parametros'], f'{parameter_loader.parameters["ruta_output"]}{files.parameters["archivo_parametros"]}')
```