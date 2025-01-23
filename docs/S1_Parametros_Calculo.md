# Modulo S1_Parametros_Calculo


Script que carga todos los parametros del archivo de parametros de calculo para que sean recogidos en los procesos posteriores

# Funcion `carga_parametros`

La función `carga_parametros` se encarga de **cargar parámetros necesarios para un proceso**. Utiliza dos archivos de la clase `Parameter_Loader`:

- **files**: Contiene información sobre los archivos Excel que se utilizarán en el proceso.
- **parameter_loader**: Proporciona información sobre los parámetros específicos de la ejecución, relacionada con un contrato de reaseguro en particular.

## Carga de Variables

Se emplean funciones de una clase para **cargar ciertas variables** necesarias para el funcionamiento del programa. Esto permite organizar y gestionar mejor los datos que se utilizarán más adelante.
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
```

El código define varias variables que se utilizarán para manejar información en una clase. 

- **diccionario_contratos**: Se crea un diccionario a partir de un DataFrame, utilizando la columna 'CONTRATO' como índice.
- **contrato**: Almacena un valor relacionado con un contrato específico.
- **tipo_calculo**: Guarda el tipo de cálculo que se va a realizar.
- **fecha_cierre**: Contiene la fecha de cierre en formato de
```python
parameter_loader.parameters['diccionario_contratos'] = parameter_loader.parameters['Diccionario Contratos'].set_index('CONTRATO').to_dict()
diccionario_contratos: dict[str,Any] = parameter_loader.parameters['diccionario_contratos']
contrato: str = parameter_loader.parameters['contrato']
tipo_calculo: str = parameter_loader.parameters['tipo_calculo']
fecha_cierre: datetime.datetime = parameter_loader.parameters['fecha_cierre']
ruta_inputs: str = f'{parameter_loader.ruta_extensa}1 Input\\{tipo_calculo}\\'
periodo: int = fecha_cierre.year*100+fecha_cierre.month
```

## Cálculos sobre Contratos

Se realizarán **cálculos** utilizando la variable `diccionario_contratos`. Estos cálculos permitirán obtener información relevante y procesar datos relacionados con los contratos de manera eficiente.
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

Este código se encarga de realizar **cálculos relacionados con fechas** que son necesarios para el proceso de cierre. Se utilizarán para determinar plazos y fechas clave en el desarrollo de un proyecto o actividad.
```python
parameter_loader.parameters['fecha_inicio_mes'] = datetime.datetime(fecha_cierre.year,fecha_cierre.month,1)
parameter_loader.parameters['fecha_cierre_mes_anterior'] = parameter_loader.parameters['fecha_inicio_mes']-datetime.timedelta(days=1)
parameter_loader.parameters['dias_exposicion'] = (fecha_cierre-parameter_loader.parameters['fecha_inicio_mes']).days+1
parameter_loader.parameters['periodo'] = periodo
parameter_loader.parameters['periodo_anterior'] = parameter_loader.parameters['periodo'] - (1 if parameter_loader.parameters['periodo']%100>1 else 89)
parameter_loader.parameters['año_cierre'] = fecha_cierre.year
```

## Cálculo de Rutas

Este código se encargará de realizar **cálculos** relacionados con las **rutas de entrada y salida**. Esto permitirá optimizar el flujo de datos y mejorar la eficiencia en el manejo de la información.
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

## Otros cálculos

Se realizarán cálculos adicionales que complementan los resultados previos. Estos cálculos son importantes para obtener una visión más completa de los datos y mejorar la precisión de los resultados finales.

La instrucción asigna un valor a la clave `'tdm_mensual'` en el diccionario `parameter_loader.parameters`. Este valor se calcula a partir de la **tasa de descuento mensual** (`tasa_dscto_mensualidades`), ajustándola para obtener una tasa efectiva mensual.

El comentario indica que esta tasa se utiliza para el **cálculo del monto asegurado** en coberturas de rentas.
```python
parameter_loader.parameters['tdm_mensual'] = (1+tasa_dscto_mensualidades)**(1/12)-1
```

La instrucción asigna un valor a `campo_rut_duplicados` en función de ciertas condiciones. 

- Si el `tipo_contrato` es **"Generales"** y **"Incendio y Sismo"** está presente en `contrato`, se asigna **'RUT_CONTRATANTE'**.
- En caso contrario, se asigna **'RUT'**.

Esto permite determinar el campo a utilizar para revisar duplicados en función del tipo
```python
parameter_loader.parameters['campo_rut_duplicados'] = 'RUT_CONTRATANTE' if (parameter_loader.parameters['tipo_contrato']=='Generales')&('Incendio y Sismo' in contrato) else 'RUT'
```

La instrucción asigna un valor a la variable `nombre_tipo_base` basado en la condición de `tipo_calculo`. Si `tipo_calculo` es igual a `'Prima de Reaseguro'`, se asigna el valor `'Expuestos '`. En caso contrario, se asigna `'Siniestros '`.

Este campo es útil para buscar archivos relacionados con los **expuestos**.
```python
nombre_tipo_base: str = 'Expuestos ' if tipo_calculo=='Prima de Reaseguro' else 'Siniestros '
```

El código determina el nombre y la ruta de archivos de entrada basándose en el valor de `tipo_base_expuestos`. Dependiendo de si es **Mensual**, **Anual**, **Histórico**, **Fecha** o **Periodos**, se generan diferentes nombres de archivos utilizando variables como `nombre_tipo_base`, `nombre_base`, y `periodo`. Si no coincide con ninguna de estas opciones, se asigna una cadena vacía a los nombres de los archivos.

**Importante**:
```python
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

Las instrucciones asignan valores a dos parámetros en un cargador de parámetros. Específicamente:

- `archivo_input`: se establece como el archivo de entrada principal.
- `archivo_input_ges`: se asigna el archivo relacionado con el sistema de administración GES.

Esto se relaciona con la **base de expuestos** de los sistemas de administración de bases de datos GES e iAxis.
```python
parameter_loader.parameters['archivo_input'] = archivo_input
parameter_loader.parameters['archivo_input_ges'] = archivo_input_ges
```

El código **crea directorios** en el sistema de archivos. 

1. Se define una lista de **rutas**.
2. Para cada ruta en la lista, se crea un directorio utilizando `mkdir`, asegurando que se creen también los directorios padres si no existen.
3. Si el tipo de prima es **"Prima Unica"**, se crea un directorio adicional para la ruta de salida histórica.

Esto permite organizar y asegurar que todas las rutas necesarias estén disponibles antes
```python
rutas: list[str]=['ruta_input', 'ruta_historico_input', 'ruta_pyme', 'ruta_recargos', 'ruta_regiones', 'ruta_si', 'ruta_otros', 'ruta_output']
for ruta in rutas:
Path(parameter_loader.parameters[ruta]).mkdir(parents=True, exist_ok=True)
if parameter_loader.parameters['tipo_prima'] == 'Prima Unica' : Path(parameter_loader.parameters['ruta_historico_output']).mkdir(parents=True, exist_ok=True)
```

El código abre un archivo llamado **"0. Reporte Errores.txt"** en modo escritura y lo asigna a la clave **'archivo_reporte'** en un diccionario de parámetros. Luego, escribe un mensaje que indica el inicio de un reporte de errores, incluyendo información sobre **tipo_calculo**, **contrato** y **periodo**. 

El comentario sugiere que este es el inicio del proceso de escritura en el archivo de reporte.
```python
parameter_loader.parameters['archivo_reporte'] = open(f'{parameter_loader.parameters["ruta_output"]}0. Reporte Errores.txt','w')
parameter_loader.parameters['archivo_reporte'].write(f'Comienzo de reporte de errores de {tipo_calculo} - {contrato} al periodo {periodo}\n\n')
```

Las instrucciones de código utilizan la función `shutil.copyfile` para **copiar archivos** de una ubicación a otra. 

- La primera línea copia el archivo especificado en `files.parameters['archivo_calculos']` a la ruta de salida definida en `parameter_loader.parameters["ruta_output"]`.
- La segunda línea realiza lo mismo para el archivo en `files.parameters['archivo_parametros']`.

Ambas acciones aseguran que los archivos se guarden en la **ruta de salida** dese
```python
shutil.copyfile(files.parameters['archivo_calculos'], f'{parameter_loader.parameters["ruta_output"]}{files.parameters["archivo_calculos"]}')
shutil.copyfile(files.parameters['archivo_parametros'], f'{parameter_loader.parameters["ruta_output"]}{files.parameters["archivo_parametros"]}')
```