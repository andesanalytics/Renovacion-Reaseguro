# Modulo S4: Calculos de la Renovacion de Reaseguro

## Tablas de Parametrizaciones

1. **Matriz de Asignación de Contratos de Reaseguro (`contrato_cob`)**  
    - Contiene la asignación de contratos de reaseguro en función de diversas variables como póliza, producto y cobertura.
    - Permite determinar a qué contrato de reaseguro está asociado cada riesgo.
```python
contrato_cob: pd.DataFrame = pd.read_excel(io=archivo_parametros, sheet_name='Matriz Contrato-Cobertura')
```

2. **Matriz de Vigencias de Contratos (`parametros_contratos`)**  
    - Asigna la vigencia de cada contrato de reaseguro.
    - Útil para determinar en qué periodo de vigencia aplica un contrato específico.

3. **Matriz de Nombres de Productos (`nombre_prods`)**  
    - Permite asignar el nombre correcto del producto de renovación.
    - Posiblemente utilizada para estandarizar o mapear nombres de productos en el proceso de reasignación.

4. **Matrices de Asignación de `RAMO_REAS` y `COB_REAS`**  
    - Estas variables (`RAMO_REAS` y `COB_REAS`) son requeridas por el área de productos.
    - Se tienen dos matrices debido a que los criterios de asignación varían según el tipo de contrato:
        - **`ramo_reas_otros`**: Se utiliza para la mayoría de los contratos de reaseguro.
        - **`ramo_reas_desgnl`**: Aplica únicamente al contrato de reaseguro de *Desgravamen No Licitado*.


## Calculos Previo a la Asignacion
1. **Preprocesamiento de la Data**
    - Se extraen datos de los sistemas de administración de bases de datos (`GES` e `Iaxis`).
    - Se aplica un preprocesamiento inicial a la data utilizando la función `pre_procesamiento(tipo_calculo)`, lo que sugiere que la transformación depende del tipo de cálculo solicitado.

2. **Anonimización del Campo "RUT"**
    - Siguiendo una solicitud del área de productos, se anonimiza la columna `RUT` mediante la función `identificador_anonimo(df, ['RUT'])`.
3. **Eliminación de Productos Específicos**
    - También a solicitud del área de productos, se eliminan registros asociados a productos específicos de hospitalización al 100% y productos de fallecimiento por COVID-19.
    - Se imprime en consola la cantidad de registros que serán eliminados:
        ```python
        print(f'Se eliminarán {sum(df["PRODUCTO"].isin([88,101,193,369,370,371,372]))} registros que pertenecen a los productos de Hospitalario 100% y productos fallecimiento COVID')
        ```
    - Luego, se filtra la DataFrame eliminando estos productos y se genera una copia con:
        ```python
        df = df[~df['PRODUCTO'].isin([88,101,193,369,370,371,372])].copy()
        ```

4. **Creación de Campo "REGISTROS"**
    - Se agrega una nueva columna `REGISTROS`, asignándole el valor `1` a cada fila. Esto puede ser útil para futuras agregaciones o conteos.

5. **Identificación de Registros con Recargos**
        - Se identifica si los registros poseen recargos de sobreprima o extraprima mediante la función `recargos(df, calcula_recargos=0)`.

## Asignacion de Contratos
1. Asignamos contrato de reaseguro a los registros  
    - Se llama a la función `asignacion_contratos(df, contrato_cob, mantiene_na=1)`, que asigna un contrato de reaseguro a cada registro del DataFrame `df`. El parámetro `mantiene_na=1` indica que se conservarán los valores `NA` en caso de que no haya coincidencia en la asignación.

2. Asignamos vigencia del contrato a la que pertenecen los registros  
    - Se ejecuta la función `asignacion_vigencias(df, parametros_contratos, tipo_calculo, mantiene_na=1)`, que determina la vigencia del contrato de reaseguro al que corresponde cada registro. Esta función retorna dos DataFrames:
        - `df`: Contiene los registros con su vigencia asignada.
        - `df_deleted_vigencia`: Contiene los registros eliminados debido a que no cumplieron con los criterios de asignación de vigencia.


## Detalle de las Funciones
::: S4_Calculos_Renovacion



