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




## Detalle de las Funciones
::: S4_Calculos_Renovacion



