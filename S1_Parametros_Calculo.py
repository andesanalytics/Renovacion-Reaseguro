"""
Script que carga todos los parametros del archivo de parametros de calculo para que sean recogidos en los procesos posteriores
"""
import datetime
import shutil
from typing import Any
from pathlib import Path
from S0_Loaders import Parameter_Loader

def carga_parametros(files: Parameter_Loader, parameter_loader: Parameter_Loader) -> None:
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

    # * Calculos basados en los parametros previamente extraidos
    # Diccionario de contratos con caracteristicas particulares de cada calculo
    parameter_loader.parameters['diccionario_contratos'] = parameter_loader.parameters['Diccionario Contratos'].set_index('CONTRATO').to_dict()
    diccionario_contratos: dict[str,Any] = parameter_loader.parameters['diccionario_contratos']
    contrato: str = parameter_loader.parameters['contrato']
    tipo_calculo: str = parameter_loader.parameters['tipo_calculo']
    fecha_cierre: datetime.datetime = parameter_loader.parameters['fecha_cierre']
    ruta_inputs: str = f'{parameter_loader.ruta_extensa}1 Input\\{tipo_calculo}\\'
    periodo: int = fecha_cierre.year*100+fecha_cierre.month
    
    # # Calculos sobre la variable diccionario_contratos
    parameter_loader.parameters['tipo_contrato']=diccionario_contratos['TIPO CONTRATO'][contrato]
    parameter_loader.parameters['tipo_prima']=diccionario_contratos['TIPO PRIMA'][contrato]
    parameter_loader.parameters['clasificacion_contrato']=diccionario_contratos['CLASIFICACION CONTRATO'][contrato]
    parameter_loader.parameters['base_ges']=diccionario_contratos['BASE GES'][contrato]
    parameter_loader.parameters['base_iaxis']=diccionario_contratos['BASE IAXIS'][contrato]
    parameter_loader.parameters['cap_expuestos']=diccionario_contratos['CAPS EXPUESTOS'][contrato]
    parameter_loader.parameters['pivotea_df']=diccionario_contratos['PIVOTEA CONTRATO'][contrato]
    nombre_base: str = diccionario_contratos['NOMBRE BASE'][contrato]

    # # Calculos de fechas para el cierre
    parameter_loader.parameters['fecha_inicio_mes'] = datetime.datetime(fecha_cierre.year,fecha_cierre.month,1)
    parameter_loader.parameters['fecha_cierre_mes_anterior'] = parameter_loader.parameters['fecha_inicio_mes']-datetime.timedelta(days=1)
    parameter_loader.parameters['dias_exposicion'] = (fecha_cierre-parameter_loader.parameters['fecha_inicio_mes']).days+1
    parameter_loader.parameters['periodo'] = periodo
    parameter_loader.parameters['periodo_anterior'] = parameter_loader.parameters['periodo'] - (1 if parameter_loader.parameters['periodo']%100>1 else 89)
    parameter_loader.parameters['año_cierre'] = fecha_cierre.year

    # Calculo sobre las rutas de entrada y de salida
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

    # Tasa de descuento para calculo de monto asegurado para coberturas de rentas
    parameter_loader.parameters['tdm_mensual'] = (1+tasa_dscto_mensualidades)**(1/12)-1
    # Campo de rut para revisar duplicados
    parameter_loader.parameters['campo_rut_duplicados'] = 'RUT_CONTRATANTE' if (parameter_loader.parameters['tipo_contrato']=='Generales')&('Incendio y Sismo' in contrato) else 'RUT'
    # Campo del tipo de base que sirve para ir a buscar los archivos de expuestos
    nombre_tipo_base: str = 'Expuestos ' if tipo_calculo=='Prima de Reaseguro' else 'Siniestros '
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
