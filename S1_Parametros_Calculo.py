"""
Script que carga todos los parametros del archivo de parametros de calculo para que sean recogidos en los procesos posteriores
"""


# Importamos librerias que vamos a utilizar
import datetime
import openpyxl
import pandas as pd
import shutil
from pathlib import Path
from S0_Inputs import archivo_calculos, archivo_parametros, ruta_extensa


# Prueba de Ejecucion del codigo
print(f'El script {__name__} se está ejecutando')

# * Importacion de parametros a tarves del archivo de excel de calculos
wb = openpyxl.load_workbook(ruta_extensa+archivo_calculos)
wb = openpyxl.load_workbook(ruta_extensa+archivo_calculos)
tipo_calculo=wb[next(wb.defined_names['tipo_calculo'].destinations)[0]][next(wb.defined_names['tipo_calculo'].destinations)[1]].value # type: ignore
contrato=wb[next(wb.defined_names['contrato'].destinations)[0]][next(wb.defined_names['contrato'].destinations)[1]].value
fecha_cierre=wb[next(wb.defined_names['fecha_cierre'].destinations)[0]][next(wb.defined_names['fecha_cierre'].destinations)[1]].value
tasa_dscto_mensualidades=wb[next(wb.defined_names['tasa_dscto_mensualidades'].destinations)[0]][next(wb.defined_names['tasa_dscto_mensualidades'].destinations)[1]].value
edad_casos_perdidos=wb[next(wb.defined_names['edad_casos_perdidos'].destinations)[0]][next(wb.defined_names['edad_casos_perdidos'].destinations)[1]].value
subcarpeta_input=wb[next(wb.defined_names['subcarpeta_input'].destinations)[0]][next(wb.defined_names['subcarpeta_input'].destinations)[1]].value
subcarpeta_historico=wb[next(wb.defined_names['subcarpeta_historico'].destinations)[0]][next(wb.defined_names['subcarpeta_historico'].destinations)[1]].value
subcarpeta_pyme=wb[next(wb.defined_names['subcarpeta_pyme'].destinations)[0]][next(wb.defined_names['subcarpeta_pyme'].destinations)[1]].value
subcarpeta_recargos=wb[next(wb.defined_names['subcarpeta_recargos'].destinations)[0]][next(wb.defined_names['subcarpeta_recargos'].destinations)[1]].value
subcarpeta_regiones=wb[next(wb.defined_names['subcarpeta_regiones'].destinations)[0]][next(wb.defined_names['subcarpeta_regiones'].destinations)[1]].value
subcarpeta_reservas=wb[next(wb.defined_names['subcarpeta_reservas'].destinations)[0]][next(wb.defined_names['subcarpeta_reservas'].destinations)[1]].value
subcarpeta_si=wb[next(wb.defined_names['subcarpeta_si'].destinations)[0]][next(wb.defined_names['subcarpeta_si'].destinations)[1]].value
subcarpeta_otros=wb[next(wb.defined_names['subcarpeta_otros'].destinations)[0]][next(wb.defined_names['subcarpeta_otros'].destinations)[1]].value
subcarpeta_lob=wb[next(wb.defined_names['subcarpeta_lob'].destinations)[0]][next(wb.defined_names['subcarpeta_lob'].destinations)[1]].value
subcarpeta_uso_seguro= wb[next(wb.defined_names['subcarpeta_uso_seguro'].destinations)[0]][next(wb.defined_names['subcarpeta_uso_seguro'].destinations)[1]].value
subcarpeta_output=wb[next(wb.defined_names['subcarpeta_output'].destinations)[0]][next(wb.defined_names['subcarpeta_output'].destinations)[1]].value
subcarpeta_compara=wb[next(wb.defined_names['subcarpeta_comparativo'].destinations)[0]][next(wb.defined_names['subcarpeta_comparativo'].destinations)[1]].value
separador_input=wb[next(wb.defined_names['separador_input'].destinations)[0]][next(wb.defined_names['separador_input'].destinations)[1]].value
decimal_input=wb[next(wb.defined_names['decimal_input'].destinations)[0]][next(wb.defined_names['decimal_input'].destinations)[1]].value
separador_fechas_input=wb[next(wb.defined_names['separador_fechas_input'].destinations)[0]][next(wb.defined_names['separador_fechas_input'].destinations)[1]].value
separador_output=wb[next(wb.defined_names['separador_output'].destinations)[0]][next(wb.defined_names['separador_output'].destinations)[1]].value
decimal_output=wb[next(wb.defined_names['decimal_output'].destinations)[0]][next(wb.defined_names['decimal_output'].destinations)[1]].value
separador_compara=wb[next(wb.defined_names['separador_compara'].destinations)[0]][next(wb.defined_names['separador_compara'].destinations)[1]].value
decimal_compara=wb[next(wb.defined_names['decimal_compara'].destinations)[0]][next(wb.defined_names['decimal_compara'].destinations)[1]].value
periodo_historico=wb[next(wb.defined_names['periodo_historico'].destinations)[0]][next(wb.defined_names['periodo_historico'].destinations)[1]].value
aplica_check_parametros=wb[next(wb.defined_names['aplica_check_parametros'].destinations)[0]][next(wb.defined_names['aplica_check_parametros'].destinations)[1]].value
uso_fecha_anulacion_historico=wb[next(wb.defined_names['uso_fecha_anulacion_historico'].destinations)[0]][next(wb.defined_names['uso_fecha_anulacion_historico'].destinations)[1]].value
tipo_base_expuestos=wb[next(wb.defined_names['tipo_base_expuestos'].destinations)[0]][next(wb.defined_names['tipo_base_expuestos'].destinations)[1]].value
add_base_expuestos=wb[next(wb.defined_names['add_base_expuestos'].destinations)[0]][next(wb.defined_names['add_base_expuestos'].destinations)[1]].value
tipo_proceso=wb[next(wb.defined_names['tipo_proceso'].destinations)[0]][next(wb.defined_names['tipo_proceso'].destinations)[1]].value
base_input_siniestros_generales = wb[next(wb.defined_names['base_input_siniestros_generales'].destinations)[0]][next(wb.defined_names['base_input_siniestros_generales'].destinations)[1]].value
wb.close()

# Otras importaciones
diccionario_contratos=pd.read_excel(io=ruta_extensa+archivo_calculos, sheet_name='Diccionario Contratos').set_index('CONTRATO').to_dict()

# * Calculos sobre las variables ya extraidas del excel
# Calculos sobre la variable diccionario_contratos
tipo_contrato=diccionario_contratos['TIPO CONTRATO'][contrato]
tipo_prima=diccionario_contratos['TIPO PRIMA'][contrato]
clasificacion_contrato=diccionario_contratos['CLASIFICACION CONTRATO'][contrato]
base_ges=diccionario_contratos['BASE GES'][contrato]
base_iaxis=diccionario_contratos['BASE IAXIS'][contrato]
cap_expuestos=diccionario_contratos['CAPS EXPUESTOS'][contrato]
pivotea_df=diccionario_contratos['PIVOTEA CONTRATO'][contrato]
nombre_base=diccionario_contratos['NOMBRE BASE'][contrato]

# Calculos de fechas para el cierre
fecha_inicio_mes=datetime.datetime(fecha_cierre.year,fecha_cierre.month,1)
fecha_cierre_mes_anterior=fecha_inicio_mes-datetime.timedelta(days=1)
dias_exposicion=(fecha_cierre-fecha_inicio_mes).days+1
# Varios periodos, ya que Desg NL tiene 2 meses de desfase, mientras que Multisocios 1
periodo=fecha_cierre.year*100+fecha_cierre.month
periodo_anterior=periodo - (1 if periodo%100>1 else 89)
año_cierre=fecha_cierre.year

# Calculo sobre las rutas de entrada y de salida
ruta_input=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_input+'\\'+nombre_base+'\\'
ruta_historico_input=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_historico+'\\'
ruta_pyme=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_pyme+'\\'
ruta_recargos=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_recargos+'\\'
ruta_regiones=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_regiones+'\\'
ruta_reservas=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_reservas+'\\'
ruta_si=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_si+'\\'
ruta_otros=ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_otros+'\\'
ruta_lob = ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_lob+'\\'
ruta_uso_seguro = ruta_extensa+'1 Input\\'+tipo_calculo+'\\'+subcarpeta_uso_seguro+'\\'
ruta_output=ruta_extensa+'2 Output\\'+tipo_calculo+'\\'+str(periodo)+'\\'+contrato+'\\'+subcarpeta_output+'\\'
ruta_historico_output=ruta_output+'Duplicados Cruce Historico'

# Tasa de descuento para calculo de monto asegurado para coberturas de rentas
tdm_mensual=(1+tasa_dscto_mensualidades)**(1/12)-1
# Campo de rut para revisar duplicados
campo_rut_duplicados='RUT_CONTRATANTE' if (tipo_contrato=='Generales')&('Incendio y Sismo' in contrato) else 'RUT'
# Campo del tipo de base que sirve para ir a buscar los archivos de expuestos
nombre_tipo_base = 'Expuestos ' if tipo_calculo=='Prima de Reaseguro' else 'Siniestros '
# Calculo de ruta y nombres de archivos de expuestos que iremos a buscar
if tipo_base_expuestos=='Mensual':
    archivo_input=nombre_tipo_base+nombre_base+' '+str(periodo)+'.txt'
    archivo_input_ges=nombre_tipo_base+nombre_base+' GES '+str(periodo)+'.txt'
elif tipo_base_expuestos=='Anual':
    archivo_input=nombre_tipo_base+nombre_base+' '+str(año_cierre)+'.txt'
    archivo_input_ges=nombre_tipo_base+nombre_base+' GES '+str(año_cierre)+'.txt'
elif tipo_base_expuestos=='Historico':
    archivo_input=nombre_tipo_base+nombre_base+'.txt'
    archivo_input_ges=nombre_tipo_base+nombre_base+' GES.txt'
elif tipo_base_expuestos=='Fecha':
    archivo_input=nombre_tipo_base+nombre_base+' ('+str(add_base_expuestos)[0:10]+').txt'
    archivo_input_ges=nombre_tipo_base+nombre_base+' GES ('+str(add_base_expuestos)[0:10]+').txt'
elif tipo_base_expuestos=='Periodos':
    archivo_input=nombre_tipo_base+nombre_base+' '+add_base_expuestos+'.txt'
    archivo_input_ges=nombre_tipo_base+nombre_base+' GES '+add_base_expuestos+'.txt'

# Crea las rutas de salidas
rutas=[ruta_input, ruta_historico_input, ruta_pyme, ruta_recargos, ruta_regiones, ruta_reservas, ruta_si, ruta_otros, ruta_output]
for ruta in rutas:
    Path(ruta).mkdir(parents=True, exist_ok=True)
if tipo_prima == 'Prima Unica' : Path(ruta_historico_output).mkdir(parents=True, exist_ok=True)


# Comienzo a escribir en el archivo de reporte
archivo_reporte=open(ruta_output+'0. Reporte Errores.txt','w')
archivo_reporte.write('Comienzo de reporte de errores de {} - {} al periodo {}\n\n'.format(tipo_calculo,contrato, periodo))

# Copia del archivo de parametros en la ruta del output
shutil.copyfile(archivo_calculos, ruta_output+archivo_calculos)
shutil.copyfile(archivo_parametros, ruta_output+archivo_parametros)
