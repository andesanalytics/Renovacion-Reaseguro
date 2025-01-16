"""De este script obtenemos los nombres de los archivos que vamos a utilizar
Archivo QUerys: Contiene informacion de las querys que realizaremos
Archivo Parametros Calculo: Contiene informacion relacionadas al calculo que realizaremos
Archivo Parametros Reaseguro: Contiene tablas de parametris relacionadas con el calculo en sí mismo y reporterias que nos han pedido
"""

# Importamos lector de excel openpyxl
import openpyxl
from typing import Any

# Se define ruta extensa por si el interprete de python no asume la ruita en la que se encuetra el proyecto
ruta_extensa: str = ''

def lee_parametro_excel(wb: openpyxl.Workbook, nombre_parametro: str) -> Any:
    return wb[next(wb.defined_names[nombre_parametro].destinations)[0]][next(wb.defined_names[nombre_parametro].destinations)[1]].value # type: ignore

# Leemos archivo "Inputs Archivos Excel.xlsx" que contiene esta informacion
wb: openpyxl.Workbook = openpyxl.load_workbook(ruta_extensa + 'Inputs Archivos Excel.xlsx')
# Extraemos archivo de calculos
archivo_calculos: str = lee_parametro_excel (wb, 'archivo_calculos')
# Extraemos archivo de querys
archivo_querys: str = lee_parametro_excel (wb, 'archivo_querys')
# Extraemos archivo de parametros de reaseguro
archivo_parametros: str = lee_parametro_excel (wb, 'archivo_parametros')
# Cerramos
wb.close()

# Prueba de Ejecucion del codigo
print(f'El script {__name__} se está ejecutando')