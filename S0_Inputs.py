import openpyxl

# ruta_extensa = 'C:\\BC\\0. Modelos Python\\'
ruta_extensa = ''

wb = openpyxl.load_workbook(ruta_extensa + 'Inputs Archivos Excel.xlsx')
archivo_calculos=wb[next(wb.defined_names['archivo_calculos'].destinations)[0]][next(wb.defined_names['archivo_calculos'].destinations)[1]].value
archivo_querys=wb[next(wb.defined_names['archivo_querys'].destinations)[0]][next(wb.defined_names['archivo_querys'].destinations)[1]].value
archivo_parametros=wb[next(wb.defined_names['archivo_parametros'].destinations)[0]][next(wb.defined_names['archivo_parametros'].destinations)[1]].value
wb.close()

# Prueba de Ejecucion del codigo
print(f'El script {__name__} se está ejecutando')