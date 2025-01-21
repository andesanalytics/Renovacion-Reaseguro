import pandas as pd
import openpyxl
from typing import Any

class Parameter_Loader:
    def __init__(self, excel_file: str, open_wb: bool=False, ruta_extensa:str='') -> None:
        self.excel_file: str = excel_file
        self.parameters: dict[str,Any] = {}
        self.ruta_extensa: str = ruta_extensa
        if open_wb: self.wb: openpyxl.Workbook = openpyxl.load_workbook(excel_file)

    def get_table_xlsx(self, sheet_name: str) -> pd.DataFrame:
        # Carga la tabla solo si no ha sido cargada antes
        if sheet_name not in self.parameters:
            self.parameters[sheet_name] = pd.read_excel(self.excel_file, sheet_name=sheet_name)
            print(f'Se ha cargado la tabla {sheet_name} del archivo {self.excel_file}')
        return self.parameters[sheet_name]
    
    def get_table_txt(self, file_path: str, decimal: str, separador: str, campos_fecha:Any = '') -> Any:
        # Carga la tabla solo si no ha sido cargada antes
        if file_path not in self.parameters:
            self.parameters[file_path]: pd.DataFrame = pd.read_csv(file_path, decimal=decimal, sep=separador, date_format='%d-%m-%Y', parse_dates=campos_fecha, index=False, encoding='latin-1',low_memory=False) # type: ignore
            print(f'Se ha cargado la tabla {file_path}')
        return self.parameters[file_path]
    
    def get_reference(self, reference: str) -> Any:
        if reference not in self.parameters:
            self.parameters[reference] = self.wb[next(self.wb.defined_names[reference].destinations)[0]][next(self.wb.defined_names[reference].destinations)[1]].value # type: ignore
            print(f'Se ha cargado la variable {reference} del archivo {self.excel_file}')
        return self.parameters[reference]
    
        