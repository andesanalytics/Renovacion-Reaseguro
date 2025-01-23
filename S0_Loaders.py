import pandas as pd
import openpyxl
from typing import Any

class Parameter_Loader:
    """
    Clase que se encarga de cargar parámetros y tablas desde archivos Excel (.xlsx) y archivos de texto (.txt, .csv, etc.).
    """
    
    def __init__(self, excel_file: str, open_wb: bool=False, ruta_extensa: str='') -> None:
        """
        Constructor de la clase Parameter_Loader.

        :param excel_file: Ruta del archivo Excel (.xlsx) que se usará para cargar parámetros.
        :param open_wb: Indica si se debe abrir el archivo Excel con openpyxl al crear la instancia.
        :param ruta_extensa: Campo opcional para almacenar rutas de archivos de texto o uso genérico.
        """
        # Definimos atributo excel_file
        self.excel_file: str = excel_file
        # Diccionario para almacenar tablas o valores que ya han sido cargados (cache).
        self.parameters: dict[str, Any] = {}
        
        # Ruta adicional o extensiva, en caso de necesitar guardar/elaborar alguna ruta completa.
        self.ruta_extensa: str = ruta_extensa
        
        # Si se indica open_wb como True, se abre el archivo Excel con openpyxl y se guarda el objeto Workbook.
        if open_wb:
            self.wb: openpyxl.Workbook = openpyxl.load_workbook(excel_file)

    def get_table_xlsx(self, sheet_name: str) -> pd.DataFrame:
        """
        Retorna un DataFrame de la hoja sheet_name del archivo Excel self.excel_file.
        Si ya está cargado en self.parameters, se reutiliza la versión en memoria.
        
        :param sheet_name: Nombre de la hoja (sheet) en el archivo Excel.
        :return: DataFrame con el contenido de la hoja especificada.
        """
        # Carga la hoja de Excel solo si no ha sido cargada antes.
        if sheet_name not in self.parameters:
            self.parameters[sheet_name] = pd.read_excel(self.excel_file, sheet_name=sheet_name)
            print(f'Se ha cargado la tabla "{sheet_name}" del archivo "{self.excel_file}".')
        
        # Retorna el DataFrame correspondiente.
        return self.parameters[sheet_name]
    
    def get_table_txt(self, file_path: str, decimal: str, separador: str, campos_fecha: Any = '') -> Any:
        """
        Retorna un DataFrame cargado desde un archivo de texto (csv, txt, etc.).
        Si ya está cargado en self.parameters, se reutiliza la versión en memoria.

        :param file_path: Ruta del archivo de texto a cargar.
        :param decimal: Carácter que define el separador decimal en el archivo.
        :param separador: Separador de campos en el archivo (por ejemplo, ',', ';', ' ').
        :param campos_fecha: Columnas que deben ser interpretadas como fechas (opcional).
        :return: DataFrame con el contenido del archivo.
        """
        # Carga la tabla de texto solo si no ha sido cargada antes.
        if file_path not in self.parameters:
            self.parameters[file_path]: pd.DataFrame = pd.read_csv(
                file_path, 
                decimal=decimal, 
                sep=separador,
                date_format='%d-%m-%Y', 
                parse_dates=campos_fecha,
                encoding='latin-1',
                low_memory=False  # Evita problemas con archivos muy grandes
            )
            print(f'Se ha cargado la tabla desde el archivo "{file_path}".')
        
        # Retorna el DataFrame correspondiente.
        return self.parameters[file_path]
    
    def get_reference(self, reference: str) -> Any:
        """
        Retorna el valor de una celda definida como nombre en el archivo Excel,
        usando openpyxl y la propiedad defined_names.

        :param reference: Nombre definido en el libro de Excel que hace referencia a una celda.
        :return: Valor de la celda a la cual apunta el nombre definido.
        """
        # Carga la referencia solo si no ha sido cargada antes.
        if reference not in self.parameters:
            # Toma la dirección (fila, columna) de la referencia y obtiene el valor de la celda.
            self.parameters[reference] = self.wb[
                next(self.wb.defined_names[reference].destinations)[0]
            ][
                next(self.wb.defined_names[reference].destinations)[1]
            ].value  # type: ignore
            
            print(f'Se ha cargado la variable "{reference}" del archivo "{self.excel_file}".')
        
        # Retorna el valor correspondiente.
        return self.parameters[reference]

        