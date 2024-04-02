import pandas as pd 
import os 
from datetime import timedelta,datetime
''' 
    # 1.Leer el archivo ---DONE
    # 2.Reconocer de alguna manera todas las hoja que se tengan y cual es la bandera ---DONE 
    # 3.Buscar hoja x hoja todas las filas de ese paciente ---DONE
    # 4.Obtener las fechas de cada hoja ---DONE
    # 5.Comparar cada una de las fechas de la tabla bandera con cada una de las demas  --DONE
    # 6.Validar las fechas si estan en un rango de 60 dias o no ---DONE
    # 7.1.Si exition alguna fecha que me sirva entonces busco los datos del paciente que pertenezcan a esa fila... ---DONE
    # 8.1.Guardar esa fila ---DONE
    # 7.2.Si la primera fecha bandera no me sirvio con ninguna de las demas de las tablas entonces... ---DONE
    # 8.2.Paso a la siguiente fecha bandera y esa fecha anterior ya no formara parte de mis datos ---DONE
    # 9.2.Seguir hasta que las fechas banderas del paciente se acaben  ---DONE
    # 10. Tomar esa fila buena y guardarla en un nuevo Data Frame  ---DONE
    # 11. Cuando se termine de iterar sobre todos los pacientes, se validara si esxiste un archivo donde guardar la informacion
    # 12.1 Si esciste se carga el archivo, convierte en Data Frame y se concatena con el anterior
    # 12.2. Si no existe entonces solo se crea uno nuevo
    # Fin del programa
'''


# ----------------------------------------------------------
# Services 
class Sheet():
    '''
    Clase para instansear e abstraer los metodos mas usados con respecto a cada hoja 
    '''
    def __init__(self,file,number):
        self.file = file
        # Se accede a cada clave del diccionario file para determinar el nombre de la hoja
        self.name = list(file.keys())[number]

    def get_sheet(self):
        # Obtener la hoja 
        sheet = self.file[self.name]
        return sheet

    def get_columns(self,patient_rid,column):
        # Obtener columna 
        sheet = self.get_sheet()
        data = sheet.loc [ sheet['RID'] == patient_rid,column ]
        return data 

    def get_rows(self,patient_rid):
        # Obtener fila 
        try:
            sheet = self.get_sheet()
            rows = sheet.loc [ sheet['RID'] == patient_rid]
            return rows
        except Exception as e:
            print(e)

    def __str__(self):
        return self.name
    
def sheet_generator(file,flag_sheet ):
    '''
    Un generador de hojas que itera todas las hojas de un archivo menos la hoja bandera
    '''
    for index,sheet in enumerate(file.values()):
        # retornamos la hoja siguiente del archivo mientras no sea la bandera
        if not sheet.equals(flag_sheet) :
            # Se crea una instacia de la clase Sheet para cada hoja
            current_sheet = Sheet(file, index)
            yield current_sheet 

def drop_row(data,file):
    '''
    Elimina la fila de la hoja basado en un diccionario con los datos
    y el archivo que contiene todas las hojas.
    Se itera cada elemento del diccionario y se obtiene el nombre de la hoja como clave y su id como valor.
    Retorna el archivo con cada una de sus hojas sin las columnas 
    '''
    for sheet_name,id in data.items():
        # Se accede a la hoja actual
        sheet_df = file[sheet_name]
        # De la hoja actual se borra la fila que contenga el id que se desempaqueto antes
        # Luego se guarda en la hoja del archivo para que los cambios sean permanentes 
        file[sheet_name] = sheet_df.drop(sheet_df[sheet_df['INDEX'] == id].index)
    return file

def ask_filter_options():
    '''
    Esta funcion solo se encarga de mostrar y preguntar al usuario si desea
    parte del archivo existente antes de trabajar con el o no.
    Retorna una tupla con:
    1.La columna por la cual se realizara el filtrado,
    2.La condicion ej:(<=,>,==,etc),
    3.El numero por el cual se tendra en cuenta para filtrar
    '''
    # Aqui se pregunta la columna x la cual se va a filtrar.
    # Puede ser 'RID','EXAMDATE' cualquier columna que sea comun en todas las hojas por supuesto
    column: str = input("Que columna desea usar para filtrar: ")
    print("Que opcion desea para filtrar:")
    options = {
        "1": "mayor que",
        "2": "mayor igual que",
        "3": "menor que",
        "4": "menor igual que",
        "5": "igual que",
    }
    # Este ciclo solo recorre el diccionarion de 'options' para mostrar cada una de las opciones
    for key, value in options.items():
        print(f"{key} : {value} ")
    response = input("Elijo la opcion :")
    condition = options[response]
    # Revisa si el usuario quiere filtrar por fechas 
    if column == 'EXAMDATE':
        # Pide la fecha y muestra ejemplos de como insertarla
        number = input(f"Desea todas las columnas {condition} formato de fecha debe ser 'mm-dd-aaaa'ej.(08-24-2012): ")
        # Trasforma el string recibido en uan fecha valida para trabajar
        number = datetime.strptime(number, '%m-%d-%Y')
    else:
        # Se pregunta el numero que el usuario escogio
        number = input(f"Desea todas las columnas {condition} : ")
        # Se comprueba que quiere filtrar por RID
        if column == 'RID':
            # Se trasforma el numer en entero para poder compararlo
            number = int(number) 
    # Si nada de lo anterior se cumple significa que el usuario escogio un campo 
    # que no fue ni 'RID' ni 'EXAMDATE'... probablemente uno como 'Phase' asi que no 
    # se trasforma en nada puesto que esa columna trabaja con strings
    return column, condition, number

def filter_dfs( file_path,shape_file_path, column=None, condition=None, number=None):
    '''
    Crea un archivo copia para no modificar el original.
    Luego lo modifica si el basado si recibe los parametros necesarios
    para la modificacion 
    '''
    print('Leyendo archivo...')
    try:
        # Se intenta acceder al archivo original y crear un Data Frame de el
        file = pd.read_excel(file_path, sheet_name=None)
        # Se guarda cada uno de los valores en el archivo copia
        shape_file = pd.concat(file.values())
        # Luego se exporta el archivo copia
        shape_file.to_excel(shape_file_path,index=False)

        # Se lee el archivo copia 
        with pd.ExcelWriter(shape_file_path, engine='openpyxl') as writer:
            # Luego si itera sobre las hojas del archivo original
            for sheet, df in file.items():
                # Se valida si las condiciones existen 
                if condition and column and number:
                    # Si existen se le camian los valores a ese Data Frame
                    if condition == "mayor que":
                        df = df[df[column] > number]
                    elif condition == "menor que":
                        df = df[df[column] < number]
                    elif condition == "mayor igual que":
                        df = df[df[column] >= number]
                    elif condition == "menor igual que":
                        df = df[df[column] <= number]
                    elif condition == "igual que":
                        df = df[df[column] == number]
                # Convierte las fechas en un formato mas facil de trabajar 
                df.loc[:, 'EXAMDATE'] = pd.to_datetime(df['EXAMDATE'], format='%m-%d-%y')
                # Crea el archivo excel y le genera una columna 'INDEX'
                df.to_excel(writer, sheet_name=sheet, index=True,index_label='INDEX')

    # Si pasa un error de columna lo atrapa y lanza un texto 
    except KeyError as exc:
        return f"Error al intentar filtrar {exc}, revise uno de los filtros e intentelo de nuevo."

def is_valid(row):
    """
    Recibe una fila y itera sus valores para ver si
    contiene algun campo vacio
    """
    if not row:
        return False
    for value in row.values():
        # Recorremos los valores de la fila para ver si es nan o no
        if pd.isna(value):
            return False
    return True

# end services
# ----------------------------------------------------------


def read_xlsx():
    '''
    Esta funcion lee el archivo y lo retorna 
    una tupla con el archivo trasformado en Data Frame y su path 
    '''
    # Leer el archivo
    file_path = input('Cual es la ruta del archivo que desea filtrar: ')
    current_path = os.getcwd()
    shape_file_path = current_path + 'shape.xlsx' 
    # Comprobar que el archivo existe 
    if not os.path.isfile(file_path):
        return read_xlsx()
    # Pregunta si se desea filtrar antes de operar el archivo
    filtrar = input('Desea filtrar el archivo por un dato en especifico: ? (y/n): ')
    if filtrar.lower() == 'y' or filtrar.lower() == 'yes':
        # Si dijo que si, se llama a la funcion que pregunta los datos necesarios
        column,condition,number = ask_filter_options()
        # Luego se filtra
        filter_dfs(file_path,shape_file_path, column, condition, number)
    else: 
        # Si dijo que no solo se crea una copia del archivo para no modificar el verdadero
        # y trabajar con la copia
        filter_dfs(file_path, shape_file_path)
    # Se lee el archivo copia y se crea un Data Frame para trabajar con el 
    file = pd.read_excel(shape_file_path, sheet_name=None)
    return file,shape_file_path

def patient_iterator(file,flag_sheet,shape_path):
    '''
    Itera cada uno de los pacientes en el archivo 
    a traves de la hoja bandera
    y los guarda cuando termina
    '''
    # Se pregunta cuantos dias quiere tener en cuenta para el filtrado de tiempo
    days = int(input('Con cuantos dias quiere comparar? : '))
    print('Procesando...')
    # Se crea el Data Frame que se va a guardar
    df_to_export = pd.DataFrame()
    # Se obtiene todos los datos RID de la hoja bandera
    rids = set(flag_sheet['RID'])
    # Se itera cada RID 
    for rid in rids:
        # A cada RID se le hace el filtrado 
        patient_df  = get_data(file, rid, flag_sheet,days)
        # Luego cada uno de esos datos se le va agregando al Data Frame que se creo posteriormente
        df_to_export = pd.concat([df_to_export,patient_df],ignore_index=True)
    # Se llama a la funcion 'save'  para guardar todo los datos en un archivo excel
    return save(df_to_export,shape_path)


def get_data(file,patient_rid,flag_sheet,days):
    '''
    Recibe el archivo, el rid del paciente y la hoja bandera 
    y retorna un Data Frame de cada paciente con los datos correctos
    '''

    # Primero se obtiene todas las filas banderas del paciente recibido por paramatros 
    patient_rows =  flag_sheet.loc [ flag_sheet['RID'] == patient_rid ]
    # Se crea un Data Frame donde se va a guardar los datos correctos del paciente
    patient_df = pd.DataFrame()
    # Se itera cada una de las filas banderas del paciente 
    # y se obtiene el indice de cada fila junto con sus datos
    for index,flag_row in patient_rows.iterrows():
        # Variable donde se va a almacenar todos los datos que hay q borrar 
        data_to_drop = {}
        # Se borra la columna 'INDEX' de la hoja bandera
        flag_row = flag_row.drop(['INDEX'])
        # Se obtiene la fecha bandera de esa fila
        flag_date = flag_row['EXAMDATE']
        # Se le pide al generador de hojas todas las hojas del archivo
        sheets_generator = sheet_generator(file,flag_sheet)
        # Diccionario donde se va a guardar todos los datos del paciente
        data = {}
        # Ciclo para iterar todas las hojas del archivo (que no sean la bandera) 
        while True :
            # Se hace un intento de obtener la data correcta
            try:
                # Solo se hace el intento de recorrer las hojas
                # hasta que de el error de final de la iteracion
                current_sheet = next(sheets_generator)
                # Se obtienen las filas de esa hoja
                rows = current_sheet.get_rows(patient_rid)
                # Se valida si alguna de esas columnas de la hoja actual cumple con la condicion 
                correct_data = compare_dates(flag_date, rows,days)
                # Se valida si se retorno un valor valido  
                if not isinstance(correct_data, tuple) :
                    # No pasa el filtro: Borrando data y rompiendo el while
                    data.clear()
                    break
                # Si paso el filtro asi que se guardan los datos relevantes y el id
                correct_data,id = correct_data
                data_to_drop[current_sheet.name] = id 
                # Si paso el filtro: Actualizando data...
                data.update(correct_data)
            # Cuando se termina de iterar todas las hojas...
            except StopIteration:
                # Final de la iteracion: Guardando datos restantes y rompiendo wl while
                data.update(flag_row)
                break
        if is_valid(data):
            # Valida si la informacion que se guardo cumple las condiciones validas 
            row = pd.DataFrame([data])
            patient_df = pd.concat([patient_df,row],ignore_index=True)
            file = drop_row(data_to_drop,file)
    return patient_df

def compare_dates(flag_date,rows,days):
    '''
    Obtiene una fecha bandera y unas filas de fechas.
    Retorna la primera fila que cumplio la condicion 
    y su respectivo id.
    De lo contrario no retorna nada
    '''
    for index,row in rows.iterrows():
        # Se obtiene la fecha de esa fila
        date = row['EXAMDATE']
        # Se compara la fecha si se encuentra en el rango de 
        # la fecha seleccionada por el usuario
        if abs(flag_date - date) <= timedelta(days=days):
            # Si cumple la condicion se adquiere el ID de esa fila
            id = row['INDEX']
            # Se borran las columnas no necesarias para coformar al Data Frame
            columns_to_drop = ['RID','EXAMDATE','Phase','INDEX']
            row = row.drop(columns_to_drop)
            return row,id

def save(df,shape_path):
    '''
    Crea el archivo excel de un Data Frame    
    '''
    print('Archivo procesado con exito.')
    # Pregunta la ruta donde se desea guardar el archivo
    file_path = input('Escriba la ruta donde quiere guardar el nuevo archivo : ')
    # Modifica las fechas en un formato de mejor visualizacion
    df['EXAMDATE'] = df['EXAMDATE'].dt.strftime( "%m/%d/%y")
    # Verifica si no existe la ruta del archivo donde el usuario dijo 
    if not os.path.isfile(file_path):
        # Ordena los valores en orden basado en el RID para una mejor visualizacion
        df = df.sort_values('RID')
        # Crea el archivo excel con el nombre de 'Hoja1'
        df.to_excel(file_path,sheet_name='Hoja1',na_rep='',index=False)
        print('Archivo creado con exito')
    # En caso que el archivo exista ya...
    else:
        # Se lee el archivo
        existing_file = pd.read_excel(file_path,sheet_name=0)
        # Modifica las fechas del archivo exsitente en un formato de mejor visualizacion
        existing_file["EXAMDATE"] = existing_file["EXAMDATE"].dt.strftime( "%m/%d/%y")
        # Combina los dos archivos 
        combined_df = pd.concat([existing_file,df],ignore_index=True)
        # Lee el archivo en modo de agregacion
        with pd.ExcelWriter(file_path,mode='a',engine='openpyxl',if_sheet_exists='replace') as writer:
            # Crea el archivo excel del Data Frame combinado recientemente 
            combined_df.to_excel(writer,index=False,sheet_name='Hoja1',na_rep='')
        print('Archivo modificado con exito')
    # Por ultimo borra el archivo copia creado al principio
    os.remove(shape_path)
    

# Se obtiene el archivo y la ruta del archivo compia
file,file_shape_path = read_xlsx()

# Se le pregunta al usuario cual es la hoja bandera
sheet_number =int( input('Que numero de hoja es la hoja bandera: ')) -1
# Se crea una instacia de Sheet de esa hoja y se obtiene su contenido 
flag_sheet = Sheet(file, sheet_number ).get_sheet()

# Se itera cada paciente
patient_iterator(file,flag_sheet,file_shape_path)
