import sys
import MySQLdb
import pandas as pd
import csv


# conexión a la base de datos
def connect_data_base():
    try:
        db = MySQLdb.connect("localhost","root","","companydata" )
    except MySQLdb.Error as e:
        print("No se pudo conectar a la base de datos: ", e)
        sys.exit(1) 
    print("Conexión correcta.")
    return db
    
    
# creación de tabla en la base de datos
def creacion_tabla(cursor, tabla, columnas):
    cursor.execute(f'DROP TABLE IF EXISTS {tabla}')
    cursor.execute(f"CREATE TABLE {tabla} ({', '.join(columnas)})")
    if(cursor):
        print(f'Se ha creado la tabla {tabla}')


tabla_empl = 'employeeperformance'
columnas = [
    'id INT PRIMARY KEY AUTO_INCREMENT',
    'department VARCHAR(255)',
    'performance_score FLOAT',
    'years_with_company INT',
    'salary FLOAT'
]

# cursor 
db = connect_data_base()
cursor = db.cursor()  
creacion_tabla(cursor, tabla_empl, columnas)


# inserción de datos a la tabla
def insertar_datos():
    with open('employeeperformance.csv', mode='r', encoding='utf-8') as csv_file:
        lector = csv.reader(csv_file, delimiter=',', quotechar='"')
        next(lector)
        try:
            for row in lector:
                cursor.execute(f"INSERT INTO {tabla_empl} (id, department, performance_score, years_with_company, salary) VALUES ( %s, %s, %s, %s, %s )", row[0:5])
        except csv.Error:
            print('Ha ocurrido un error al insertar los datos')
            sys.exit()
    try:
        db.commit()
        print('Datos insertados correctamente.')
        print("filas insertadas:", cursor.execute("SELECT * FROM employeeperformance"))
    except:
        db.rollback()
        

insertar_datos()


def estadistica_por_departamento(db, tabla):
    
    consulta = f'SELECT * FROM {tabla}'
    df = pd.read_sql(consulta, db) # ejecuta una consulta sql y carga el resultado en un DataFrame directamente
    return df

df = estadistica_por_departamento(db, tabla_empl)
print(df)
    
    
    # agrupamos por departamento y calculamos estadisticas de performance score
    # stats_perf_score = df.groupby('department')['performance_score'].agg(
    #     media='mean',
    #     mediana='median',
    #     desviacion_estandar='std'
    # ).reset_index()
    
    
    # print("Estadísticas de performance_score por departamento:")
    # print(stats_perf_score)
    
    
    # estadisticas de salary
    # stats_salary = df.groupby('department')['salary'].agg(
    #     media='mean',
    #     mediana='median',
    #     desviacion_estandar='std'
    # ).reset_index()
    
    # print("\nEstadísticas de salary por departamento:")
    # print(stats_salary)
    
    
    # calcula el número total de empleados
    # total_empleados = df.groupby('department')['id'].count().reset_index()
    # total_empleados.columns = ['department', 'total_empleados']
    
    # print("\nNúmero total de empleados por departamento:")
    # print(total_empleados)

# estadistica_por_departamento()


cursor.close()
db.close()
