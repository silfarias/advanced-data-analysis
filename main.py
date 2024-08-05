import sys
import MySQLdb
import pandas as pd

try:
    db = MySQLdb.connect("localhost","root","","companydata" )
except MySQLdb.Error as e:
    print("No se pudo conectar a la base de datos: ", e)
    sys.exit(1) 
print("Conexión correcta.")


cursor = db.cursor()


def estadistica_por_departamento():
    consulta = 'SELECT * FROM employeeperformance'
    lec_consulta = pd.read_sql(consulta, con=db)
    df = pd.DataFrame(lec_consulta)
    
    # agrupamos por departamento y calculamos estadisticas de performance score
    stats_perf_score = df.groupby('department')['performance_score'].agg(
        media='mean',
        mediana='median',
        desviacion_estandar='std'
    ).reset_index()
    
    
    print("Estadísticas de performance_score por departamento:")
    print(stats_perf_score)
    
    
    # estadisticas de salary
    stats_salary = df.groupby('department')['salary'].agg(
        media='mean',
        mediana='median',
        desviacion_estandar='std'
    ).reset_index()
    
    print("\nEstadísticas de salary por departamento:")
    print(stats_salary)
    
    
    # calcula el número total de empleados
    total_empleados = df.groupby('department')['id'].count().reset_index()
    total_empleados.columns = ['department', 'total_empleados']
    
    print("\nNúmero total de empleados por departamento:")
    print(total_empleados)

estadistica_por_departamento()
db.close()


    # consulta = "SELECT * FROM employeeperformance"
    # lec_consulta = pd.read_sql(consulta, con=db)
    # df = pd.DataFrame(lec_consulta)


# Creación de la tabla
# tabla_empleados = 'EmployeePerformance'
# columnas = [
#     'id INT AUTO_INCREMENT PRIMARY KEY',
#     'deparment VARCHAR(50)',
#     'performance_score DECIMAL',
#     'years_with_company INT',
#     'salary DECIMAL'
# ]

# def create_table(tabla, columnas):
#     cursor.execute(f"DROP TABLE IF EXISTS {tabla}") # Elimina la tabla si ya existe
#     cursor.execute(f"CREATE TABLE {tabla} ({', '.join(columnas)})") # Crea la tabla
#     if cursor:
#         print(f"Se ha creado la tabla {tabla}")


# create_table(tabla_empleados, columnas)