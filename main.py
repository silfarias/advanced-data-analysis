import matplotlib.pyplot as plt
import pandas as pd
import MySQLdb
import sys
import csv

# establecemos conexión con el servidor mysql
def connect_mysql():
    try:
        db = MySQLdb.connect("localhost","root","" )
    except MySQLdb.Error as e:
        print("No se pudo conectar: ", e)
        sys.exit(1) 
    print("Conexión correcta.")
    return db


# creamos base de datos
def creacion_database(cursor, db_name):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        print(f'Se creó correctamente la base de datos "{db_name}"')
    except MySQLdb.Error as e:
        print(f"Error al crear la base de datos: {e}")


# seleccionamos la base de datos a usar
def select_database(cursor, db_name):
    try:
        cursor.execute(f"USE {db_name};")
        print(f'Se seleccionó la base de datos "{db_name}"')
    except MySQLdb.Error as e:
        print(f"Error al seleccionar la base de datos: {e}")


# creamos la tabla en la base de datos
def creacion_tabla(cursor, tabla, columnas):
    try:
        cursor.execute(f'DROP TABLE IF EXISTS {tabla};')
        cursor.execute(f"CREATE TABLE {tabla} ({', '.join(columnas)});")
        print(f'Se ha creado la tabla {tabla}')
    except MySQLdb.Error as e:
        print(f"Error al crear la tabla: {e}")


# asignamos nombre a la tabla y sus columnas con sus respectivos tipos de datos
tabla_empl = 'employeeperformance'
columnas = [
    'id INT PRIMARY KEY AUTO_INCREMENT',
    'department VARCHAR(255)',
    'performance_score FLOAT',
    'years_with_company INT',
    'salary FLOAT'
]

# creamos cursor
db = connect_mysql()
cursor = db.cursor()


creacion_database(cursor, 'companydata')
select_database(cursor, 'companydata')
creacion_tabla(cursor, tabla_empl, columnas)


# inserción de datos a la tabla mediante la lectura de un archivo csv
def insertar_datos(cursor, db):
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
        

insertar_datos(cursor, db)

 # ejecuta una consulta sql y carga el resultado en un DataFrame directamente
def create_dataframe(db, tabla):
    consulta = f'SELECT * FROM {tabla}'
    df = pd.read_sql(consulta, db)
    return df

df = create_dataframe(db, tabla_empl)

# cerramos el cursor y conexión
cursor.close()
db.close()


# estadisticas de performance_score
def calculos_perf_score(df):
    stats_metascore = df.groupby('department').agg(
        media=('performance_score', 'mean'),
        mediana=('performance_score', 'median'),
        desv_estandar=('performance_score', 'std'),
    ).reset_index()
    return stats_metascore


estad_perf_score = calculos_perf_score(df)
print('\nEstadisticas segun el Performance Score:')
print(estad_perf_score)

# estadisticas de salary
def calculos_salary(df):
    stats_salary = df.groupby('department')['salary'].agg([
        'mean',
        'median',
        'std'
    ])
    return stats_salary

estad_salary = calculos_salary(df)
print('\nEstadisticas segun el Salario:')
print(estad_salary)


# Numero de empleados por departamento
def empl_depart(df):
    return df.groupby('department').size()

total_empl = empl_depart(df)
print('\nNúmero total de empleados por departamento:')
print(total_empl)


# correlación entre years_with_company
def correlacion_1(df):
    return df['years_with_company'].corr(df['performance_score'])

print('\nCorrelación entre years_with_company y performance_score: ')
print(correlacion_1(df))


# correlacion entre salary y performace_score
def correlacion_2(df):
    return df['salary'].corr(df['performance_score'])

print('\nCorrelación entre salary y performance_score: ')
print(correlacion_2(df))


# graficos

# histograma del performance_score por departamento
def hist_department(df):
    datos = df[df['department'] == 'Legal']['performance_score'] # elegimos el departamento 'Legal'
    plt.figure(figsize=(9, 5))
    plt.hist(datos, bins=10, alpha=0.5, edgecolor='black')
    plt.title("Performance Score para el Departamento 'Legal' ")
    plt.xlabel('Performance Score')
    plt.ylabel('Frecuencia')
    plt.grid(True)
    plt.show()

hist_department(df)

# gráfico de dispersión de years_with_company vs. performance_score
def disp_years_score(df):
    plt.figure(figsize=(9, 5))
    plt.scatter(df['years_with_company'], df['performance_score'], alpha=0.7, edgecolors='w', s=100)
    plt.title('Years with Company vs. Performance Score')
    plt.xlabel('Years with Company')
    plt.ylabel('Performance Score')
    plt.grid(True)
    plt.show()

disp_years_score(df)

# gráfico de dispersión de salary vs. performance_score
def disp_salary_score(df):
    plt.figure(figsize=(9, 5))
    plt.scatter(df['salary'], df['performance_score'], alpha=0.7, edgecolors='w', s=100)
    plt.title('Salary vs. Performance Score')
    plt.xlabel('Salary')
    plt.ylabel('Performance Score')
    plt.grid(True)
    plt.show()

disp_salary_score(df)

