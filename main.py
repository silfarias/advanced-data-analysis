import matplotlib.pyplot as plt
import pandas as pd
import MySQLdb
import sys
import csv

class MySQLConnection:
    def __init__(self, host="localhost", user="root", password=""):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = MySQLdb.connect(self.host, self.user, self.password)
            self.cursor = self.connection.cursor()
            print("Conexión correcta.")
        except MySQLdb.Error as e:
            print("No se pudo conectar: ", e)
            sys.exit(1)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Conexión cerrada.")

    def execute(self, query):
        try:
            self.cursor.execute(query)
        except MySQLdb.Error as e:
            print(f"Error al ejecutar la consulta: {e}")

    def commit(self):
        try:
            self.connection.commit()
        except:
            self.connection.rollback()

    def create_dataframe(self, query):
        return pd.read_sql(query, self.connection)

class DatabaseManager:
    def __init__(self, db_connection: MySQLConnection, db_name):
        self.db_connection = db_connection
        self.db_name = db_name

    def create_database(self):
        self.db_connection.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name};")
        print(f'Se creó correctamente la base de datos "{self.db_name}"')

    def select_database(self):
        self.db_connection.execute(f"USE {self.db_name};")
        print(f'Se seleccionó la base de datos "{self.db_name}"')

    def create_table(self, table_name, columns):
        self.db_connection.execute(f'DROP TABLE IF EXISTS {table_name};')
        self.db_connection.execute(f"CREATE TABLE {table_name} ({', '.join(columns)});")
        print(f'Se ha creado la tabla {table_name}')

class DataInserter:
    def __init__(self, db_connection: MySQLConnection, table_name):
        self.db_connection = db_connection
        self.table_name = table_name

    def insert_data_from_csv(self, csv_file_path):
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='"')
            next(reader)  # Saltar la cabecera
    
            cursor = self.db_connection.cursor  # Usa el cursor existente en lugar de crear uno nuevo
    
            try:
                for row in reader:
                    cursor.execute(f"INSERT INTO {self.table_name} (id, department, performance_score, years_with_company, salary) VALUES (%s, %s, %s, %s, %s)", row[0:5])
                self.db_connection.commit()
                print('Datos insertados correctamente.')
            except csv.Error as e:
                print(f'Ha ocurrido un error al insertar los datos: {e}')
                self.db_connection.rollback()
                sys.exit(1)
            except MySQLdb.Error as e:
                print(f'Error al ejecutar la consulta: {e}')
                self.db_connection.rollback()
                sys.exit(1)
            finally:
                cursor.close()  # Cerrar el cursor después de la operación



class StatisticsCalculator:
    def __init__(self, df):
        self.df = df

    def calculate_performance_stats(self):
        return self.df.groupby('department').agg(
            media=('performance_score', 'mean'),
            mediana=('performance_score', 'median'),
            desv_estandar=('performance_score', 'std')
        ).reset_index()

    def calculate_salary_stats(self):
        return self.df.groupby('department')['salary'].agg([
            'mean', 'median', 'std'
        ])

    def calculate_employee_count(self):
        return self.df.groupby('department').size()

    def calculate_correlation(self, column1, column2):
        return self.df[column1].corr(self.df[column2])

class Plotter:
    def __init__(self, df):
        self.df = df

    def plot_histogram(self, department, column, bins=10):
        data = self.df[self.df['department'] == department][column]
        plt.figure(figsize=(9, 5))
        plt.hist(data, bins=bins, alpha=0.5, edgecolor='black')
        plt.title(f"{column} para el Departamento '{department}'")
        plt.xlabel(column)
        plt.ylabel('Frecuencia')
        plt.grid(True)
        plt.show()

    def plot_scatter(self, x_column, y_column):
        plt.figure(figsize=(9, 5))
        plt.scatter(self.df[x_column], self.df[y_column], alpha=0.7, edgecolors='w', s=100)
        plt.title(f'{x_column} vs. {y_column}')
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.grid(True)
        plt.show()

# Main function
def main():
    # Conexión a la base de datos
    db_connection = MySQLConnection()
    db_connection.connect()

    # Gestión de la base de datos y tabla
    db_manager = DatabaseManager(db_connection, 'companydata')
    db_manager.create_database()
    db_manager.select_database()

    tabla_empl = 'employeeperformance'
    columnas = [
        'id INT PRIMARY KEY AUTO_INCREMENT',
        'department VARCHAR(255)',
        'performance_score FLOAT',
        'years_with_company INT',
        'salary FLOAT'
    ]
    db_manager.create_table(tabla_empl, columnas)

    # Inserción de datos desde un archivo CSV
    data_inserter = DataInserter(db_connection, tabla_empl)
    data_inserter.insert_data_from_csv('employeeperformance.csv')

    # Creación de DataFrame desde la base de datos
    df = db_connection.create_dataframe(f'SELECT * FROM {tabla_empl}')

    # Cálculo de estadísticas
    stats_calculator = StatisticsCalculator(df)
    performance_stats = stats_calculator.calculate_performance_stats()
    salary_stats = stats_calculator.calculate_salary_stats()
    employee_count = stats_calculator.calculate_employee_count()

    print('\nEstadísticas según el Performance Score:')
    print(performance_stats)

    print('\nEstadísticas según el Salario:')
    print(salary_stats)

    print('\nNúmero total de empleados por departamento:')
    print(employee_count)

    print('\nCorrelación entre years_with_company y performance_score:')
    print(stats_calculator.calculate_correlation('years_with_company', 'performance_score'))

    print('\nCorrelación entre salary y performance_score:')
    print(stats_calculator.calculate_correlation('salary', 'performance_score'))

    # Graficar resultados
    plotter = Plotter(df)
    plotter.plot_histogram('Legal', 'performance_score')
    plotter.plot_scatter('years_with_company', 'performance_score')
    plotter.plot_scatter('salary', 'performance_score')

    # Cerrar la conexión
    db_connection.close()

if __name__ == "__main__":
    main()
