import logging
import azure.functions as func
import random
import pyodbc
import os
from datetime import datetime

app = func.FunctionApp()

# Number of sensors to simulate
N = 10

# Database connection string
connection_string = "Driver={ODBC Driver 18 for SQL Server};" + os.getenv("SqlConnectionString")

# Trigger at a regular iterval every 30 seconds
@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def SensorDataFunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    # Simulate sensor data for N sensors
    for _ in range(N):
        # Generate random data for each sensor
        co2_level = round(random.uniform(300, 600), 2)  # CO2 level in ppm
        temperature = round(random.uniform(18.0, 30.0), 2)  # Temperature in °C
        humidity = round(random.uniform(30.0, 70.0), 2)  # Humidity in %
        timestamp = datetime.now()  # Current timestamp

        # SQL query to insert the data
        sql_query = """
        INSERT INTO SensorData (CO2Level, Temperature, Humidity, Timestamp)
        VALUES (?, ?, ?, ?)
        """

        # Connect to the database and insert data
        try:
            with pyodbc.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_query, (co2_level, temperature, humidity, timestamp))
                    conn.commit() 
                    logging.info(f'Data inserted successfully: CO2={co2_level}, Temp={temperature}, Humidity={humidity}, Timestamp={timestamp}')
        # If an error occurs it is logged
        except Exception as e:
            logging.error(f"Error inserting data: {str(e)}")
