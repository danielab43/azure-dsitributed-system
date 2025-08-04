import logging
import azure.functions as func
import pyodbc
import os

app = func.FunctionApp()

# Database connection string
connection_string = "Driver={ODBC Driver 18 for SQL Server};" + os.getenv("SqlConnectionString")

# SQL trigger to start the function when a change in SensorData table is detected
@app.function_name(name="StatsFunction")
@app.sql_trigger(
    arg_name="SensorChanges",
    table_name="SensorData",
    connection_string_setting="SqlConnectionString" # Connection string got from environment variable
)
def StatsTriggerFunction(SensorChanges: str) -> None:
    logging.info("Changes detected in SensorData table.")


    # Attempt to connect to database
    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                # Define stat types which corrosponds to columns
                stats = ["CO2Level", "Temperature", "Humidity",]

                for stat_type in stats:
                    # Compute statistics for the current column
                    query = f"""
                        SELECT 
                            AVG({stat_type}) AS AverageValue,
                            MIN({stat_type}) AS MinValue,
                            MAX({stat_type}) AS MaxValue,
                            COUNT({stat_type}) AS TotalCount
                        FROM SensorData
                        WHERE {stat_type} IS NOT NULL
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result:
                        # Fetch and round the statistics
                        average = round(result[0], 2) 
                        min_value = round(result[1], 2) 
                        max_value = round(result[2], 2) 
                        count = result[3]  

                        # Check if the StatType already exists
                        check_query = "SELECT COUNT(*) FROM AggregatedStats WHERE StatType = ?"
                        cursor.execute(check_query, (stat_type,))
                        exists = cursor.fetchone()[0]

                        if exists:
                            # Update existing record
                            update_query = """
                                UPDATE AggregatedStats
                                SET Average = ?, 
                                    MinValue = ?, 
                                    MaxValue = ?, 
                                    Count = ?, 
                                    LastUpdated = GETDATE()
                                WHERE StatType = ?
                            """
                            cursor.execute(update_query, (average, min_value, max_value, count, stat_type))
                            logging.info(f"Updated stats for {stat_type}")
                            logging.info(f'Data inserted successfully: Average={average}, MinValue={min_value}, MaxValue={max_value}, Count={count}')
                        else:
                            # Insert new record
                            insert_query = """
                                INSERT INTO AggregatedStats (StatType, Average, MinValue, MaxValue, Count, LastUpdated)
                                VALUES (?, ?, ?, ?, ?, GETDATE())
                            """
                            cursor.execute(insert_query, (stat_type, average, min_value, max_value, count))
                            logging.info(f"Inserted stats for {stat_type}")
                            logging.info(f'Data inserted successfully: Average={average}, MinValue={min_value}, MaxValue={max_value}, Count={count}')
                        
                    # Commit all changes
                    conn.commit

    # If an error occurs it is logged
    except Exception as e:
        logging.error(f"Error while updating stats: {e}")