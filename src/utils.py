# Databricks notebook source
def getUnprocessedFiles(spark,directory,obj):
    """
    Returns a list of unprocessed files from a given directory based on the last recorded modification timestamp 
    in the 'bronze.processed_files' tracking table for a specific object (e.g., 'product', 'orders').

    Parameters:
    ----------
    spark : SparkSession
        The active Spark session used to query the tracking table.
        
    directory : str
        The DBFS path to the directory containing files (e.g., '/mnt/data/incoming/products/').
        
    obj : str
        A label representing the type of object being processed (e.g., 'product', 'orders', 'customers').
        Used to filter tracking table records.

    Returns:
    -------
    List[Dict[str, Any]]
        A list of dictionaries, each containing:
        - file_path: Full path to the unprocessed file
        - object: The object type passed as input
        - modification_time: Last modified timestamp of the file (epoch millis)
    
    Notes:
    -----
    - Relies on a Delta table `bronze.processed_files` with columns: 
        `file_path STRING`, `modification_time TIMESTAMP`, `object STRING`.
    - Uses `dbutils.fs.ls()` which works only in Databricks runtime.
    - Assumes timestamp-based deduplication (latest modified file wins).
    """
    max_ts = spark.sql(f"select max(modification_time) from bronze.processed_files where object='{obj}'").collect()[0][0]
    files = dbutils.fs.ls(directory)
    if max_ts is not None:
        files = [f for f in files if f.modificationTime > max_ts]
    files = [{"file_path":f.path, "object":"product","modification_time":f.modificationTime} for f in files]
    return files

# COMMAND ----------

def mark_file_process(spark,files):
    df = spark.createDataFrame(files)
    df.write.mode('append').saveAsTable('bronze.processed_files')