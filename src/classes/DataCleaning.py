from pyspark.sql.functions import from_utc_timestamp, date_format, lit
from pyspark.sql.types import TimestampType
from datetime import datetime
class DataProcessing:
    def __init__(self,spark, base_path, dbutils):
        self.spark = spark
        self.dbutils = dbutils
        self.base_path = base_path
        self._tables_name = []
        self.update_tables_name()
        
    
    @property
    def tables_name(self):
        return self._tables_name
    
    def update_tables_name(self) -> None:
        for i in self.dbutils.fs.ls(self.base_path):
            self._tables_name.append(i.name.split("/")[0])

    
    def find_latest_file(self,table_name):
        """
        Find the latest file in the table directory
        """
        files = self.dbutils.fs.ls(f"{self.base_path}/{table_name}")
        if not files : 
            return None
        latest_file = max(files, key= lambda file: file.name)
        return latest_file.path+table_name + ".parquet"
    
    @staticmethod
    def time_processing(df):
        """
        Convert the timestamp column to date column
        """
        column = df.columns

        for col in column:
            if "Date" in col or "date" in col:
                df = df.withColumn(col, 
                                   date_format(
                                       from_utc_timestamp(df[col].cast(TimestampType()),
                                                           "UTC"),
                                                             "yyyy-MM-dd"))
        return df
                

    @staticmethod
    def drop_duplicates(df):
        """
        Drop duplicates in the dataframe
        """
        df = df.dropDuplicates()
        return df
    
    @staticmethod
    def drop_null(df):
        """
        Drop null values in the dataframe
        """
        df = df.dropna()
        return df
    
    @staticmethod
    def add_time_processing(df):
        """
        Add time processing to the dataframe
        """
        df = df.withColumn("date_processed", lit(datetime.now().strftime('%Y-%m-%d')))
        return df
    
    def bronze_to_silver(self) -> None:
        """
        Transform the bronze tables to silver delta tables
        """
        for table in self._tables_name:
            df = self.spark.read.parquet(self.find_latest_file(table_name=table))
            df = self.time_processing(df)
            df = self.drop_duplicates(df)
            df = self.drop_null(df)
            df = self.add_time_processing(df)
            silver_path = self.base_path.replace("bronze","silver") + table
            (df.write
             .format("delta")
             .partitionBy("date_processed")
             .save(silver_path))
        return None 

    @staticmethod
    def rename_columns(df):
        """
        Rename the columns of the dataframe
        """
        columns_name = df.columns
        for old_col_name in columns_name:
            new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
            #Change the name of column
            df = df.withColumnRenamed(old_col_name, new_col_name)
        return df
    
    def find_location_silver(self,table_name):
        """
        Find the location of the silver table
        """
        #base_path = '/mnt/data/bronze/sql_server/SalesLT/'
        #silver_path = '/mnt/data/silver/sql_server/SalesLT/Address/'
        base_path = self.base_path.replace("bronze","silver")
        return base_path + table_name + "/"

    def silver_to_gold(self) -> None:
        """
        Transform the silver tables to gold delta tables
        """
        for table in self._tables_name:
            df = self.spark.read.parquet(self.find_location_silver(table_name=table))
            df = self.rename_columns(df)
            gold_path = self.base_path.replace("silver","gold") + table
            (df.write
             .format("delta")
             .partitionBy("date_processed")
             .save(gold_path))
        return None