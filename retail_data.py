import os
from pathlib import Path
import logging
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import col

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetailData:

    def __init__(self,
                 spark_session: SparkSession,
                 folder_loc: str = "./"
                ):
        """
        Retail data representation, to be called and scaled as a library if needed.

        :param spark_session: Spark session instance (external to save reinitialization time).
        :param folder_loc: Base folder location where 'Sales_Data' resides (default: current directory).
        """
        
        self.folder_loc = Path(folder_loc)
        self.sales_data_folder = self.folder_loc / 'Sales_Data'
        self.output_path = self.folder_loc / 'cleansed'

        self.spark = spark_session
        self.df = self._load_data
        
        self._clean_data

    @property
    def _load_data(self) -> DataFrame:
        """
        Load and combine all CSV files in the sales data folder into a single DataFrame.
        
        :return: DataFrame of the combined data.
        """
        if not self.sales_data_folder.exists():
            raise FileNotFoundError(f"Sales data folder not found at: {self.sales_data_folder}")

        all_dfs = []
        for file in os.listdir(self.sales_data_folder):
            file_path = self.sales_data_folder / file
            if file_path.suffix == '.csv':
                logger.info(f"Loading file: {file}")
                df = self.spark.read.option("header", True).csv(str(file_path))
                all_dfs.append(df)

        if not all_dfs:
            raise ValueError("No CSV files found in the sales data folder.")

        combined_df = all_dfs[0]
        for df in all_dfs[1:]:
            combined_df = combined_df.unionByName(df)

        return combined_df

    @property
    def _clean_data(self):
        """Private method to clean and reorganize the data."""
        
        required_cols = ['Order ID', 'Quantity Ordered', 'Price Each', 'Order Date']
        missing_cols = [c for c in required_cols if c not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        self.df = self.df.filter(~col('Order ID').contains('Order ID'))  # Remove repeated headers

        # Convert every column into the correct type
        self.df = (
            self.df
            .withColumn('Order ID', col('Order ID').cast('int'))  # The Order ID are integers
            .withColumn('Quantity Ordered', col('Quantity Ordered').cast('int'))  # The Quantity Ordered are integers
            .withColumn('Price Each', col('Price Each').cast('float'))  # The Price Each are floats
            # The Order Date are timestamps with the format 'MM/dd/yy HH:mm'
            .withColumn('Order Date', F.to_timestamp(col('Order Date'), 'MM/dd/yy HH:mm'))
        )
        
        self.df = self.df.na.drop()  # Remove rows with NULLs
        self.df = self.df.dropDuplicates()  # Remove all duplicate rows
        
        logger.info("Data cleaning completed.")
        logger.info(f"Final schema:\n{self.df.printSchema()}")

    def save_data(self,
                  format: str = 'parquet'
                 ):
        """
        Save the data.

        :param format: String with the format in which we want to save the data (default: parquet).        
        """
        # coalesce(1) will combine all partitions into a single file
        self.df.coalesce(1).write.mode('overwrite').save(path=str(self.output_path), format=format)
        logger.info("Data successfully saved.")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Sales Data Cleaning").getOrCreate()
    retail_data = RetailData(spark_session=spark, folder_loc="./")
    retail_data.save_data(format='parquet')