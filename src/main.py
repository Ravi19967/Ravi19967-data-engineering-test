import os
from datetime import datetime

import isodate
from dynaconf import settings
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, dayofmonth, mean, month, round, trim,
                                   udf, when, year)
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField,
                               StructType)
from pyspark.sql.utils import PythonException

from src.ETLInterface import ETLInterface


class Task1(ETLInterface):
    """
    Task1 implements ETLInterface. Using Apache Spark and Python, reads the source data, pre-processes it and persists it to ensure optimal structure and performance for further processing.
    The source events are located in the input folder.
    The preprocessed events are located in output folder
    """

    def __init__(self, spark: SparkSession) -> None:
        self.events_schema = StructType(
            [
                StructField("name", StringType()),
                StructField("ingredients", StringType()),
                StructField("url", StringType()),
                StructField("image", StringType()),
                StructField("cookTime", StringType()),
                StructField("recipeYield", StringType()),
                StructField("datePublished", StringType()),
                StructField("prepTime", StringType()),
                StructField("description", StringType()),
            ]
        )
        self.spark = spark
        self.spark.sparkContext.setLogLevel("info")
        log4jLogger = self.spark._jvm.org.apache.log4j
        self.LOGGER = log4jLogger.LogManager.getLogger(__name__)
        self.LOGGER.info("pyspark script logger initialized")
        self.LOGGER.info(f"Task1 Started")

    def lambda_PT_time_udf(self) -> callable:
        """
        Python lambda to convert ISO-8601 duration to minutes
        """
        return (
            lambda timestamp: (isodate.parse_duration(timestamp).seconds // 60)
            if timestamp
            else None
        )

    def lambda_str_date_udf(self) -> callable:
        """
        Python lambda to convert string to DateType
        """
        return lambda x: datetime.strptime(x, "%Y-%m-%d")

    def read_data(self) -> None:
        """
        Reads data from input path for raw events
        """
        try:
            self.LOGGER.info(f"Reading Data from: {settings.EVENTS_PATH}")
            self.data = self.spark.read.json(
                f"{settings.EVENTS_PATH}", schema=self.events_schema
            )
        except (PythonException, TypeError) as error:
            self.LOGGER.error(f"Error reading data from {settings.EVENTS_PATH}")
            raise

    def transform_data(self) -> None:
        """
        Transfroms imported data.
        -> Checks data for null
        -> Converts string to date and extracts year, month and day out of it for partition
        -> Converts ISO 8601 duration to minutes for prepTime and cookTime
        """
        self.LOGGER.info(f"Transforming Data: Cleaning up whitespace")
        ## Strips strings of whitespace and replace empty string with None
        self.data = self.data.select([trim(col(c)).alias(c) for c in self.data.columns])
        self.data = self.data.select(
            [
                when(col(c) == "", None).otherwise(col(c)).alias(c)
                for c in self.data.columns
            ]
        )

        self.LOGGER.info(
            f"Transforming Data: Converting str to date and creating partition columns"
        )
        ## Converts string to date and extracts year, month and day out of it for partition
        parse_str_date_udf = udf(self.lambda_str_date_udf(), DateType())
        self.data = self.data.withColumn(
            "datePublished_parsed", parse_str_date_udf(col("datePublished"))
        )
        self.data = self.data.drop("datePublished").withColumnRenamed(
            "datePublished_parsed", "datePublished"
        )
        self.data = self.data.withColumn("year", year(col("datePublished")))
        self.data = self.data.withColumn("month", month(col("datePublished")))
        self.data = self.data.withColumn("day", dayofmonth(col("datePublished")))

        ## Converts ISO 8601 duration to minutes for prepTime and cookTime
        self.LOGGER.info(
            f"Transforming Data: Converting ISO 8601 duration to minutes for prepTime and cookTime"
        )
        parse_time_udf = udf(self.lambda_PT_time_udf())
        self.data = self.data.withColumn(
            "cookTime_parsed", parse_time_udf(col("cookTime"))
        )
        self.data = self.data.drop("cookTime").withColumnRenamed(
            "cookTime_parsed", "cookTime_in_minutes"
        )
        self.data = self.data.withColumn(
            "cookTime_in_minutes", self.data.cookTime_in_minutes.cast(IntegerType())
        )
        self.data = self.data.withColumn(
            "prepTime_parsed", parse_time_udf(col("prepTime"))
        )
        self.data = self.data.drop("prepTime").withColumnRenamed(
            "prepTime_parsed", "prepTime_in_minutes"
        )
        self.data = self.data.withColumn(
            "prepTime_in_minutes", self.data.cookTime_in_minutes.cast(IntegerType())
        )

    def write_data(self) -> None:
        """
        Writes data to output/PreProcessedData based on partition key of year,month and day of datePublished
        """
        try:
            self.LOGGER.info(
                f"Writing Data to: {settings.DATALAKE_PATH}/PreProcessedData"
            )
            self.data.write.option("header", True).option(
                "truncate", "true"
            ).partitionBy(["year", "month", "day"]).mode("overwrite").save(
                f"{settings.DATALAKE_PATH}/PreProcessedData", format="parquet"
            )
        except (PythonException, TypeError) as error:
            self.LOGGER.error(
                f"Error writing data to {settings.DATALAKE_PATH}/PreProcessedData"
            )
            raise


class Task2(ETLInterface):
    """
    Task2 implements ETLInterface.
    -> Extracts only recipes that have beef as one of the ingredients.
    -> Calculate average cooking time duration per difficulty level.
    -> Persist dataset as CSV to the output folder.
    -> The dataset has 2 columns: difficulty,avg_total_cooking_time.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.spark.sparkContext.setLogLevel("info")
        log4jLogger = self.spark._jvm.org.apache.log4j
        self.LOGGER = log4jLogger.LogManager.getLogger(__name__)
        self.LOGGER.info("pyspark script logger initialized")
        self.LOGGER.info(f"Task2 Started")

    def read_data(self) -> None:
        """
        Reads PreProcessedData from output/processedpath where prepTime and cookTime is not null and ingredients contain beef
        """
        try:
            self.LOGGER.info(
                f"Read Data from: {settings.DATALAKE_PATH}/PreProcessedData"
            )
            self.data = (
                self.spark.read.parquet(f"{settings.DATALAKE_PATH}/PreProcessedData")
                .filter(
                    "prepTime_in_minutes IS NOT NULL AND cookTime_in_minutes IS NOT NULL AND ingredients LIKE '%Beef%'"
                )
                .select("cookTime_in_minutes", "prepTime_in_minutes")
            )
        except (PythonException, TypeError) as error:
            self.LOGGER.error(
                f"Error reading data from {settings.DATALAKE_PATH}/PreProcessedData"
            )
            raise

    def transform_data(self) -> None:
        """
        Implements transformation of data
        """
        self.LOGGER.info(f"Transforming Data")
        return self.find_average_duration_for_difficulty_level()

    def find_average_duration_for_difficulty_level(self) -> None:
        """
        -> Implements total_cook_time = cookTime + PrepTime
        -> Implements difficulty:
                                        -> easy - less than 30 mins
                                        -> medium - between 30 and 60 mins (both inclusive i.e. [30,60])
                                        -> hard - more than 60 mins
        -> Aggregate average total_cook_time grouped on difficulty
        """
        self.LOGGER.info(f"Transforming Data: Adding column total_cook_time")
        self.data = self.data.withColumn(
            "total_cook_time",
            self.data.cookTime_in_minutes + self.data.prepTime_in_minutes,
        )
        self.LOGGER.info(f"Transforming Data: Adding column difficulty")
        self.data = self.data.withColumn(
            "difficulty",
            when(self.data.total_cook_time < 30, "easy")
            .when(
                (self.data.total_cook_time >= 30) & (self.data.total_cook_time <= 60),
                "medium",
            )
            .otherwise("hard"),
        )
        self.data = self.data.select("difficulty", "total_cook_time").repartition(
            3, "difficulty"
        )
        self.LOGGER.info(
            f"Transforming Data: Calculating average duration grouped by difficulty"
        )
        self.data = self.data.groupBy("difficulty").agg(
            round(mean("total_cook_time"), 2).alias("avg_total_cooking_time")
        )
        self.data = self.data.repartition(1)

    def write_data(self) -> None:
        """
        Writes data to output/AverageDurationData
        """
        try:
            self.LOGGER.info(
                f"Writing Data to: {settings.DATALAKE_PATH}/AverageDurationData"
            )
            self.data.write.option("header", True).option("truncate", "true").mode(
                "overwrite"
            ).save(f"{settings.DATALAKE_PATH}/AverageDurationData", format="parquet")
        except (PythonException, TypeError) as error:
            self.LOGGER.error(
                f"Error writing data to {settings.DATALAKE_PATH}/AverageDurationData"
            )
            raise


if __name__ == "__main__":

    ## Regular Spark job executed on a Docker container
    os.environ["PYSPARK_PYTHON"] = "./pyspark_venv.pex"
    spark = (
        SparkSession.builder.appName("hellofresh_test")
        .master(f"spark://{settings.SPARK_HOST_URL}")
        .config("spark.files", "pyspark_venv.pex")
        .getOrCreate()
    )

    ## Job Flow
    Task1_run = Task1(spark)
    Task1_run.read_data()
    Task1_run.transform_data()
    Task1_run.write_data()
    Task2_run = Task2(spark)
    Task2_run.read_data()
    Task2_run.transform_data()
    Task2_run.write_data()

    ## Stop Spark Session
    spark.stop()
