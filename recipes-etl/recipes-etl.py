'''
pyspark recipes-etl

by Alexander Shinkov (@ashinkov)

Apache Spark
Download the following dataset of Open Recipes
https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json

Write an Apache Spark application in Python that reads the recipes json, extracts every recipe that has "beef"
as one of the ingredients.
Add an extra field to each of the extracted recipes with the name difficulty.
The difficulty field would have a value of "Hard" if the total of prepTime and cookTime is greater than 1 hour,
"Medium" if the total is between 30 minutes and 1 hour, "Easy" if the total is less than 30 minutes, and "Unknown"
otherwise.

The resulting dataset saved as parquet file.
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import regexp_extract, col, udf, split
import urllib.request


class recipes_etl(object):

    def __init__(self):
        download = recipes_etl.download()
        session = recipes_etl.create_session()
        transformation = recipes_etl.transformation(session)
        last = recipes_etl.last(transformation)
        recipes_etl.save_to_parquet(last)

    # Download the source-file
    #
    @staticmethod
    def download():
        urllib.request.urlretrieve('https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json', 'recipes.json')

    # Create Spark session
    #
    @staticmethod
    def create_session():
        spark = SparkSession \
            .builder \
            .appName("HelloFresh_ashinkov_recipes-etl") \
            .config("spark.some.config.option") \
            .getOrCreate()

        return spark

    # transformation block
    #
    @staticmethod
    def transformation(spark):

        # Read .json to df
        #
        df = spark.read.json("recipes.json")

        # Filter df - create a new df_filtered with 'beef' (apply upper_case before to get all matches properly)
        #
        df_filtered = df.filter("UPPER(ingredients) like UPPER('%beef%')")

        # splitting cookTime and prepTime columns into time_hours_minutes to get rid of "PT"
        #
        split_col_cook_time = split(df_filtered['cookTime'], 'PT')
        split_col_prep_time = split(df_filtered['prepTime'], 'PT')

        df_filtered_splitted_cook_time_prep_time = df_filtered\
            .withColumn('time_hours_minutes_cookTime', split_col_cook_time.getItem(1)) \
            .withColumn('time_hours_minutes_prepTime', split_col_prep_time.getItem(1))

        # regexp patterns for hours and minutes
        #
        hour_pattern = '([0-9]*)[H]{1}'
        minutes_pattern = '([0-9]*)[M]{1}'

        # regexp extract and cast to int for hours_cookTime, minutes_cookTime, hours_prepTime, minutes_prepTime
        #
        result_cook_time_prep_time = df_filtered_splitted_cook_time_prep_time\
            .withColumn('hours_cookTime', regexp_extract(col('time_hours_minutes_cookTime'), hour_pattern, 1)
                        .cast(IntegerType())) \
            .withColumn('minutes_cookTime', regexp_extract(col('time_hours_minutes_cookTime'), minutes_pattern, 1)
                        .cast(IntegerType())) \
            .withColumn('hours_prepTime', regexp_extract(col('time_hours_minutes_prepTime'), hour_pattern, 1)
                        .cast(IntegerType())) \
            .withColumn('minutes_prepTime', regexp_extract(col('time_hours_minutes_prepTime'), minutes_pattern, 1)
                        .cast(IntegerType()))

        # convert results to minutes and get the total sum = cookTime + prepTime in minutes
        #
        result_cooktime_preptime_minutes = result_cook_time_prep_time.na.fill(0)\
            .withColumn('result_time_minutes_cookTime', col('hours_cookTime') * 60 + col('minutes_cookTime')) \
            .withColumn('result_time_minutes_prepTime', col('hours_prepTime') * 60 + col('minutes_prepTime')) \
            .withColumn('total_time_cook_prep_minutes', col('result_time_minutes_cookTime') + col('result_time_minutes_prepTime'))
        return result_cooktime_preptime_minutes

    # function to determine the recipe complexity level using the rule described in the task
    #
    @staticmethod
    def defining_difficulty(value):
        if value > 60:
            return 'Hard'
        elif value > 30 & value < 60:
            return 'Medium'
        elif value < 30:
            return 'Easy'
        else:
            return 'Unknown'

    # Add a new column to df_filtered with 'beef' called 'difficulty'
    #
    @staticmethod
    def last(result_cooktime_prep_time_minutes):
        defining_difficulty_udf = udf(recipes_etl.defining_difficulty, StringType())
        final_result_set = result_cooktime_prep_time_minutes.withColumn('difficulty', defining_difficulty_udf(col('total_time_cook_prep_minutes')))
        return final_result_set

    # save the result to parquet
    #
    @staticmethod
    def save_to_parquet(final_result_set):
        final_result_set.write.format("parquet").mode('overwrite').save("recipes.parquet")


# Execution
#
run = recipes_etl()

