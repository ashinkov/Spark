1. Download the following dataset of [Open Recipes](https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json)
2. Write an Apache Spark application in **Python** that reads the recipes json, extracts every recipe that has **"beef"** as one of the ingredients.
3. Add an extra field to each of the extracted recipes with the name `difficulty`. The `difficulty` field would have a value of "Hard" if the total of `prepTime` and `cookTime` is greater than 1 hour, "Medium" if the total is between 30 minutes and 1 hour, "Easy" if the total is less than 30 minutes, and "Unknown" otherwise.
4. The resulting dataset should be saved as parquet file.
5. Your Spark application could run in Stand-alone mode or it could run on YARN.

