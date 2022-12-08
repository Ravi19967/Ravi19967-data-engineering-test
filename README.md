# Introduction
This repo reads raw events data from input folder and outputs the finally data in output/AverageDurationData regarding average cooking duration per difficulty level as defined below:
* easy - less than 30 mins
* medium - between 30 and 60 mins
* hard - more than 60 mins.

# Run
To run this solution. You should have running docker and then go to the root of directory and run `docker compose up`

# Approach

## Infrastructure
* Created a docker-compose to start Spark-master and worker nodes
* Created a DockerFile to build an image for logic-executor with required configuration
* Use `PEX` to pass venv along with code to the spark nodes. The requirements for the same are logged in `spark-requirements.txt` and the logic-executor's requirements are logged in `requirements.txt`. More details on PEX [here.](https://github.com/pantsbuild/pex#overview)
* To execute this on a perodic basis we could trigger the logic-executor from Data flow Orchestartor like `Airflow` (but i didn't get time to add that here along with `Great Expectations's Test Suite` for data validation)

## Tests
* To execute the tests locally run `python3 -m unittest` from the root of directory. Incase you run into issues check the `PYTHONPATH` for the command

## Code Formating
* The code was cleaned up using `black` and imports were sorted by `isort`

## Configuration Management
* Used `dynaconf` for configuration management. The details of the same can be found in `src/config/settings.toml`

## Logic
The approach can be broken down into 2 main Tasks:
* **Task1** - Pre-Processes the data from raw json and transforms data for further processing. Steps performed in this are as follows:
    * I work with the assumption that the schema of the raw events would remain unchanged and `datePublished` is non nullable
    * Defines the schema for the json input
    * Strips whitespaces from all records and replaces the result data with `None` for empty strings. This lead to discovery of some nulls in `Name`, `prepTime` and `cookTime`
    * Converts date string to `DateType()` for `datePublished` column
    * The `datePublished` column was used as soruce for partition key under the assumption that flow of recipies is consistent across months and days and has not much seasonality to it
    * The year, month and dayofMonth were used as the partition keys from `datePublished`. Though in this case there is so little data that we don't need partition but this is done to accomodate larger volumes of data
    * Converts duration from ISO8601 to minutes for `prepTime` and `cookTime`
    * Finally, the data is written with `overwrite mode` to output/PreProcessedData based on partition key discussed above

* **Task2** - Extracts all recipies with `Beef` and generates `avg_total_cooking_time` wrt to predefined difficulty level. Steps performed in this are as follows:
    * Filter the data to records that contain `Beef` in their `ingredients`
    * I assumed we should not use `null` data in the calculation as if `cookTime` is null than we can't estimate the `total_cooking_time` correctly. For `prepTime` i initally thought `null` could be no preperation time needed but it doesn't seem to be the case as i found some values with 0 minutes in data. So I decieded to ignore any such cases that have `null` in `prepTime` or `cookTime`.
    * Created calculated column `total_cook_time` with the following definition : `total_cook_time = cookTime + PrepTime`
    * Created calculated column `difficulty` based on definition discussed before.
    * Aggreagated data to `difficulty` based on average `total_cook_time`
    * Finally write the data to `output/AverageDurationData` with the follwing columns:
        * `avg_total_cooking_time` rounded of to 2 decimal points
        * `difficulty`

Thanks for reading!
