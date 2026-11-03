# From Apache Quickstart:DataFrame documentation 
from pyspark.sql import SparkSession
# From the functions section of the Apache API reference page in the Apache documentation 
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate() # This line must follow the first import based on Apache Quickstart

# This section is from the Databricks tutorial notebook 
catalog = "cs236"                      
schema = "project"                     
volume = "phase1"      # download line not needed because CSV on computer 
file_name = "nyc_311_800k.csv"         
table_name = "nyc"     # path_volume is not needed here because CSV is on computer
path_table = catalog + "." + schema
print(path_table) 
# spark.read.csv is from the databricks tutorial (step 3)
df = spark.read.csv(f"./{file_name}", header=True, inferSchema=True, sep=",")
df.show() # from Quickstart:Dataframe, Viewing data. Show instead of Display is used because Display is a databricks function. show is from the pyspark documentation
# sorting the column names into groups here using .dtypes from the Apache API reference (Spark SQL -> dataframe). 
# essentially dtypes gets a pair "[('age', 'bigint'), ('name', 'string')]" (taken from dtypes documenation), using that to sort the columns by type
textCols = [] # initializing lists
decCols = []
numCols = []
dateCols = []
 
for col, dtype in df.dtypes:
    if dtype == "string":
        textCols.append(col)
    if dtype in ("float", "double") or dtype.startswith("decimal"): # here, decimals could be represented as decimal(x,y) so check for the prefix with startswith
        decCols.append(col)                                         # this is specifically for longitude and latitude, to check if the longitude and latitude is not invalid later
    if dtype in ("tinyint", "smallint", "int", "bigint", "float", "double") or dtype.startswith("decimal"):  # essentially everything else
        numCols.append(col)
    if dtype in ("timestamp", "date"):
        dateCols.append(col)

# from databricks (step 4) 
df.printSchema()
# from Apache API Reference, use count to get the total rows in the dataframe 
total_rows = df.count()
# from Apache API reference, .columns lists all the column names, used len to get the number of them
print("Rows:", total_rows, "| Columns:", len(df.columns), df.columns)
print("Text columns:   ", textCols)
print("Number columns: ", numCols, "Decimal columns: ", decCols)
print("Date columns:   ", dateCols)
# Display the earliest and latest dates in each date column
for cols in dateCols:
    print("\nDate range: ", cols)
    df.select(
        F.min(cols).alias("Earliest"), #.min and .max from apache documentation. Finding the earliest and latest times with these checks. Alias from documentation as well to change the name 
        F.max(cols).alias("Latest")
    ).show()
# show the first 3 rows in vertical format. From viewing data in apache quickstart:dataframe
df.show(3, vertical=True)
# Data quality 
print("\nMissing values:")
for cols in df.columns: # iterate through all columns (.columns from apache, lists all the columns)
    if cols in textCols:  # .filter in Databricks tutorial, returns a new DataFrame with only the rows where the condition is true
        missingRows = df.filter(F.col(cols).isNull() | (F.trim(F.col(cols)) == "")) # .trim from apache, removes whitespace
    else:                                                                           # .col refers to a column by its name
        missingRows = df.filter(F.col(cols).isNull())
    print("\nRows where", cols, "is missing:", missingRows.count()) # .count from apache, returns number of rows in the DF
    missingRows.show(vertical=True) #20 rows by default
# .distinct from apache, returns new dataframe with distinct rows. find duplicates from the total rows - the distinct ones 
duplicates = total_rows - df.distinct().count()
print("Duplicates:", duplicates)
if duplicates > 0:
    print("\nDuplicate records:")
    df.exceptAll(df.distinct()).show(vertical=True) # except all from apache documentation, it returns new DF while containing rows from old DF and preserving duplicates
# finds how many distinct values each column has, finds columns with different values
distinct_counts = df.select([F.count_distinct(c) for c in df.columns]).collect()[0] # collect is from from apache, it returns as a list of row
print("\nDistinct values:")
for i in range(len(df.columns)):
    print(df.columns[i], ":", distinct_counts[i])
print("\nDistinct value (per row):")
for i in range(len(df.columns)):
    if distinct_counts[i] == total_rows:
        print(df.columns[i])
if numCols: # showing the rows where there are 0's. To determine if the 0 is valid or an error
    # this shows the summary of the dataframe, from Apache Quickstart
    df.select(numCols).describe().show()
    for c in numCols: # Databricks tutorial. Filter is used here to select rows to return in the dataframe. 
        print("\nRows where", c, "equals 0:")
        df.filter(F.col(c) == 0).show(5, vertical=True)
        print("\nRows with negative number:")
        df.filter(F.col(c) < 0).show(5, vertical=True)
if decCols: # This is all potential latitude and longitude value checking
    print("\nInvalid latitude values:")
    df.select([F.count_if((F.col(c) < -90) | (F.col(c) > 90)).alias(c) for c in decCols]).show(1, vertical=True)
    print("\nInvalid longitude values:")
    df.select([F.count_if((F.col(c) < -180) | (F.col(c) > 180)).alias(c)for c in decCols]).show(1, vertical=True)
if dateCols:
    # .current_timestamp() from apache, it returns the timespamtp at the beginning of the query
    df.select([F.count_if(F.col(c) > F.current_timestamp()).alias(c) for c in dateCols]).show(1, vertical=True)
    for cols in dateCols:
        print("\nFuture dates in", cols)
        df.filter(F.col(cols) > F.current_timestamp()).show(5, vertical=True) 
print("\nRows where the later date column is earlier than the first:")
for i in range(len(dateCols)):
    for j in range(i + 1, len(dateCols)):
        first = dateCols[i]
        second = dateCols[j]
        before = df.filter(F.col(second) < F.col(first)).count()
        print(second, "is before", first, ":", before)
if textCols:
    # Check for leading or trailing spaces
    print("\nText values with spaces (start or end per column):")
    df.select([F.count_if(F.col(cols) != F.trim(F.col(cols))).alias(cols) for cols in textCols]).show(1, vertical=True)
    # Check for empty or only whitespace
    print("\nText values that are empty (per column):")
    df.select([F.count_if(F.trim(F.col(cols)) == "").alias(cols) for cols in textCols]).show(1, vertical=True)
for c in textCols: # 10 most common
    # groupby, desc, count from apache
    # Databricks shows how to use groupby
    df.groupBy(c).count().orderBy(F.desc("count")).show(10) 
if numCols:
    df.select(numCols).describe().show() # Includes things like count, mean, and standard deviation + more 
for c in dateCols:
    # year, month, and hour from Apache API 
    print("\nPer month, per hour: ", c)
    df.groupBy(F.year(c).alias("year"), F.month(c).alias("month")).count().orderBy("year", "month").show(100) 
    df.groupBy(F.hour(c).alias("hour")).count().orderBy("hour").show(25) 