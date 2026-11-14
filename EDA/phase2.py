from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
unified_file = "unified.csv"
df = spark.read.csv(f"./{unified_file}", header=True, inferSchema=True, sep=",")
cities = ["New York", "Los Angeles"]
# create column
dated = df.filter(F.col("created_day").isNotNull())
# create period column based on created day
# .withColumn creates period, .when from apache API documentation is used to create the time periods, it is conditional statemntement (== or <=). otherwise is also used for the last time period, its like else
dated = dated.withColumn("period", F.when(F.col("created_day") <= 7, "1-7").when(F.col("created_day") <= 14, "8-14").when(F.col("created_day") <= 21, "15-21").otherwise("22-end"))
# count requests per month and the period through .count() and groupby. .groupby groups by specified columns
monthCount = dated.groupBy("city", "created_year", "created_month", "period").count()
# weekly request volume 
for city in cities:
    print("\n" + city + ": requests per month and period")
    monthCount.filter(F.col("city") == city).orderBy("created_year", "created_month", "period").show(50)  #randon big number to show everything
periodCount = dated.groupBy("city", "period").count()

for city in cities:
    city_counts = periodCount.filter(F.col("city") == city)
    highest = city_counts.agg(F.max("count")).first()[0] # max to find highest request and opposite for lowest
    lowest = city_counts.agg(F.min("count")).first()[0] # .first from apache, returns the first row, [0] to get the value
    print("\n" + city + " - highest request volume")
    city_counts.filter(F.col("count") == highest).show()
    print(city + " - lowest request volume")
    city_counts.filter(F.col("count") == lowest).show()

# service-request hotspots
for city in cities:
    cityDf = df.where(F.col("city") == city) # keep rows where city matches current city and remove those where zipcode is null
    cityDf = cityDf.where(F.col("zipcode").isNotNull()) # .desc from api, orders in descending order
    topPostal = cityDf.groupBy("zipcode").count().orderBy(F.desc("count")).limit(10) # 10 postal codes per requirements, highest service requests
    print("\n" + city + "- t10 postal codes")
    topPostal.select("zipcode", "count").show(truncate=False) # print

    for codes in topPostal.collect():
        print("\n" + city + " - postal code", codes["zipcode"]) # [""] from apache (row). just accessing the row value
        zipDf = cityDf.where(F.col("zipcode") == codes["zipcode"]) # keep rows where zipcode matches current and remove null, gives the 3 most common per requirements
        zipDf = zipDf.where(F.col("request_type").isNotNull())
        topReq = zipDf.groupBy("request_type").count()
        topReq = topReq.orderBy(F.desc("count")).limit(3)
        topReq = topReq.withColumn("percentage",F.col("count") / codes["count"] * 100) # .round from apache, this line gets the percentage by dividing the count by the total count. Multiplying by 100
        topReq = topReq.withColumn("percentage", F.round(F.col("percentage"), 2)) # from apache documentation, rounds to 2 decimal places
        topReq.show(truncate=False) # truncate from apache to show full names

# bonus 
completed = df.where(F.col("is_valid_resolution") == True) # only rows where is_valid_resolution column is true

print("\nAverage resolution")
completed.groupBy("city").avg("resolution_hours").show() # avg from apache
print("\nMedian resolution")
completed.groupBy("city").agg(F.median("resolution_hours")).show() # median and agg from apache, agg is used to get median

for city in cities:
    print("\n" + city + " - t5 request types with longest average resolution time") # getting the t5
    topRequests = completed.where(F.col("city") == city) # keep rows where city matches current, remove null, find avg
    topRequests = topRequests.where(F.col("request_type").isNotNull())
    topRequests = topRequests.groupBy("request_type").avg("resolution_hours")
    topRequests = topRequests.orderBy(F.desc("avg(resolution_hours)"))
    topRequests = topRequests.limit(5)
    topRequests = topRequests.withColumn("avg(resolution_hours)", F.round(F.col("avg(resolution_hours)"), 2)) # round to 2 decimals
    topRequests.show(truncate=False) # truncate from apache to show full names

for city in cities:
    cityDf = df.where(F.col("city") == city)
    tot = cityDf.count() # total number of requests
    counts = cityDf.groupBy("status").count() #group by status and count requests per status
    counts = counts.withColumn("percentage", F.col("count") / tot * 100)
    counts = counts.withColumn("percentage", F.round(F.col("percentage"), 2))
    counts = counts.orderBy(F.desc("count"))
    print("\n" + city + " - status counts and percentages")
    counts.show()
    print("Most common status - ", counts.first()[0])

    counts = cityDf.groupBy("channel").count() # samew thing as status
    counts = counts.withColumn("percentage", F.col("count") / tot * 100)
    counts = counts.withColumn("percentage", F.round(F.col("percentage"), 2))   
    counts = counts.orderBy(F.desc("count"))
    print("\n" + city + " -  channel counts and percentages")
    counts.show()
    print("Most common channel - ", counts.first()[0])