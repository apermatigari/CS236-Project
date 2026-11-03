from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

catalog = "cs236"
schema = "project"
volume = "phase1"
nyc_file = "nyc_311_800k.csv"
la_file = "la_311_800k.csv"
path_table = catalog + "." + schema
print(path_table)
max_values = 600       # text column limit
nyc_df = spark.read.csv(f"./{nyc_file}", header=True, inferSchema=True, sep=",")
la_df = spark.read.csv(f"./{la_file}", header=True, inferSchema=True, sep=",")
nyc_rows = nyc_df.count()
la_rows = la_df.count()
nyc_df.printSchema()
la_df.printSchema()

# Compares every NYC column name with every LA column name
matches = []  
for ny in nyc_df.columns:        
    for la in la_df.columns:     
        if ny == la or ny.lower().replace("_", "") == la.lower().replace("_", ""): # make everything lowercase and remove underscores to do a simple name match
            matches.append((ny, la))   # store as pair
# For every matched pair, look up the type and prints both. find the ones that differ
for ny, la in matches:           
    for name, dtype in nyc_df.dtypes:  
        if name == ny:                  
            nyc_type = dtype             # keep its type
    for name, dtype in la_df.dtypes:    
        if name == la:
            la_type = dtype
    line = "NYC " + ny + " (" + nyc_type + ")  -  LA " + la + " (" + la_type + ")"
    if ny != la:                         # the names are not the same
        line += " : different name"    
    if nyc_type != la_type:              # different types
        line += " : different type"
    print(line)
if not matches:                          # if nothing matched
    print("(no columns match by name)")
# Lists the columns that were not part of any match
# m[0] is the list of NYC names that matched, m[1] is la name
nyc_only = [c for c in nyc_df.columns if c not in [m[0] for m in matches]]   # never matched items in NY and LA are saved
la_only = [c for c in la_df.columns if c not in [m[1] for m in matches]]    
print("Only in NYC " + str(len(nyc_only)) + ":", nyc_only)
print("Only in LA  " + str(len(la_only)) + ":", la_only)
# stores its set of different valuesfor each text column to compare later
nyc_values = {}   
la_values = {}
for col, type in nyc_df.dtypes:  
    if type == "string":          # only text columns
        nyc_values[col] = {r[0] for r in nyc_df.select(col).distinct().limit(max_values).collect()} # .limit from apache documentation
for c, t in la_df.dtypes:   
    if t == "string":
        la_values[c] = {r[0] for r in la_df.select(c).distinct().limit(max_values).collect()}
for ny, la in matches:
    if ny in nyc_values and la in la_values:      # only text columns
        print("\nNYC: ", ny, "vs LA: ", la)
        if len(nyc_values[ny]) >= max_values or len(la_values[la]) >= max_values:   # limit check
            print(" ", max_values, "or more different values")
        else:
            nyc_vals = nyc_df.select(ny).distinct()    # the different values in each column
            la_vals = la_df.select(la).distinct()
            print(" only in NYC:", nyc_vals.subtract(la_vals).count())   # subtract and intersect from apache API documentation
            nyc_vals.subtract(la_vals).show(15)                          # subtract keeps the values only in the first column while intersect keeps the values in both, like SQL
            print(" only in LA:", la_vals.subtract(nyc_vals).count())
            la_vals.subtract(nyc_vals).show(15)
            print(" in both:", nyc_vals.intersect(la_vals).count())      
            nyc_vals.intersect(la_vals).show(15)
# NYC and LA text column comparison if did not match by name
found = []   
for ny in nyc_values:         
    for la in la_values:     
        # skip columns that were too long and pairs that match
        if len(nyc_values[ny]) < max_values and len(la_values[la]) < max_values and (ny, la) not in matches:
            # remove missing values
            shared = (nyc_values[ny] - {None}) & (la_values[la] - {None})
            if shared:       
                found.append((len(shared), ny, la, sorted(shared, key=str)[:8]))   # count, names, first 8 values
found.sort(reverse=True)      
for count, ny, la, shared in found:    
    print("NYC: ,", ny, "LA: ,", la, count, "shared values: ", shared)
if not found:
    print("(no shared values)")
# Find where every row has a different value
# .count_distinct from apache, it counts the different values in a column
nyc_counts = nyc_df.select([F.count_distinct(col) for col in nyc_df.columns]).collect()[0]
la_counts = la_df.select([F.count_distinct(col) for col in la_df.columns]).collect()[0]
# no repeating values
nyc_ids = []
for i in range(len(nyc_df.columns)):
    if nyc_counts[i] == nyc_rows:
        nyc_ids.append(nyc_df.columns[i])
la_ids = []
for i in range(len(la_df.columns)):
    if la_counts[i] == la_rows:
        la_ids.append(la_df.columns[i])
print("NYC ids:", nyc_ids)
for name, dtype in nyc_df.dtypes:     # look through the pairs
    if name in nyc_ids:               # print the type of each identifier column
        print("   ", name, dtype)
print("LA ids:", la_ids)
for name, dtype in la_df.dtypes:
    if name in la_ids:
        print("   ", name, dtype)
# find if the two periods overlap
for n, l in matches:
    for name, dtype in nyc_df.dtypes:      # find the type
        if name == n:
            nyc_type = dtype
    for name, dtype in la_df.dtypes:     
        if name == l:
            la_type = dtype
    if nyc_type in ("timestamp", "date") and la_type in ("timestamp", "date"):   # find earliest and latest dates and print range
        nmin, nmax = nyc_df.select(F.min(n), F.max(n)).collect()[0]
        lmin, lmax = la_df.select(F.min(l), F.max(l)).collect()[0]
        print(n, "/", l, "| NYC", nmin, "to", nmax, "| LA", lmin, "to", lmax)
    nycMissing = nyc_df.filter(F.col(n).isNull()).count() # missing entries in columns
    laMissing = la_df.filter(F.col(l).isNull()).count()
    # get percentage
    nycPct = round((nycMissing / nyc_rows) * 100, 1)
    laPct = round((laMissing / la_rows) * 100, 1)
    print("\n", n, "/", l)
    print("NYC:", nycMissing, nycPct)
    print("LA:", laMissing, laPct)
# compare columns that didn't name match 
nyc_df.select(nyc_only).printSchema()
la_df.select(la_only).printSchema()
# missing rows in column
nyc_df.select([F.count_if(F.isnull(c)) for c in nyc_only]).show(1, vertical=True)
la_df.select([F.count_if(F.isnull(c)) for c in la_only]).show(1, vertical=True)
# differenty values of each column 
nyc_df.select([F.count_distinct(c) for c in nyc_only]).show(1, vertical=True)
la_df.select([F.count_distinct(c) for c in la_only]).show(1, vertical=True)
# smallest and largest value of each
nyc_df.select([F.min(c) for c in nyc_only] + [F.max(c) for c in nyc_only]).show(1, vertical=True)
la_df.select([F.min(c) for c in la_only] + [F.max(c) for c in la_only]).show(1, vertical=True)