## Spark SQL

Spark SQL is probably the most important spark library. It contains the dataframe api.
You can apply SQL queries to dataframes. There are very popular big data tools that only do this (hive). It's very powerful because you can query almost anything from a distributed db.

```python
from pyspark import SparkSessioin, Row

spark = SparkSession.bulder.appName('Foo').getOrCreate()
df = spark.read.json('data_file_path')

# sql queries
results_df = spark.sql("select foo from bar order by foobar)
# pandas style queries
x = df.grouby(df['col1']).mean()

spark.stop()
```

The trend in spark is to use dataframes instead of RDDs.
They are faster and higher level.
Spark streaming and ml are moving from RDDs to dataframes. 

**Dataset** Under the hood scala struct. You don't have to worry to much about this if you are using pythom.
