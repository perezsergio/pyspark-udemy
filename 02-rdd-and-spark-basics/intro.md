 ## Intro
 Spark is a general engine for big data processing.
 Big data tool -> Horizontal scaling, fault tolerance.
 Spark core + Std libraries (streaming, sql, mlib, graphx)

 This performance is achieved with a Directed Acyclic Graph engine (lazy engine).
 There's apis in python java and scala. Most common is python.
 
 A main concept of Spark is the Resilient Distributed Dataset (RDD) .
 

 - Why python, not scala or java?
 
   It's just easier to set up and to program. It's the most popular language, most people already know it.
 
   An alternative is Scala. It is a tiny bit faster, and spark features are first come out on scala. This is because scala is written in sccala. However, python is better for most use cases.
 
   Python and Scala code is very similar anyways.
 

 - Why use spark instead of mapreduce?
 
   It's 2-100 times faster, and it's easier to program.
 

 ## Resilient Distributed Dataset.
 

 RDDs are a resilient (fault tolerant), distributed, dataset abstraction.
 
 In practice, you will create a RDD from some stored data, then you will transform it to get the information that you want.
 

 **SparkContext** basically a spark context object gives you access to the methods that you need to create a RDD.
 

 **Creation**
 From text file `sc.textfile` in local system or hdfs.
 From python object with `sc.parallelize`
 You can also create RDDs from hive objects with a HiveContext object.
 In general you can create an RDD from any popular big data formats (Cassandra, ElasticSearch, HBase) and normal data formats (json, csv, ...)
 
 **Transformation**
 The methods used to transform a rdd are: `map`, `flatmap`, `filter`, `distinct`, `sample`, and `union`, `intersection`, `subtract`, `cartesian`. As you can see, there aren't many methods, but they are all very powerful.
 
 **Actions**
 Common actions: collect, count, countByValue, take, top, reduce, ...
 

 Spark has **lazy evaluation**, it won't do anything until there is an action call.
 
 ### Key/value rdd
 Key/value rdd is one of the most common and most useful patterns. They look like a python arr of 2d tuples.
 
 If you map values only, and you don't modify the keys,
 it's more efficient to use `mapValues` and `flatMapValues`.
 