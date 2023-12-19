## Spark Streaming 

Executes map and reduce operations once per every given time interval.

You can input the data from pretty much anywhere:
some port, hdfs, kafka, flume, amazon kinesis, ...

Nice 'checkpointing' feature, allows spark to pick up the execution where it
left of if some error causes the execution to break.

Technically it is not streaming, it is mini batch data processing. 
Where each mini batch is a spark rdd.


## GraphX

Currently only available in scala.

Useful for specific things: social graphs, 
connectedness in different graphs, join graphs, ...


## Extra: Where to go from here

This are just the basics. 
The best way to learn more about spark is to read books:
Learning Spark (useful code snippets)
Advanced Analytics with Spark (more ML, data mining),
Data Algorithms by O'Reilly (more general to Hadoop and MapReduce)

More courses from the same guy: 
probably the most interesting is about building recommender systems.
