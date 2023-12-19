## Running Spark on a cluster
We'll use EMR -> AWS service. 

You have to pay to use this.
I will just watch the videos to not get into trouble.
Famously, it is very easy to mess up and get a huge bill from amazon if you are not very careful.

### Initial set up
- Create account provide billing info
- Go to EC2 service to set up the ssh keys. 
- Go to EMR (Elastic Map Reduce) Service

### Partitioning
There are more considerations that we have to take into account 
when we execute spark on a cluster.
We must think about how the data will be partitioned.

If we just used the `movie_ratings` script as is for the 1M dataset,
it would not work. 
It would run out of memory, causing the executables to fail.
Spark does not automatically partition the data: you must think about it.

Choosing number of partitions.
Too few -> does not use all computing resources.
Too many -> a lot of overhead. 
Standard, reasonable place to start: `partitionBy(100)`

### `movie_ratings` on a cluster
Instead of local file use cluster storage, e.g. hdfs. 
Again this is a little bit more complicated than doing it on your local computer.

You can use an empty `SparkConf` to stick with the defaults. 
In the case of the 1M dataset we must change
`spark-submit--executor-memory 1g` to increase the executor memory from 
the default 500mb to 1Gb.

Copy needed dataset and python script to cluster.
In aws they should be copied to a "bucket".

Ssh to the master node of the aws cluster.
You can copy the files that you placed in the bucket to the master node
```bash
aws s3 cp {path}
```
then just execute the script with spark-submit
```bash
spark-submit {script.py}
```

Most common error is `out of memory`, 
you can fix it by allocating more memory per executable.

### Troubleshooting
It's hard. 

In your own cluster you can run a useful spark dashboard on a port of your choice. 
However, this is not available in AWS.
The dashboard has the directed acyclic graph, a timeline, a list of executables,
an env tab with a list of all the dependencies.
Also, it is easier to look at the logs here. 
If you don't have access to the dashboard, 
you have to retrieve the distributed logs from all around the clusters.
There are commands to easily do this, 
but it is easy to see that this is harder than looking at a ui in port 4040.

Usually it's a good idea to take a look at the job that's taking the longest to execute.

Very important thing to remember: 
**Just because something runs in your computer,**
**it does not mean it will run properly on the cloud/cluster**

Finally, it is very useful to simplify your environment: 
don't use obscure packages, use as few packages as posible,
use the most popular packages.

### **Very important** Remember to terminate cluster when you are done.
