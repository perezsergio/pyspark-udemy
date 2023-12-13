from pyspark import SparkConf, SparkContext
import collections
from pathlib import Path

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

root_dir_path = Path(__file__).resolve().parent.parent
data_file_path = root_dir_path / "data/ml-100k/u.data"

lines = sc.textFile(str(data_file_path))
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
