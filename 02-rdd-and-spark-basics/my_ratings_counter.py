from re import split
from pathlib import Path
from pyspark import SparkConf, SparkContext


def sorted_dict(x: dict) -> dict:
    return {key: x[key] for key in sorted(x)}


# Create SparkContext object
conf = SparkConf().setMaster("local").setAppName("RatingsValueCounts")
sc = SparkContext(conf=conf)

# Create RDD from movie data
root_dir_path = Path(__file__).resolve().parent.parent
data_file_path = root_dir_path / "data/ml-100k/u.data"
lines = sc.textFile(
    str(data_file_path)
)  # textFile returns arr of string, each string is a line of the text file

# Get ratings data
ratings = lines.map(lambda x: split("\t", x)[2])

# Value counts of ratings
ratings_count = ratings.countByValue()
ratings_count_sorted = sorted_dict(ratings_count)

# Print value counts
for key, value in ratings_count_sorted.items():
    print(key, value)

# Stop Spark context
sc.stop()
