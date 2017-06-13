from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext(appName="Streaming")
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("172.23.80.245", 5580)
lines.pprint()


ssc.start()