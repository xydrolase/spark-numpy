# spark-numpy 
A Spark [v2 Data Source](1) for reading and writing `Dataset`/`DataFrame` as [structured Numpy arrays](2).

## Use cases
By providing bidirectional conversion between Spark `DataFrame`/`Dataset` and structured numpy arrays,
`spark-numpy` can serve as the missing link to allow seamless interoperability between Scala/Java-based Spark ETL pipeline and Python workflows.

For example, a Spark ETL pipeline can prepare training data for downstream model training with PyTorch/Tensorflow.

## Schema compatibility
Since each record in a structured Numpy array is essentially a C `struct`, it must have a fixed size.
This means that for data types of variable length like `StringType` and `ArrayType`, `spark-numpy` must determine an
appropriate size before writing the data to `.npy` files.

The user may explicitly provide a size hint, or let `spark-numpy` infer the maximum size based on the actual data.

### Size hint
You may provide size hints for specific DataFrame columns using option `maximumSize:COLUMN_NAME`, e.g.,

```scala
val df = Seq(("Alice", "10.0.9.12"), ("Bob", "10.1.3.42")).toDF("name", "ipAddress")
df.write
  .option("maximumSize:name", "5")
  .option("maximumSize:ipAddress", "16")
  .format("io.xydrolase.spark.npy.NumpyDataSourceV2")
  .save("output.npy")
```

### Automatically inferring maximum size
_TO BE ADDED_

## Examples
See Unit tests.

[1]: https://spark.apache.org/docs/2.4.0/api/java/index.html?org/apache/spark/sql/sources/v2/DataSourceV2.html
[2]: https://numpy.org/doc/stable/user/basics.rec.html
