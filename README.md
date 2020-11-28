# spark-numpy 
A Spark [v2 Data Source](1) for ~reading~ and writing `Dataset`/`DataFrame` as [structured Numpy arrays](2).

_Support for reading Numpy arrays is still in development_

## Use cases
By providing bidirectional conversion between Spark `DataFrame`/`Dataset` and structured numpy arrays,
`spark-numpy` can serve as the missing link to allow seamless interoperability between Scala/Java-based Spark ETL pipeline and Python workflows.

For example, a Spark ETL pipeline can prepare training data for downstream model training with PyTorch/Tensorflow.

## Schema mapping

 | Spark `DataType`          | Numpy `dtype`                                                            |
 |---------------------------|--------------------------------------------------------------------------|
 | IntegerType               | `<i4`                                                                    |
 | LongType                  | `<i8`                                                                    |
 | FloatType                 | `<f4`                                                                    |
 | DoubleType                | `<f8`                                                                    |
 | BooleanType               | `\|b1`                                                                   |
 | StringType                | `\|Sx` where `x` is the size limit                                       |
 | StructType                | Composite `dtype` with named fields.                                     |
 | ArrayType(elementType, _) | A `dtype` with base type derived `elementType`, and a 1d shape parameter |
 
Note that `spark-numpy` supports the following forms of nested schema:

 - :white_check_mark: `StructType` within an `ArrayType`
 - :white_check_mark: `StructType` within another `StructType`
 
Spark `DataType` not supported:
 - :negative_squared_cross_mark: `ArrayType(StringType, _)` (see below for details)

## Special considerations for strings and arrays
**You need to pay extra attention when writing a DataFrame containing columns of `StringType` and/or `ArrayType`.**

Unlike other data file formats, like Avro, Parquet or CSV, each record in a structured Numpy array is encoded like a
C `struct`, and thus must have **fixed size**. This means that `spark-numpy` must pre-determine a fixed size limit for
string- and array-typed columns.

### Variable-length strings
For strings, as long as you know the maximum length, you can easily provide a size constraint through **options** of
the `DataFrameWriter` (see below). For example, if you set the maximum string length to 16, strings with less than 16
bytes will be **padded** with `'\\0'` (zero bytes). Therefore, the padded string will be well-behaved as a normal
C null-terminated string.

### Variable-length arrays
It is generally **NOT recommended** to dump DataFrame containing variable-length arrays, for the following reasons:

 1. If the array length exhibits very large variance (containing both short and long arrays), then we either need to
 truncate arrays that are too long to a specific length limit, or we will need to pad all short arrays to the maximum
 array size, leading to bloated data file sizes.
 2. If the maximum array size is `N`, and the array to be encoded only has `M` (`M < N`) elements, the
 remaining bytes will all be padded with zero, which would be interpreted as zero integers or floating numbers in Numpy. 
 Thus, if your actual array data contains zero, you may not be able to distinguish between actual zero values and
 the artificially padded zeros. This issue can be mitigated by including an additional column in your DataFrame to
 encode the actual array length (before padding.)
 
There are, however, some legitimate use cases to dump arrays, e.g.:

 - Embedding vectors of fixed size

### Size hint
You may provide size hints for specific columns using option `maximumSize:COLUMN_NAME`, e.g.,

```scala
val df = Seq(("Alice", "10.0.9.12"), ("Bob", "10.1.3.42")).toDF("name", "ipAddress")
df.write
  .option("maximumSize:name", "5")
  .option("maximumSize:ipAddress", "16")
  .format("io.xydrolase.spark.npy.NumpyDataSourceV2")
  .save("output.npy")
```

Without specifying the size limit explicitly, the default size limit for both strings and arrays are `16`.

### Automatically inferring maximum size
_TO BE ADDED_

## Examples
See Unit tests.

[1]: https://spark.apache.org/docs/2.4.0/api/java/index.html?org/apache/spark/sql/sources/v2/DataSourceV2.html
[2]: https://numpy.org/doc/stable/user/basics.rec.html
