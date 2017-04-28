This repo contains examples of mapreduce version 1 programs.

MapOnly - This is a map job only example. i.e. no reducer.

CompositeWritableMap - This is also a map only example. But with a complex object as input key/value.

WordCount - The famous wordcount example - with mapper and reducer.

CompositeWritableReduce - This is with both mapper and reducer. But with a complex object as input key/value.

CombinerExample - An example to use a combiner component in the map reduce.

DistributedCacheExample - This shows how to use distributed cache, which reduces shuffling data at mapper level. This is used only when one of the data set is smaller in size, able to be fit in memory of every node (i.e. otherwise called as distributed cache)

PigEvalUDF - Pig UDF example with Eval function, which we could use it in the foreach-generate statement. This UDF converts celsius to fahrenheit.

PigFilterUDF - Pig UDF example with filter function, which could be used in the filter statement. This always returns only Boolean.

HiveUDF - a simple Hive UDF example.
