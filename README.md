This repo contains examples of <b>hadoop java mapreduce</b> version 1 programs.

<b>MapOnly</b> - This is a map job only example. i.e. no reducer.

<b>CompositeWritableMap</b> - This is also a map only example. But with a complex object as input key/value.

<b>WordCount</b> - The famous wordcount example - with mapper and reducer.

<b>CompositeWritableReduce</b> - This is with both mapper and reducer. But with a complex object as input key/value.

<b>CombinerExample</b> - An example to use a combiner component in the map reduce.

<b>DistributedCacheExample</b> - This shows how to use distributed cache, which reduces shuffling data at mapper level. This is used only when one of the data set is smaller in size, able to be fit in memory of every node (i.e. otherwise called as distributed cache)

<b>PigEvalUDF</b> - Pig UDF example with Eval function, which we could use it in the foreach-generate statement. This UDF converts celsius to fahrenheit.

<b>PigFilterUDF</b> - Pig UDF example with filter function, which could be used in the filter statement. This always returns only Boolean.

<b>HiveUDF</b> - a simple Hive UDF example.
