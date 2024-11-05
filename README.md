CoPPT Run（See SemiCode folder for source code）
Running environment:Hadoop 3.0.0; Spark2.4.0; java 1.8.0; Scala 2.11.8
The missing jar packages in the run go to the jars file and look for them.
RDF Dataset format:.nt
Queries: SPARQL queries
运行模式设置： config.SparkConfiguration类
存储模块运行方式：
spark-submit --master yarn-client  --driver-memory 20g --executor-memory 15g --num-executors 20 --executor-cores 4 --conf spark.default.parallelism=200 --conf spark.shuffle.file.buffer=96k  --conf spark.reducer.maxSizeInFlight=96m  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.executor.memoryOverhead=6144m --class MyStore.StoreOrigin jar包文件路径 数据集路径 HDFS数据存储目录 数据集分隔符
	数据集： 	  .nt格式RDF三元组数据；
	HDFS存储路径： 数据存储位置；
	数据集分隔符：	 三元组中分隔符   默认为tab，可指定space即为空格
	
查询模块运行方式：
spark-submit  --master yarn-client --driver-memory 20g --executor-memory 15g --num-executors 20 --executor-cores 4 --conf spark.default.parallelism=200 --conf spark.shuffle.file.buffer=96k --conf spark.reducer.maxSizeInFlight=96m --conf spark.shuffle.memoryFraction=0.5 --conf  spark.executor.memoryOverhead=6144m --class MyQuery.RunDriver jar包文件路径   HDFS数据存储目录   查询文件路径  优化类型   结果存储路径
	HDFS数据存储目录：	HDFS上数据存储路径（数据存储模块对应）
	查询文件路径：		本地SPARQL查询文件路径
	优化类型：		 R启用规则优化； B启用半连接优化；也可以是RB组合；
    结果存储路径：		存放查询结果
```
其它系统：

​	S2RDF 	https://github.com/aschaetzle/S2RDF

​    Prost	https://github.com/tf-dbis-uni-freiburg/PRoST
