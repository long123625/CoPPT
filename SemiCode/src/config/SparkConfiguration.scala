package config

/**
 * @author: ${yhs}
 * @create: 2022-05-30 09:11
 * */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
class SparkConfiguration extends Serializable{

  def getContext:SparkContext={
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    @transient val conf: SparkConf =  new SparkConf()
      .setAppName("FlexStore")
//      .setMaster("local[2]")
    //      .setSparkHome("/usr/local/hadoop-2.7.2")
    //      .setMaster("yarn-client")
    //      .setJars(Seq("hdfs:///user/jiguangxi/test0509/MyCode.jar"))
    //      .set("spark.num.executors","3")
    //      .set("spark.executor.memory", "2g")
    //      .set("spark.driver.memory", "2g")
    //      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.rdd.compress","true")

    //      .set("spark.sql.shuffle.partitions","100");
    ////      .setMaster("local[4]")
    //      .setMaster("yarn-client")
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .registerKryoClasses(Array(classOf[TP],classOf[computeDataPatternBenefitF]))
    ////      .set("spark.kryoserializer.buffer.max","2000m")
    //      .set("spark.rdd.compress","true")
    ////      .set("spark.hive.mapred.supports.subdirectories","true")
    ////      .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
    //      .set("spark.sql.shuffle.partitions","200")

    @transient val sc: SparkContext = new SparkContext(conf)
    sc
  }

  def getSqlContext(sparkContext: SparkContext):SQLContext={
    new SQLContext(sparkContext)
  }
  def getHiveSqlContext(sparkContext: SparkContext):HiveContext={
    new HiveContext(sparkContext)
  }

}