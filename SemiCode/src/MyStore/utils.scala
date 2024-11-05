package MyStore
import java.io.{File, PrintWriter}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
/**
 * @author: ${yhs}
 * @create: 2022-05-30 09:16
 * */
object utils {
  // TODO: 数据存储  保存为parquet文件并且记录一些统计信息   模式集列表; 数据; 存储路径; 谓词反向字典; 关系谓词集合==>构建对应的数据表
//  def constructKStar(kSchemaList:ArrayBuffer[Array[Int]],sqlCtx:SQLContext, data:RDD[(String,Long,String)],outputPath:String,
//                     res_presboard:Broadcast[scala.collection.Map[Long,String]]):Unit={
//    import org.apache.spark.sql.functions._
//    import sqlCtx.implicits._
//    // 构造成一个特别大的表, 然后进行多次select查询, 每次获得的结果构造一张表
//    var totalR=data.map(triple=>TP(triple._1,triple._2,triple._3)).toDF()
//      .groupBy("subject")
//      .pivot("predicate")
//      .agg(collect_list("obj")) //转成了主语属性表的形式存储
//    val removeEmpty=udf( (ids: Seq[String]) =>
//      if (ids.isEmpty) null else ids
//    ) //用户自定义函数，对于字符串序列，如果是空值，返回null，如果不是返回该序列
//    val arrayColumns=totalR.schema.fields.collect{
//      case StructField(name,ArrayType(StringType,true),_,_)=>name
//    } // 猜测？？？？
//    totalR = arrayColumns.foldLeft(totalR)((acc, c) => acc.withColumn(c, removeEmpty(totalR(c)))).persist(StorageLevel.MEMORY_AND_DISK_SER) //更改列c的值，去除空值
//    // 更改列字段名, 改为字符串类型
//    for( cname<- totalR.schema.fields){ //获得就是StrctFiled 的 集合对象，每一列一个StructField
//      if(!cname.name.equals("subject")) //如果不是主语列，那就是谓词列
//        totalR=totalR.withColumnRenamed(cname.name,res_presboard.value.get(cname.name.toLong).get) //转成字符串的谓词列
//    }
//    for(schema<-kSchemaList.toList){ //嵌套的数组，转为list，原来是划分的模式集（谓词集）,所以schema是一个谓词集
//      val ddfSchema=new ArrayBuffer[String]()
//      ddfSchema.append("subject")
//      val schString=schema.map(pre=>res_presboard.value.get(pre).get) // 获得字符串的谓词 数组集合
//      ddfSchema.appendAll(schString)
//      //        谓语字段不全部为空即可
//      var finalR=totalR.select(ddfSchema.map(pre => col(pre) as pre): _*).na.drop("all",schString) //变长参数，select所有谓词列，删除所有谓词列中含有null值的行
//      // 对数据进行转换,将关系谓词对应宾语 由String类型转换为Long类型
//      val schemaToString=res_presboard.value.get(schema(0)).get //以谓词集和 的 第一个谓词进行命名文件保存
//      finalR.repartition(col("subject")).write.format("parquet").mode(SaveMode.Overwrite).save(outputPath+"Storage//"+schemaToString.toString)
//    }
//    totalR.unpersist()
//  }

  def writeFrequencyToFile(predicateMapFrequency:RDD[(String,Long)], storageFilePath:String): Unit ={
    predicateMapFrequency.saveAsObjectFile(storageFilePath+"statistic/predicateMapFrequency")
  }

  // 将parquet文件转换为csv文件
  def parquetToCSV(dirPath:String,savePath:String,sql:SQLContext): Unit = {
    val files=new File(dirPath).listFiles()
    for( file <-files){
      val df=sql.read.parquet("file://"+file)
      val csv_writer=new PrintWriter(new File(savePath+file.getName.replace(dirPath,"")+".csv"))
      var head_string = ""
      for(x<-df.columns)
        head_string+=x+","
      head_string.drop(1)
      csv_writer.write(head_string+"\n")
      df.collect().foreach(line=>csv_writer.write(line.toString().drop(1).dropRight(1)+"\n"))
      csv_writer.close()
    }
  }


  // 每个谓词  及其对应的数据表    需要把int转换为谓词
  def writeIndexToFile(kSchemaList: ListBuffer[Array[Int]], sc:SparkContext, storageFilePath:String,
                       res_presboard: Broadcast[scala.collection.Map[Long,String]]): Unit = {

    val SchemaIndex = sc.makeRDD(kSchemaList).map(x=> x.map(p=> res_presboard.value.get(p.toLong).get)).flatMap(schema=> schema.map(p=> (p,schema))).coalesce(1)
//    SchemaIndex.saveAsTextFile(storageFilePath + "statistic/pMapSchemaTextFile")
    SchemaIndex.saveAsObjectFile(storageFilePath + "statistic/pMapSchemaObjectFile")
  }
//  // 每个谓词  及其对应的数据表    需要把int转换为谓词
//  def writeIndexToFile(kSchemaList:ArrayBuffer[Array[Int]],sc:SparkContext,storageFilePath:String,
//                       res_presboard:Broadcast[scala.collection.Map[Long,String]]): Unit ={
//
//    val SchemaIndex=sc.makeRDD(kSchemaList).map(x=>x.map(p=>res_presboard.value.get(p.toLong).get)).flatMap(schema=>schema.map(p=>(p,schema))).coalesce(1)
//    SchemaIndex.saveAsTextFile(storageFilePath+"statistic/pMapSchemaTextFile")
//    SchemaIndex.saveAsObjectFile(storageFilePath+"statistic/pMapSchemaObjectFile")
//  }

  // 每个谓词  及其对应的数据表    需要把int转换为谓词
  def writePreSelectorMapToFile(preSelectorMap:scala.collection.mutable.HashMap[String,Double],sc:SparkContext,storageFilePath:String): Unit ={
    val preSelector=sc.parallelize(preSelectorMap.toSeq).coalesce(1)
//    preSelector.saveAsTextFile(storageFilePath+"statistic/preSelectorTextFile")
    preSelector.saveAsObjectFile(storageFilePath+"statistic/preSelectorObjectFile")
  }

  // 每个谓词  及其对应的数据表    需要把int转换为谓词
  def writeNumPreMapToFile(name:String, Numpre:RDD[(String,Int)], sc:SparkContext, storageFilePath:String): Unit ={
    Numpre.coalesce(1).saveAsTextFile(storageFilePath+"statistic/"+name+"NumPreTextFile")
    Numpre.coalesce(1).saveAsObjectFile(storageFilePath+"statistic/"+name+"NumPreObjectFile")
  }

  // 每个谓词  及其对应的数据表    需要把int转换为谓词
  def writeSubNumPreMapToFile(name:String, subNumPre:collection.Map[String,Int], sc:SparkContext, storageFilePath:String): Unit ={
    val subNumPreRDD=sc.parallelize(subNumPre.toSeq).coalesce(1)
//    subNumPreRDD.saveAsTextFile(storageFilePath+"statistic/"+name+"NumPreTextFile")
    subNumPreRDD.saveAsObjectFile(storageFilePath+"statistic/"+name+"NumPreObjectFile")
  }

  def writeDataPatternTableSizeMapToFile(sc:SparkContext,DataPatternTableSizeMap:scala.collection.mutable.HashMap[String,Long], storageFilePath:String): Unit ={
    val dataPatternSize=sc.parallelize(DataPatternTableSizeMap.toSeq).coalesce(1)
    dataPatternSize.saveAsObjectFile(storageFilePath+"statistic/DataPatternTableSizeObjectFile")
//    dataPatternSize.saveAsTextFile(storageFilePath+"statistic/DataPatternTableSizeTextFile")
  }

  def writePrediacesToFile(name:String,sc:SparkContext,pres:collection.Set[String], storageFilePath:String): Unit = {
    val predicatesRDD = sc.parallelize(pres.toSeq).coalesce(1)
    predicatesRDD.saveAsTextFile(storageFilePath + "statistic/" + name + "PredicatesTextFile")
    predicatesRDD.saveAsObjectFile(storageFilePath + "statistic/" + name + "PredicatesObjectFile")
  }

  def writeIndexToFile2(kSchemaList:ArrayBuffer[Array[String]], sc:SparkContext, storageFilePath:String): Unit ={
    val SchemaIndex=sc.makeRDD(kSchemaList).flatMap(schema=>schema.map(p=>(p,schema))).coalesce(1)
    SchemaIndex.saveAsTextFile(storageFilePath+"statistic2/pMapSchemaTextFile")
    SchemaIndex.saveAsObjectFile(storageFilePath+"statistic2/pMapSchemaObjectFile")
  }

  // 每个谓词  及其对应的数据表    需要把int转换为谓词
  def writePreSelectorMapToFile2(preSelectorMap:scala.collection.mutable.HashMap[String,Double],sc:SparkContext,storageFilePath:String): Unit ={
    val preSelector=sc.parallelize(preSelectorMap.toSeq).coalesce(1)
    preSelector.saveAsTextFile(storageFilePath+"statistic2/preSelectorTextFile")
    preSelector.saveAsObjectFile(storageFilePath+"statistic2/preSelectorObjectFile")
  }

  // 每个谓词  及其对应的数据表    需要把int转换为谓词
  def writeNumPreMapToFile2(name:String,Numpre:RDD[(String,Int)],sc:SparkContext,storageFilePath:String): Unit ={
    Numpre.coalesce(1).saveAsTextFile(storageFilePath+"statistic2/"+name+"NumPreTextFile")
    Numpre.coalesce(1).saveAsObjectFile(storageFilePath+"statistic2/"+name+"NumPreObjectFile")
  }

  // 每个谓词  及其对应的数据表    需要把int转换为谓词
  def writeSubNumPreMapToFile2(name:String,subNumPre:collection.Map[String,Int],sc:SparkContext,storageFilePath:String): Unit ={
    val subNumPreRDD=sc.parallelize(subNumPre.toSeq).coalesce(1)
    subNumPreRDD.saveAsTextFile(storageFilePath+"statistic2/"+name+"NumPreTextFile")
    subNumPreRDD.saveAsObjectFile(storageFilePath+"statistic2/"+name+"NumPreObjectFile")
  }

  def writeDataPatternTableSizeMapToFile2(sc:SparkContext,DataPatternTableSizeMap:scala.collection.mutable.HashMap[String,Long], storageFilePath:String): Unit ={
    val dataPatternSize=sc.parallelize(DataPatternTableSizeMap.toSeq).coalesce(1)
    dataPatternSize.saveAsObjectFile(storageFilePath+"statistic2/DataPatternTableSizeObjectFile")
    dataPatternSize.saveAsTextFile(storageFilePath+"statistic2/DataPatternTableSizeTextFile")
  }
}
