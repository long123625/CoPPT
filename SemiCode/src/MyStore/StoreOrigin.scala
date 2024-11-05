package MyStore

import java.util
import java.util.regex.Pattern
import MyStore.utils._
import config.SparkConfiguration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
/**
 * @author: ${yhs}
 * @create: 2022-05-30 09:15
 * */

// file:///home/hadoop/Desktop/storeData/watdiv10.nt
// file:///home/jiguangxi/Desktop/QueryProcess/statisticold/
// file:///home/jiguangxi/Desktop/QueryProcess/DataStore3/

object StoreOrigin {
  case class TP(subject:String,predicate:Long,obj:String)
  var sparkConfig:SparkConfiguration = null
  var sc:SparkContext = null
  var sql:SQLContext = null

  var schema: ArrayBuffer[Int] = null

  var preSimMatrix:util.HashMap[Int,util.HashMap[Int,Double]] = null
  var sim: util.ArrayList[util.ArrayList[Int]] = new util.ArrayList[util.ArrayList[Int]]  //记录每个谓词及与其相似度大于阈值的所有谓词
  var visit: Array[Int]= null

  var tableSize = 0
  var preFreMax = -1
  var preSetBenefitMax : ListBuffer[Int] = new ListBuffer[Int]
  var avgPreRelMax : Double = 0
  var nullRatioMax : Double = 0
  var preBenefitMax : Double = 0

  var preVisitedSet: ListBuffer[Int] = ListBuffer[Int]()
  val allSchemaSet = ListBuffer[Array[Int]]() //listBuffer常量添加 全局变量

  def main(args: Array[String]): Unit = {

    sparkConfig = new SparkConfiguration()
    sc = sparkConfig.getContext
    sql = sparkConfig.getSqlContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val startTime = System.currentTimeMillis()
    val pattern = sc.broadcast(Pattern.compile("[^a-zA-Z0-9]"))
    // todo: 传入参数: RDF数据集路径,  查询集路径,  数据存储路径,  权重
    val rdfFileName = args(0) // file:///home/hadoop/Desktop/storeData/watdiv10.nt

    val storagePath = args(1) // file:///home/hadoop/Desktop/storeData/

    var splitChData = '\t'
    if(args.length > 2) {
      if(args(2).equals("space"))
        splitChData = ' '
    }
    println(splitChData  + " 分割符 ")
    println("new jar + new partition + 60 pre")
    // 2. 读取数据文件
    var data = sc.textFile(rdfFileName)
      .map { triple =>
        val element = triple.split(splitChData)
        (element(0).trim, pattern.value.matcher(element(1)).replaceAll("").toLowerCase(), element(2).trim)
      }.persist(StorageLevel.DISK_ONLY) // 磁盘持久化

//    //计算谓语频率
//    val predicateMapFrequency=data.map(triple=>(triple._2,1:Long)).reduceByKey((x,y)=>x+y).sortBy(x=>x._2, false)
//    writeFrequencyToFile(predicateMapFrequency,args(1))
//    val frequency=sc.broadcast(predicateMapFrequency.collectAsMap())
//    val predicateMapFrequencyTime = System.currentTimeMillis() - startTime
//    println("接下来这步很费时间predicateMapFrequency" + predicateMapFrequencyTime)
    // 记录每个谓词对应的宾语主语数 objNumPre： 谓词：不同宾语数目

    if(storagePath.startsWith("file") || fs.exists(new Path(storagePath+"statistic/objNumPreTextFile"))==false){
      println("需要新建"+storagePath+"statistic/objNumPreTextFile")
      val objNumPre=data.filter(x => x._3.startsWith("<")).filter(x => x._2.length < 300).map(x=>(x._2, Set(x._3))).reduceByKey((pset1, pset2) => pset1 ++ pset2).map(x=>(x._1,x._2.size))
      writeNumPreMapToFile("obj", objNumPre, sc, storagePath)
    } //为什么宾语需要 < 开头？ 常量宾语不行吗？
    val objNumPreTextFileTime = System.currentTimeMillis() - startTime
    println("接下来这步很费时间objNumPreTextFileTime" + objNumPreTextFileTime)
    // 对谓词进行编码
    val preMap = data.map(triple => triple._2).distinct().zipWithIndex().cache()    // zipWithIndex编码后需要进行缓存  每次编码是不一样的
    val res_preMap = preMap.map(x => (x._2, x._1)) //数字在前，谓词在后
    val presboard = sc.broadcast(preMap.collectAsMap()) //谓词在前，元组转map
    val res_presboard = sc.broadcast(res_preMap.collectAsMap()) ///数字在前，谓词在后 元组转map
    val dataSPOID = data.map(x => (x._1,presboard.value.get(x._2).get, x._3)).distinct().persist(StorageLevel.DISK_ONLY)  // 经过实体+谓语编码后数据 ////实体， 谓词是编码的id？？？？
    val preMapTime = System.currentTimeMillis() - startTime
    println("谓语映射信息preMapTime" + preMapTime)
    //    for(x <- preMap.collectAsMap())
    //      println(x._1 + "  " + x._2)


    // 谓词集合： 次数
    val dataSchemaFrequency  = dataSPOID.map(triple => (triple._1, Set(triple._2.toInt))).reduceByKey((pset1, pset2) => pset1 ++ pset2)
      .map(sPset => (sPset._2, 1)).reduceByKey((count1, count2) => count1 + count2).persist(StorageLevel.DISK_ONLY) // 数据主语特征集频率
//    val dataSchemaFrequency2 = dataSchemaFrequency.collectAsMap()
    //    val dataschemaCnt = dataSchemaFrequency.map(x => x._2).sum()
    //    println("特征集个数为: " + dataSchemaFrequency2.size)
    val dataSchemaFrequencTime = System.currentTimeMillis() - startTime
    println("特征集抽取完毕dataSchemaFrequencTime" + dataSchemaFrequencTime)
    //  每个谓词对应的不同主语数量
    var subNumPre : collection.Map[Long,Int] = null
    //    subNumPre = dataSPOID.map(x=>(x._2, x._1)).distinct().map(x => (x._1, 1)).reduceByKey((a,b) => a+b).sortByKey().collectAsMap()
    //    writeSubNumPreMapToFile("sub", subNumPre.map(x => (res_presboard.value.get(x._1).get, x._2)), sc, storagePath)
    if(storagePath.startsWith("file") || fs.exists(new Path(storagePath + "statistic/subNumPreTextFile")) == false){
      subNumPre=dataSPOID.map(x=>(x._2,x._1)).distinct().map(x=>(x._1,1)).reduceByKey((a,b)=>a+b).map(x=>(x._1,x._2)).sortByKey().collectAsMap()
//      subNumPre=dataSPOID.map(x=>(x._2,x._1)).distinct().map(x => (x._1, 1)).reduceByKey((a,b) => a+b).sortByKey().collectAsMap()
      writeSubNumPreMapToFile("sub", subNumPre.map(x => (res_presboard.value.get(x._1).get, x._2)), sc, storagePath)
    }else{
      subNumPre = sc.textFile(storagePath+"statistic/subNumPreTextFile").map(x => x.replace("(","").replace(")","").split(","))
        .map(x=>((presboard.value.get(x(0))).get,x(1).toInt)).collectAsMap()
    }
    val subNumPreTime = System.currentTimeMillis() - startTime
    println("谓词频率计算完毕subNumPreTime" + subNumPreTime)
    val subNumPreboard = sc.broadcast(subNumPre)
    //计算谓语出现的总次数
    //    val predicateMapFrequency = data.map(triple => (triple._2, 1:Long)).reduceByKey((x,y) => x+y).sortBy(x => x._2, false)
    //    writeFrequencyToFile(predicateMapFrequency, storagePath)
    //    val preFrequency = predicateMapFrequency.collectAsMap()
    //    val preFrequency = sc.broadcast(predicateMapFrequency.collectAsMap())

    //    println("谓词出现的总次数")
    val appNumPre = dataSPOID.map(x=>(x._2, 1)).reduceByKey((a,b) => a+b).collectAsMap()
    //    appNumPre.map(x => (res_presboard.value.get(x._1), x._2)).foreach(println)
    //    println("=====================")
    // 计算每个谓词的选择性,这里选择性是指  谓词在数据集中出现次数/表中谓词非空值数量(即对应主语数) 平均一个谓词的不同主语数 = 谓词总数/谓词不同主语数
    var preSelectorMap:scala.collection.mutable.HashMap[String,Double] = null
    if(storagePath.startsWith("file")||fs.exists(new Path(storagePath+"statistic/preSelectorTextFile"))==false){
      preSelectorMap=new scala.collection.mutable.HashMap[String,Double]()
      val allPres=preMap.collectAsMap().keySet
      for(pre<-allPres)
        preSelectorMap.put(pre,appNumPre.get(presboard.value.get(pre).get).get/(subNumPre.get(presboard.value.get(pre).get).get*1.0))
      writePreSelectorMapToFile(preSelectorMap,sc,storagePath)
    }
    val preSelectorMapTime = System.currentTimeMillis() - startTime
    println("谓词的选择性preSelectorMapTime" + preSelectorMapTime)
    val dataSchemaFrequency2 = dataSchemaFrequency.collectAsMap()
    val dataSchemaFrequency2Time = System.currentTimeMillis() - startTime
    println("CS to Collect " + dataSchemaFrequency2Time)
    heuristicSchemaPartition(preMap.count().toInt, dataSchemaFrequency, dataSchemaFrequency2, subNumPreboard, res_presboard, dataSPOID, sc, sql, storagePath)
    val endTime = System.currentTimeMillis() //获取结束时间
    println("Used" + (endTime - startTime)/1000 + "s")
  }

  def heuristicSchemaPartition(hotPreNum: Int, schemaFrequencyRDD: RDD[(Set[Int], Int)], schemaFrequencyRDD2: scala.collection.Map[Set[Int], Int],
                               subNumPre: Broadcast[collection.Map[Long, Int]], res_presboard:Broadcast[scala.collection.Map[Long,String]],
                               hotdata: RDD[(String,Long,String)], sc:SparkContext, sqlCtx:SQLContext, storagePath:String ): Unit = {
    //    (1)获得相关矩阵
    //    println("进入主要功能函数")
    visit = new Array[Int](hotPreNum)
    preSimMatrix = getSimMatrix(hotPreNum, schemaFrequencyRDD2, subNumPre) //用RDD 形式重写
    //    for(i<-0 until hotPreNum){
    //      if(preSimMatrix.contains(i))
    //        println(i +"   " + preSimMatrix.get(i))
    //    }
    //    (2) 计算特征集的收益，并选出收益最大的特征集和 相应的最值信息
    getSinglePreSetBenefit(schemaFrequencyRDD2, subNumPre, preSimMatrix, res_presboard)
    //    (3)根据获得的特征集，利用相关矩阵，计算谓词频率最大所在行，更新特征集，获得划分的数据表
    updatePreSet(hotPreNum, preSimMatrix, subNumPre, res_presboard)
    //    (4)一轮之后，剔除用过的谓词，继续计算剩下的特征集的收益
    //    removePreAndComputeBenefit(schemaFrequencyRDD, subNumPre, preSimMatrix)
    //    (5)重复前面的操作，直到谓词划分完毕

    while(preVisitedSet.size != hotPreNum) {
      removePreAndComputeBenefit(schemaFrequencyRDD2, subNumPre, preSimMatrix, res_presboard)
      updatePreSet(hotPreNum, preSimMatrix, subNumPre, res_presboard)
    }

    var bestDataPatternTableSizeMap = new scala.collection.mutable.HashMap[String,Long]()
    println("总共表的个数 " + allSchemaSet.length)
    for (sch <- allSchemaSet) {
      println("打印表" + sch.map(x => res_presboard.value(x)).toSeq)
      println("打印表" + sch.toSeq)
      var tableRow = 0 //每个模式集的 表size
      sch.foreach(x => tableRow = Math.max(tableRow, subNumPre.value.get(x).get)) //更新表size：subNumPre：谓词id：主语个数，一个谓词的主语个数的最大值就是谓词集的表size
      bestDataPatternTableSizeMap.put(res_presboard.value.get(sch(0)).get,tableRow) //以第一个谓词为键，谓词(字符串）:所在谓词集的表size
    }
    //    (6) 进行数据存储
        dataStorage3(allSchemaSet, res_presboard, hotdata, sqlCtx, storagePath) //
//    dataStorage(allSchemaSet, res_presboard, hotdata, sqlCtx, storagePath) //my fangfa
//    bestDataPatternTableSizeMap.foreach(println)
    writeDataPatternTableSizeMapToFile(sc, bestDataPatternTableSizeMap, storagePath)
    writeIndexToFile(allSchemaSet, sc, storagePath, res_presboard)
  }

  //********************** rewrite getSimMatrix with RDD ***********************************
  def getSimMatrix(hotPreNum: Int, subSchema: scala.collection.Map[Set[Int], Int], subNumPre: Broadcast[collection.Map[Long, Int]] ): util.HashMap[Int,util.HashMap[Int,Double]] = {
    val preMatrix = new util.HashMap[Int, util.HashMap[Int, Double]](hotPreNum)
    //    println("进入到相似矩阵计算")

    for ((schema, frequence) <- subSchema.map(x => (x._1.toArray, x._2))) {

      for (i <- 0 until schema.length) {
        val a = schema(i).toInt
        //当谓词集合中谓词个数为1的时候，不会进入到下面
        //并且如果这个谓词，不和其他任意谓词共现，那么就不会包含在相关矩阵中 比如watdiv中的country，但是type，通过其他特征集中的共现，会被处理到
        for (j <- i + 1 until schema.length) {
          val b = schema(j).toInt

          if(!preMatrix.keySet.contains(a)) {

            preMatrix.put(a, new util.HashMap[Int, Double](300))
          }
          if(!preMatrix.get(a).keySet.contains(b)) {
            preMatrix.get(a).put(b,0) // 初始：a和b谓词之间的 为0
          }
          preMatrix.get(a).put(b, preMatrix.get(a).get(b) + frequence) //a 和 b之间 （次数）频率叠加

          if(!preMatrix.keySet.contains(b)) {
            preMatrix.put(b, new util.HashMap[Int, Double](300))
          }
          if(!preMatrix.get(b).keySet.contains(a)) {
            preMatrix.get(b).put(a,0)
          }
          preMatrix.get(b).put(a, preMatrix.get(b).get(a) + frequence) //b 和 a之间 （次数）频率叠加
        }
      }
    }

    //    println("谓词总数量   "+ hotPreNum + " 映射 "+ preMatrix.size)
    //  计算属性相似(共现)度矩阵
    for(i <- 0 until hotPreNum){
      if(preMatrix.contains(i)){
        for(j <- preMatrix.get(i).keySet){
          val tmp = preMatrix.get(i).get(j)
          //          preMatrix.get(i).put(j,tmp/(dataPreFre.get(i) + dataPreFre.get(j) - tmp))
          //          preMatrix.get(i).put(j, tmp / (subNumPre.value.get(i).get + subNumPre.value.get(j).get - tmp)) // (2)
          //          (1)
          var minDataPreFre = subNumPre.value.get(i).get
          if(minDataPreFre > subNumPre.value.get(j).get) {
            minDataPreFre = subNumPre.value.get(j).get
          }
          preMatrix.get(i).put(j, tmp / minDataPreFre)
        }
      }
    }
    //    println("打印谓语共现矩阵")
    //    for(i <- 0 until hotPreNum){
    //      if(preMatrix.contains(i))
    //        println(i + "   " + preMatrix.get(i))
    //    }
    preMatrix
  }

  def getSinglePreSetBenefit(subSchema: scala.collection.Map[Set[Int], Int], subNumPre: Broadcast[collection.Map[Long, Int]],
                             preSimMatrix: util.HashMap[Int, util.HashMap[Int, Double]], res_presboard: Broadcast[scala.collection.Map[Long,String]]) {

    //    var NonSelectPredicateSet = predicateFrequencyMap.value.keys.toSet //更新的时候，传参
    //    val newdataPattern = dataPattern.diff(dataPattern.diff(NonSelectPredicateSet)) //更新谓词集
    //    if(NonSelectPredicateSet.containsAll(Set(pattern: _*))) { //https://sparkbyexamples.com/spark/spark-extract-dataframe-column-as-list/
    //      partitionedSchema.append(pattern.toArray) //添加这些谓词到划分的模式
    //      NonSelectPredicateSet = NonSelectPredicateSet.diff(pattern.toSet) //剔除已经选了的谓词
    //    }
    println("进入收益计算")
    var flag = 0
    for ((preArray, frequence) <- subSchema.map(x => (x._1.toArray, x._2))) {
      var singlePreFreMax = 0
      var singlePreMax = -1
      //    var singlepreSetMaxBenefit: Array[Double] = null
      var avgPreRel: Double = 0
      var ratioNull: Double = 0
      var sumPreFre = 0
      var sumRel: Double = 0

      val n = preArray.length

      var flageCnt = 0
      //如果只有一个谓词，根本就进入不到第二重for循环

      if(n == 1) {
        flageCnt += 1
        //        println("单独的这个谓词是 " + res_presboard.value.get(preArray(0)).get)
        val hotPreNum = preSimMatrix.size()
        var cnt = 0
        if(preSimMatrix.contains(preArray(0))) {
          //          //有共现谓词,暂时不处理 可以直接!contains()来实现
          //          for(i <- 0 until hotPreNum) {
          //            if(preSimMatrix.get(preArray(0)).get(i) != 0) {
          //              cnt = cnt + 1
          ////                println("有共现的谓词  " +  i)
          //            }
          //          }
          //          println("与其他 " + cnt + " 个谓词共现")
        } else{
          //            println("没有这个谓词" + res_presboard.value.get(preArray(0)).get)
          println("单独成表" + res_presboard.value.get(preArray(0)).get)
          allSchemaSet += preArray
          visit(preArray(0)) = 1
          preVisitedSet += preArray(0)
        }
      }
      else if(n > 1) {
        flag = 1
        var minFre = 0
        val tmpArray = new Array[Int](n)
        var typePreFre = 0
        for (i <- 0 until n) {
          val pre1 = preArray(i)
          val frePre1 = subNumPre.value.get(pre1).get
          tmpArray(i) = frePre1
          sumPreFre = sumPreFre + frePre1
          if (frePre1 > singlePreFreMax) {
            //频率更新,谓词不更新
            if(!res_presboard.value(pre1).equals("httpwwww3org19990222rdfsyntaxnstype")) {
              //              println(pre1 + " type 谓词更新")
              singlePreMax = pre1
              singlePreFreMax = frePre1
            }
            else {
              typePreFre = frePre1
            }
          }
          for (j <- i + 1 until n) {
            val pre2 = preArray(j)
            val twoFre = preSimMatrix.get(pre1).get(pre2)
            val frePre2 = subNumPre.value.get(pre2).get
            minFre = frePre1
            if (frePre2 < minFre) {
              minFre = frePre2
            } //如果没有这部分, 用RDD好像也可以，只要加上累加变量，后面再管
            //            val rel = twoFre / minFre
            val rel = twoFre
            sumRel = sumRel + rel
          }
        }
        if(typePreFre > singlePreFreMax) {
          singlePreFreMax = typePreFre
        }

        avgPreRel = sumRel / (n * (n - 1) / 2)
        //空单元数 = 列数 * 行数 - 总的非空单元格数
        val sumNull = (n * singlePreFreMax) - sumPreFre
        //表格空值比例 [只是一个估值]
        ratioNull = 1.0 * sumNull / (n * singlePreFreMax)
        val schemaBenefit = avgPreRel / ratioNull //可能存在没有空值的情况
        if(ratioNull == 0) {
          //对于空值比例为0的谓词集合,说明属于一类实体的固有属性,此时该表的收益是无穷大(相关为1,null为0),后续不可能会有新的谓词加入
          //所以要么,和上面一样暂时跳过,不计算该表的收益[次优化]; 要么单独成表[其实就是完全按公式];要么,将这些谓词和与其共现的谓词都分配,重复存储[后续做的优化,暂时先不做,可以用一个集合统一存这类谓词]
          //这边选择直接暂时不考虑的实现方法,其他的以后再考虑
          println("空值比例为0")
          println("平均相关性: " + avgPreRel + "; sumRel: " + sumRel)
          //          println(frequence)
          //          for (elem <- preArray) {
          //            println(res_presboard.value(elem) + " 谓词的不同主语数: " + subNumPre.value(elem))
          //            for (elem2 <- preArray) {
          //              println(res_presboard.value(elem2) + " 谓词的不同主语数: " + subNumPre.value(elem2))
          //              println("两个谓词的共现相关性 " + preSimMatrix.get(elem).get(elem2))
          //              println("两个谓词的共现次数 " +  minFre * preSimMatrix.get(elem).get(elem2))
          //            }
          //          }
          // 计算方差
          val avgPreFre = sumPreFre / n
          var sum : Double = 0
          for (elem <- tmpArray) {
            //            println(elem + "  " + singlePreFreMax)
            sum = sum + ((elem - avgPreFre) * (elem - avgPreFre))
          }
          val fangcha = sum / n
          println(" 方差 " + fangcha)
          println("谓词集合size " + tmpArray.length)
          println("收益 " + schemaBenefit)
        }
        if (schemaBenefit > preBenefitMax ) {
          preBenefitMax = schemaBenefit
          preSetBenefitMax = preArray.toList.to[ListBuffer]
          println("当前最大收益谓词集个数:" + preSetBenefitMax.length)
          avgPreRelMax = avgPreRel
          nullRatioMax = ratioNull
          preFreMax = singlePreMax
          //          println(preFreMax + " 更新后的谓词频率最大的谓词")
          tableSize = singlePreFreMax
        }
      }
      //

    }
    if(flag == 0) {
      println("剩下的都是1个谓词的集合: geshu " + subSchema.size)
      for((preArray, frequence) <- subSchema.map(x => (x._1.toArray, x._2))) {
        if(preArray.length > 0) { //会有空表,可能是country谓词带来的
          println("单独成表" + res_presboard.value.get(preArray(0)).get)
          allSchemaSet += preArray
          visit(preArray(0)) = 1
          preVisitedSet += preArray(0)
        }
      }
      println("****table numbers*******" + allSchemaSet.length)
    }

    //    (preBenefitMax, preSetBenefitMax, nullRatioMax, avgPreRelMax, preFreMax, tableSize)
  }

  //  def getSinglePreSetBenefit(schemaFrequencyRDD: RDD[(Set[Int], Int)], subNumPre: Broadcast[collection.Map[Long, Int]],
  //                             preSimMatrix: util.HashMap[Int, util.HashMap[Int, Double]]): Unit = {
  //    var singlePreFreMax = 0
  //    var singlePreMax = 0
  //    //    var singlepreSetMaxBenefit: Array[Double] = null
  //    var avgPreRel: Double = 0
  //    var ratioNull: Double = 0
  //    var sumPreFre = 0
  //    var sumRel: Double = 0
  //    //    var NonSelectPredicateSet = predicateFrequencyMap.value.keys.toSet //更新的时候，传参
  //    //    val newdataPattern = dataPattern.diff(dataPattern.diff(NonSelectPredicateSet)) //更新谓词集
  //    //    if(NonSelectPredicateSet.containsAll(Set(pattern: _*))) { //https://sparkbyexamples.com/spark/spark-extract-dataframe-column-as-list/
  //    //      partitionedSchema.append(pattern.toArray) //添加这些谓词到划分的模式
  //    //      NonSelectPredicateSet = NonSelectPredicateSet.diff(pattern.toSet) //剔除已经选了的谓词
  //    //    }
  //    println("进入收益计算")
  //    val value = schemaFrequencyRDD.map(x => x._1).map { preSet =>
  //      val preArray = preSet.toList.to[ListBuffer]
  //      val n = preArray.length
  //      for (i <- 0 until n) {
  //        val pre1 = preArray(i)
  //        val frePre1 = subNumPre.value(pre1)
  //        sumPreFre = sumPreFre + frePre1
  //        if (frePre1 > singlePreFreMax) {
  //          singlePreFreMax = frePre1
  //          singlePreMax = pre1
  //        }
  //        for (j <- i + 1 until n) {
  //          val pre2 = preArray(j)
  //          val twoFre = preSimMatrix.get(pre1).get(pre2)
  //          val frePre2 = subNumPre.value(pre2)
  //          var minFre = frePre1
  //          if (frePre2 < minFre) {
  //            minFre = frePre2
  //          }
  //          val rel = twoFre / minFre
  //          sumRel = sumRel + rel
  //        }
  //      }
  //      //一个谓词集合的谓词平均相关性
  //      avgPreRel = sumRel / (n * (n - 1) / 2)
  //      //空单元数 = 行数 * 列数 - 总的非空单元格数
  //      val sumNull = (n * singlePreFreMax) - sumPreFre
  //      //表格空值比例 [只是一个估值]
  //      ratioNull = sumNull / (n * singlePreFreMax)
  //
  //      val schemaBenefit = avgPreRel / ratioNull
  //      if (schemaBenefit > preBenefitMax) {
  //        preBenefitMax = schemaBenefit
  //        preSetBenefitMax = preArray
  //        avgPreRelMax = avgPreRel
  //        nullRatioMax = ratioNull
  //        preFreMax = singlePreMax
  //        tableSize = singlePreFreMax
  //      }
  //      (schemaBenefit, preArray, ratioNull, avgPreRel, singlePreMax, singlePreFreMax)
  //      //返回值: 最大收益， 谓词集合(数组），空值比，平均谓词相关性，最大频次的谓词，谓词的最大频次
  //      //      val tuple = (preBenefitMax, preSetBenefitMax, nullRatioMax, avgPreRelMax, preFreMax, tableSize)
  //      //      tuple
  //    }
  ////        value.map(x => .collect() //需要collect吗？
  //  } //第一轮的


  def updatePreSet(hotPreNum: Int, preSimMatrix: util.HashMap[Int, util.HashMap[Int, Double]], subNumPre: Broadcast[collection.Map[Long, Int]], res_presboard: Broadcast[scala.collection.Map[Long,String]]): Unit = {
    println("开始更新谓词集合")
    var sum: Double = 0
    var avgPreRelNew: Double = 0


    val n = preSetBenefitMax.length
    println("最大收益集合里的size " + preSetBenefitMax.length)
    if(n == 0) {
      println("没有需要更新的, 已经覆盖所有谓词")
      return
    }
    if(n == 0) println("return 没有起作用")
    if(n > 60) {
      println("谓词集合 中谓词个数大于20(yago)就停止这轮的更新")
      println("谓词集合 中谓词个数大于60(DB pedia)就停止这轮的更新")
      return
    }
    for(i <- preSetBenefitMax) {
      if(visit(i) == 0) {
        //        println( res_presboard.value(i) + "*** "+ i + " 最大收益集合里的谓词 ")
        //        if(res_presboard.value(i).equals("httpwwww3org19990222rdfsyntaxnstype")) {
        //          println(preFreMax + " ***********************************************************************************************")
        //        }
        visit(i) = 1
        preVisitedSet += i
      }
    }

    for(i <- 0 until hotPreNum) {
      val m = preSetBenefitMax.length
      if(m > 60) {
        println("谓词集合 中谓词个数大于20(yago)就停止这轮的更新")
        println("谓词集合 中谓词个数大于60(DB pedia)就停止这轮的更新")
        return
      }
      if(visit(i) == 0L && preSimMatrix.get(preFreMax).get(i) != 0)  {
        for(j <- preSetBenefitMax) {
          sum = sum + preSimMatrix.get(i).get(j)
        }
        var tag = 0
        var update = 0
        if(sum > n * avgPreRelMax) {
          avgPreRelNew = (n * (n - 1) * avgPreRelMax + 2 * sum) / (n * (n + 1))
          tag = 1
        }
        if(tag == 1) {
          val sumNull = (n + 1) * tableSize
          val preFreNew = subNumPre.value(i)
          val productNull = Math.abs(tableSize - preFreNew)
          val nullRatioNew = (nullRatioMax * (n * tableSize) + productNull) / sumNull
          val benefitNew = avgPreRelNew / nullRatioNew
          if(benefitNew > preBenefitMax) {
            update = 1
            preSetBenefitMax += i
            visit(i) = 1
            preVisitedSet += i
            avgPreRelMax = avgPreRelNew
            nullRatioMax = nullRatioNew
            preBenefitMax = benefitNew
          }
        }
      }
    }
    allSchemaSet += preSetBenefitMax.toArray //table
  }//更新选出来的最大收益的谓词模式表

  def removePreAndComputeBenefit(schemaFrequencyRDD2: scala.collection.Map[Set[Int], Int], subNumPre: Broadcast[collection.Map[Long, Int]],
                                 preSimMatrix: util.HashMap[Int, util.HashMap[Int, Double]], res_presboard: Broadcast[scala.collection.Map[Long,String]]) {
    //重新初始化最值变量
    tableSize = 0
    preFreMax = -1
    preSetBenefitMax = new ListBuffer[Int]
    avgPreRelMax = 0
    nullRatioMax = 0
    preBenefitMax = 0
    //遍历特征集，剔除覆盖过的谓词集合
    //    (1)直接在rdd上操作剔除
    //    schemaFrequencyRDD.map{ x =>
    //      var preSet = x._1
    //      preSet = preSet.diff(preVisitedSet.toSet)
    //    }
    //    （2）先复制一份特征集，再操作
    //    val preSetRDD = schemaFrequencyRDD.map(x => x._1).map(preSet => preSet.diff(preVisitedSet.toSet))
    val updateSchemaFrequencyRDD = schemaFrequencyRDD2.map(x => (x._1.diff(preVisitedSet.toSet), x._2))
    //计算剔除谓词后的代价：计算相关性，计算null值比例，同时记录最大的谓词集和对应的相关性和空值比
    getSinglePreSetBenefit(updateSchemaFrequencyRDD, subNumPre, preSimMatrix, res_presboard)
    //    getSinglePreSetBenefit(updateSchemaFrequencyRDD, subNumPre, preSimMatrix)
    //      (preBenefitMax, preSetMaxBenefit, ratioNullMax, avgPreRelMax, preMax, tableSize) = getSinglePreSetBenefit(preSet, schemaFrequencyRDD, subNumPre, preSimMatrix)
  }

  def dataStorage(schemaSet: ListBuffer[Array[Int]], res_presboard:Broadcast[scala.collection.Map[Long,String]],
                  hotdata:RDD[(String,Long,String)], sqlCtx:SQLContext, storagePath:String): Unit = {
    val count = schemaSet.size()
    val k = 7 //(3) 单个模式集，进行存储实现，相当于 k = 1
    var capacity = count / k
    if(count % k != 0) {
      capacity = capacity + 1
    }
    val varSchemaSet: Array[ListBuffer[Array[Int]]] = new Array[ListBuffer[Array[Int]]](capacity)
    var cnt = 0
    var i = 0
    for(i <- 0 until capacity) {
      varSchemaSet(i) = new ListBuffer[Array[Int]]()
    }

    for (sch <- schemaSet)
    {
      varSchemaSet(i).append(sch)
      cnt = cnt + 1
      if(cnt % k == 0 && cnt != count) {
        i = i + 1
      }
    } //最后不足k(10)个，遍历完所有的会自动结束

    for( j <- 0 until i + 1){
      println("第 "+ j + " 个块共有 "+ varSchemaSet(j).size + "个")
      val propertyPreSet:mutable.Set[Long] = new mutable.HashSet[Long]()
      varSchemaSet(j).foreach(sch => sch.foreach(pro => propertyPreSet.add(pro)))
      val data1 = hotdata.filter(x => propertyPreSet.contains(x._2))
      val constructKStarTime = System.currentTimeMillis()
      constructKStar(varSchemaSet(j), sqlCtx, data1, storagePath, res_presboard)
      val constructKStarEndTime = System.currentTimeMillis() - constructKStarTime
      println(" constructKStarEndTime each is " + constructKStarEndTime)
    }
  }

  // TODO: 数据存储  保存为parquet文件并且记录一些统计信息   模式集列表; 数据; 存储路径; 谓词反向字典; 关系谓词集合==>构建对应的数据表
  def constructKStar(kSchemaList: ListBuffer[Array[Int]], sqlCtx:SQLContext, data:RDD[(String,Long,String)], outputPath:String,
                     res_presboard:Broadcast[scala.collection.Map[Long,String]]): Unit = {
    import org.apache.spark.sql.functions._
    import sqlCtx.implicits._
    // 构造成一个特别大的表, 然后进行多次select查询, 每次获得的结果构造一张表
    var totalR=data.map(triple=>TP(triple._1,triple._2,triple._3)).toDF()
      .groupBy("subject")
      .pivot("predicate")
      .agg(collect_list("obj"))
    val removeEmpty=udf( (ids: Seq[String]) =>
      if (ids.isEmpty) null else ids
    )
    val arrayColumns=totalR.schema.fields.collect{
      case StructField(name,ArrayType(StringType, true), _, _)=>name
    }
    totalR = arrayColumns.foldLeft(totalR)((acc, c) => acc.withColumn(c, removeEmpty(totalR(c)))).persist(StorageLevel.MEMORY_AND_DISK_SER) //更改列c的值，去除空值
    // 更改列字段名, 改为字符串类型
    for( cname<- totalR.schema.fields){ //获得就是StrctFiled 的 集合对象，每一列一个StructField
      if(!cname.name.equals("subject")) //如果不是主语列，那就是谓词列
        totalR=totalR.withColumnRenamed(cname.name,res_presboard.value.get(cname.name.toLong).get) //转成字符串的谓词列
    }
    println("constructed table structure**********")
    for(schema<-kSchemaList.toList){ //嵌套的数组，转为list，原来是划分的模式集（谓词集）,所以schema是一个谓词集
      val ddfSchema=new ArrayBuffer[String]()
      ddfSchema.append("subject")
      val schString=schema.map(pre=>res_presboard.value.get(pre).get) // 获得字符串的谓词 数组集合
      ddfSchema.appendAll(schString)
      println("da yin biao " + schString.toSeq)
      //        谓语字段不全部为空即可
      var finalR=totalR.select(ddfSchema.map(pre => col(pre) as pre): _*).na.drop("all",schString) //变长参数，select所有谓词列，删除所有谓词列中含有null值的行
      // 对数据进行转换,将关系谓词对应宾语 由String类型转换为Long类型
      val schemaToString=res_presboard.value.get(schema(0)).get //以谓词集和 的 第一个谓词进行命名文件保存
      println("save by subject as partition**********")
//      val partition_num=finalR.repartition(300).rdd.partitions.length
//      // 打印分区个
//      println(partition_num)
      println(finalR.rdd.partitions.length)
      finalR.repartition(300).write.format("parquet").mode(SaveMode.Overwrite).save(outputPath+"Storage//"+schemaToString.toString)
//      finalR.repartition(col("subject")).write.format("parquet").mode(SaveMode.Overwrite).save(outputPath+"Storage//"+schemaToString.toString)
    }
    println("over")
    totalR.unpersist()
  }

  //(3) 单个模式集，进行存储实现，相当于 k = 1 ,不过这样还得添加到varSchemaSet(i)，又只有一个元素，单独实现一下
  def dataStorage2(schemaSet: ListBuffer[Array[Int]], res_presboard:Broadcast[scala.collection.Map[Long,String]],
                   hotdata:RDD[(String,Long,String)], sqlCtx:SQLContext, storagePath:String): Unit = {
    val propertyPreSet:mutable.Set[Long] = new mutable.HashSet[Long]()
    var cnt = 0
    for(sch <- schemaSet) {
      println("第 "+ cnt + " 个谓词集合的长度" + sch.length)
      cnt = cnt + 1
      if(sch.length > 0) {
        sch.foreach(pro => propertyPreSet.add(pro))
        val data1 = hotdata.filter(x => propertyPreSet.contains(x._2))
        constructKStar2(sch, sqlCtx, data1, storagePath, res_presboard)
      }
    }
  }


  def constructKStar2(schema: Array[Int], sqlCtx: SQLContext, data:RDD[(String,Long,String)], outputPath: String, res_presboard:Broadcast[scala.collection.Map[Long,String]]): Unit = {
    import org.apache.spark.sql.functions._
    import sqlCtx.implicits._
    // 构造成一个特别大的表, 然后进行多次select查询, 每次获得的结果构造一张表
    var totalR = data.map(triple => TP(triple._1, triple._2, triple._3)).toDF()
      .groupBy("subject")
      .pivot("predicate")
      .agg(collect_list("obj")) //转成了主语属性表的形式存储
    val removeEmpty = udf( (ids: Seq[String]) =>
      if (ids.isEmpty) null else ids
    ) //用户自定义函数，对于字符串序列，如果是空值，返回null，如果不是返回该序列
    val arrayColumns = totalR.schema.fields.collect{
      case StructField(name, ArrayType(StringType, true), _, _) => name
    } // 猜测？？？？
    totalR = arrayColumns.foldLeft(totalR)((acc, c) => acc.withColumn(c, removeEmpty(totalR(c))))//.persist(StorageLevel.MEMORY_AND_DISK_SER) //更改列c的值，去除空值
    // 更改列字段名, 改为字符串类型
    for(cname <- totalR.schema.fields){ //获得就是StrctFiled 的 集合对象，每一列一个StructField
      if(!cname.name.equals("subject")) //如果不是主语列，那就是谓词列
        totalR = totalR.withColumnRenamed(cname.name, res_presboard.value.get(cname.name.toLong).get) //转成字符串的谓词列
    }
    val schemaToString = res_presboard.value.get(schema(0)).get
    totalR.repartition(col("subject")).write.format("parquet").mode(SaveMode.Overwrite).save(outputPath+"Storage//"+schemaToString.toString)
    totalR.unpersist()
  }


  def dataStorage3(schemaSet: ListBuffer[Array[Int]], res_presboard:Broadcast[scala.collection.Map[Long,String]],
                   hotdata:RDD[(String,Long,String)], sqlCtx:SQLContext, storagePath:String): Unit = {
    // 按照长度进行分块
    var maxxLen = 0
    schemaSet.foreach(sch => maxxLen = Math.max(sch.length, maxxLen)) //所有谓词集中的最大长度 谓词集中谓词的个数，相当于表中的行数
    // 所有模式集按照长度进行划分
    val varSchemaSet: Array[ListBuffer[Array[Int]]] = new Array[ListBuffer[Array[Int]]](maxxLen + 1)
    for( i <- 0 until maxxLen + 1)
      varSchemaSet(i) = new ListBuffer[Array[Int]]
    for (sch <- schemaSet)
      varSchemaSet(sch.length).append(sch) //谓词集的长度 作索引，元素是 谓词集
    // 按照长度分别处理各模式
    for( i <- 1 until maxxLen+1){
      println("当前模式长度为 "+ i +"  "+ "共有 "+ varSchemaSet(i).size + "个") //相同长度的谓词集 的个数，以长度作第一层索引，内层是每一个同样长度的谓词集合[[][]]
      val propertyPreSet:mutable.Set[Long] = new mutable.HashSet[Long]() //即把同长度谓词集中的谓词放一起
      varSchemaSet(i).foreach(sch => sch.foreach(pro => propertyPreSet.add(pro))) //sch 是谓词集，pro是单个的谓词(属性) 即把同长度谓词集中的谓词放一起
      val data1 = hotdata.filter(x => propertyPreSet.contains(x._2)) //过滤三元组数据，包含这样谓词的三元组，相当于起到分块数据的作用
      constructKStar(varSchemaSet(i), sqlCtx, data1, storagePath, res_presboard)
    }
  }
  // 一个变量和RDD 变量交互，那么就是分到多个节点上，所以需要广播，广播本身是需要将数据拉到driver端的，也是shuffle

  //  preSet 特征集， subNumPre:谓词的不同主语数目; 相关矩阵;
  //  def getSinglePreSetBenefit(preSet:RDD[(Set[String],Int)], subNumPre : collection.Map[Long,Int], preSimMatrix : util.HashMap[Int,util.HashMap[Int,Double]], preBenefitMax, preSetMaxBenefit) {
  //    var singlePreMaxFre = 0
  //    var singlePreMax = 0
  //    var singlepreSetMaxBenefit : Array = Array()
  //    val avgPreRel: Double = 0
  //    var ratioNull: Double = 0
  //    var sumPreFre = 0
  //    var sumRel = 0
  //    for(i <- 0 until preSet.length) {
  //      val pre1 = preSet(i).toInt
  //      val frePre1 = subNumPre.get(pre1) //获得谓词的不同主语数
  //      sumPreFre = sumPreFre + frePre1
  //      if(frePre1 > singlePreMaxFre) {
  //        singlePreMaxFre = frePre1
  //        singlePreMax = pre1
  //      }
  //
  //      for(j <- i + 1 until preSet.length) {
  //        val pre2 = preSet(j).toInt
  //        val twoPre = preSimMatrix.get(pre1).get(pre2)
  //        // if(twoPre == 0) { //特征集内部肯定是不会为0，只有在更新的时候，才会遇到这样的谓词
  //        //     continue // 应该可以实现较大剪枝。
  //        // }
  //
  //        val frePre2 = subNumPre(pre2)
  //        val minFre = min(frePre1, frePre2)
  //        //两个谓词之间的相关性
  //        val rel = twoFre / minFre
  //        sumRel = sumRel + rel
  //      }
  //    }
  //
  //    //一个谓词集合的谓词平均相关性
  //    avgPreRel = sumRel / (n *(n - 1) / 2);
  //    //空单元数 = 行数 * 列数 - 总的非空单元格数
  //    val sumNull = (n * singlePreMaxFre) - sumPreFre;
  //    //表格空值比例 [只是一个估值]
  //    ratioNull = sumNull / (n * singlePreMaxFre)
  //
  //    val schemaBenefit = avgPreRel / ratioNull
  //    if(schemaBenefit > preBenefitMax) {
  //      preBenefitMax = schemaBenefit
  //      preSetMaxBenefit = preSet
  //      avgPreRelMax = avgPreRel
  //      ratioNullMax = ratioNull
  //      preMax = singlePreMax
  //      tableSize = singlePreMaxFre
  //    }
  //    //返回值: 最大收益， 谓词集合(数组），空值比，平均谓词相关性，最大频次的谓词，谓词的最大频次
  //    (preBenefitMax, preSetMaxBenefit, ratioNullMax, avgPreRelMax, preMax, tableSize)
  //  }


}
