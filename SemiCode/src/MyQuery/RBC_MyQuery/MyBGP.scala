package MyQuery.RBC_MyQuery

import MyQuery.Utils.{StoragePath, translator, _}
import MyQuery.bgpProcess.subQueryComponent
import breeze.linalg.*

import scala.Console.in
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
//import MyQuery.bgpProcess.subQueryComponent
import MyQuery.bgpProcess.tableInfo
import MyQuery.bgpProcess.QueryDecomposer
//import MyQuery.bgpProcess.{QueryDecomposer, subQueryComponent, tableInfo}
import MyQuery.nodeClass.BGP_Node
import MyQuery.{Utils, bgpProcess}
import org.apache.jena.sparql.expr.Expr
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.util.sketch.BloomFilter

import java.util
import java.util.Collections
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
 * Created by jiguangxi on 2021/11/30 下午9:47
 * BGP查询处理逻辑  主要流程:
 * (1) 查询分解  (2) 读取各自对应的数据表,filter过滤,分组可空,  (3) 估计表元组数并排序
 * (4) 动态应用布隆过滤器 (5) 使用布隆过滤器   (6) 数据表连接
 */
object MyBGP {

  def selectColumnsAccordingConditions(table:DataFrame,subQuery:subQueryComponent):DataFrame ={
    if(subQuery.getWhere != null) table
      .where{
//        subQuery.getWhere.map{ columnAndCondition=> "array_contains("+columnAndCondition.getKey+",'"+columnAndCondition.getValue+"')"}.mkString(" && ")
        subQuery.getWhere.map{ columnAndCondition=> "array_contains("+columnAndCondition.getKey+",'"+columnAndCondition.getValue+"')"}.mkString(" AND ")
      }
      .select(subQuery.getSelectAs.filter(_.getValue.startsWith("_")).map(tableColumnAndNewName=>col(tableColumnAndNewName.getValue)):_*)
    else table
      .select(subQuery.getSelectAs.filter(_.getValue.startsWith("_")).map(tableColumnAndNewName=>col(tableColumnAndNewName.getValue)):_*)
  }

  // 输入: 查询,  当前BGP节点的必选变量集;   可空分组(条件);
  //      当前节点对应的所有Filter条件;  Filter条件对应的变量集;  全局filter条件, 布隆过滤器映射,  是否为也子
  def executeBF(root:BGP_Node, query: String, var_list: util.HashSet[String],
                gbExprs: util.ArrayList[Expr], semiDF: DataFrame, pairOne: String, pairTwo: String, subTableName: String, isSemi: Boolean): DataFrame = {
    val executeStartTime = System.currentTimeMillis()
    // 1. 查询分解, 获得若干子查询
    val subQueryList = new QueryDecomposer(query, pIndexSchema, preSval, preOval).getSubQueryList
    var isFirstSemi = false // 每个孩子节点只进行一次半连接
    val tts: Array[tableInfo] = new Array[tableInfo](subQueryList.size())       //  所有子查询对应的数据表
    var intermediateResult: DataFrame = null                                    //  中间结果表

    // (2) 数据表读取, 估计表的基数  并根据基数对TableInfo进行排序
    val tableReadStartTime = System.currentTimeMillis()
    for (i <- 0 until subQueryList.size() ) {
      // todo: 每个子查询对应了一个local node
      val subQuery = subQueryList.get(i)
      // (2.0) 获得当前子查询涉及的所有必选变量
      val tablevars = new util.HashSet[String](var_list)
      tablevars.retainAll(subQuery.getSelectAs.map(tableColumnAndNewName => tableColumnAndNewName.getValue)) //

      // (2.1) 读取数据表  注意部分位置可空,而部分位置不可空
      println("表名  "+StoragePath + "Storage//" + subQuery.getTableName)
      var table: DataFrame = sqlCtx.read.parquet(StoragePath + "Storage//" + subQuery.getTableName)
        .select(subQuery.getSelectAs.map(tableColumnAndNewName => col(tableColumnAndNewName.getKey) as tableColumnAndNewName.getValue): _*)
        .na.drop(tablevars.toSeq)


      table = selectColumnsAccordingConditions(table, subQuery)


      // (2.3) 利用filter进行过滤  先找出所有符合变量集包含关系的条件,然后构造一个where语句 进行过滤.
      //  注意区别   exprs中为之前就有的条件   即真的filter ,  而filterVarGroup中 为孩子节点的filter条件
      val tableName = subQuery.tableName
      val tableSubject = subQuery.subject

      val ln = root.findLocalNodeBySchema(subQuery.subject, subQuery.tableName)
      if(ln != null && ln.exprs.size()>0){
        val (where_st, varSet) = visitFilter(ln.exprs)
        println("这里是一个build in filter  " + where_st)
        table = Utils.explodeJoinColumnByJoinVariable(table, varSet)
        table = table.filter(where_st)

        //合并下来的三元组中的filter表达式
        val (where_st_Up, varSet_Up) = visitFilter(ln.exprsUp)
        println(where_st_Up.length + " " + varSet_Up.size())
        println("这里是一个build in Up_filter var  " + varSet_Up)
        println("这里是一个build in Up_filter  " + where_st_Up)
        if(where_st_Up.length != 0 && varSet_Up.size() != 0) {
          table = Utils.explodeJoinColumnByJoinVariable(table, varSet_Up)
          table = table.filter(where_st_Up)
        }
      }

      //然后是删除掉合并下来的变量的列
      val columns_to_drop = ln.varSetUp.toList
//      val coll = List("1", "2", "3")
//      for(col2 <- coll) {
//        table.drop(col(col2))
//      }
      var tmpCol = ListBuffer()
      for(col2 <- columns_to_drop) {
//        tmpCol.add(col2)
        println("zheli shanchu lie **************************************************************")
        table = table.drop(col(col2))
      }
      var isCurrentSemi = false
      //在这边同时把半连接执行了，因为dataframe不能改，后续就算半连接改变了table 但是并不能重新赋值回去
      //      val isThisTbale = subQuery.getSelectAs.contains(new Pair(pairOne, pairTwo))

      var isEqualSubject: Boolean = false
      var isEqualTableName: Boolean = false
      var isPredObj: Boolean = false
      println("输出表名和主语，谓词和谓词变量等")
      println(tableName + " " + tableSubject + " " + pairOne + " " + pairTwo + " " + subTableName)
      if(pairOne == "subject") {

        //如果是连接变量是主语变量，那么用subject + tableName判断
        isEqualSubject = tableSubject.equals(pairTwo)
        isEqualTableName = tableName.equals(subTableName)
      }
      else {
        isPredObj = subQuery.getSelectAs.contains(new Pair(pairOne, pairTwo))
      }

//      if(semiDF != null) {
//        println(" semi DataFrame show")
//        semiDF.show(10)
//      }

      if(isFirstSemi == false && ((isEqualTableName && isEqualSubject) || isPredObj)) {
        println(" 执行版链接")
        //是第一次进行半连接，并且当前子查询含有是要进行半连接的子查询
        isFirstSemi = true //更新， 使得每个孩子节点只能进行一次半连接
        isCurrentSemi = true

        var tmpList = new util.ArrayList[String]()
        tmpList.add(pairTwo) //存半连接的公共列，即连接变量
        table = Utils.semiJoin(tmpList, table, semiDF) //自定义实现，强制广播半连接 or让spark 引擎本身去选择是否广播半连接 函数内部去实现广播

        //        firstDataFrame = table // 存参与半连接的dataframe
        intermediateResult = table //如果当前子查询有半连接，那么就把半连接缩减的表作为第一个表
      }
//      table.explain()

      // (2.4) 构造每个子查询对应的相关统计信息
      tts(i) = bgpProcess.tableInfo(table, getTableScore(subQuery), subQuery, isCurrentSemi)
    }

    scala.util.Sorting.quickSort(tts)   //  (3) 根据估计的元祖数进行排序
    val tableReadEndTime = System.currentTimeMillis()
    println("数据表读取所用时间 " + (tableReadEndTime - tableReadStartTime))

    // (4) 动态选择需要构造布隆过滤器的谓词  一个map,[String, List<Int>]记录了每个变量对应的所有主语数
    val bfVarNum=new mutable.HashSet[String]()

    //  (3) 真正执行连接
    val tableJoinStartTime=System.currentTimeMillis()
    val vis: Array[Int] = new Array[Int](subQueryList.size()) // 已访问标记
    for (k <- 0 until tts.length) {
      // 下面是找出第i个元素,使得i是目前未使用的元祖数最小 且与前面查询有公共变量的查询
      if (tts(k).isCurrentSemi == true) vis(k) = 1 //把半连接的表标记了
      if (tts(k).isCurrentSemi == false) { //如果是true，即是半连接过的表跳过不处理，因为作为第一个起始表了
        // 下面是找出第i个元素,使得i是目前未使用的元祖数最小 且与前面查询有公共变量的查询
        var i: Int = 0
        breakable {
          for (j <- 0 until tts.length) {
            if ((vis(j) == 0) && (intermediateResult == null || hasSharedVar(intermediateResult.columns.toList, tts(j).tab.columns.toList) == 1)) {
              i = j
              break()
            }
          }
        }
        vis(i) = 1
        // 基于连接变量, 将tts(i).tab表进行展开
        val joinVariables = new util.HashSet[String]()
        if (intermediateResult != null)
          joinVariables.addAll(intermediateResult.columns.toList)
        joinVariables.retainAll(tts(i).tab.columns.toList)

        intermediateResult = Utils.explodeJoinColumn(intermediateResult, joinVariables.toList)

        //        //  还有可能有变量在varBfMap中已经有了
        //        val varBfVars = new util.HashSet[String](varBfMap.keySet)
        //        varBfVars.retainAll(tts(i).tab.columns.toList)

        //        var tab = Utils.explodeJoinColumn(Utils.explodeJoinColumn(tts(i).tab, joinVariables.toList), varBfVars.toList)
        var tab = Utils.explodeJoinColumn(tts(i).tab, joinVariables.toList) //不用像之前一样在map的变量列炸开一次了，因为暂时不使用map

        intermediateResult = Utils.innerJoin(joinVariables.toList, tab, intermediateResult)

      }
    }

    // (4) 利用全局filter进行过滤
    if(gbExprs.size()>0) {
      val (where_st,varSet)=visitFilter(gbExprs)
      println("这里是一个条件build in " + where_st)
      intermediateResult = Utils.explodeJoinColumnByJoinVariable(intermediateResult, varSet)
      intermediateResult = intermediateResult.filter(where_st)
    }

    val tableJoinEndTime = System.currentTimeMillis()
    println("需要构造布隆过滤器的变量  "+ bfVarNum + "    ;数据表连接所用时间 "+(tableJoinEndTime-tableJoinStartTime))
    val executeEndTime = System.currentTimeMillis()
    println("执行时间 " + (executeEndTime - executeStartTime) + "ms\t")
    intermediateResult
  }


  // todo: 估计一个子查询的结果数
  def getTableScore(subQuery:subQueryComponent):Double={  //(找一个最小的谓词对应主语数量作为表的基数,然后再乘以各个连接谓词的选择性 以作为估计的表元组数)
    var score:Double=1
    var base:Double=Integer.MAX_VALUE
    for(prePair<-subQuery.getSelectAs)
      if(prePair.getKey!="subject") {
        base=Math.min(base, preSval.getOrDefault(prePair.getKey,0).toInt)     // 计算表的基数  这里属于粗略估计  取谓词对应主语数最小值,但是可能小于该值的
        if(subQuery.getJoinVariables.contains(prePair.getValue))  // 只有公共变量才会被展开
          score=score*preSelector.getOrDefault(prePair.getKey,0)
      }
    score*base
  }

  // todo: 构造OpFilter节点对应的sql where语句
  def visitFilter(eps: util.ArrayList[Expr]): (String, util.HashSet[String]) = {
    var where_st=""
    val varSet = new util.HashSet[String]()
    for(x <- eps){    // 遍历每个条件 获得其转换后的where语句
      val trans = translator.translate(x)
      varSet.addAll(trans(1).asInstanceOf[util.HashSet[String]])
      where_st += trans(0).asInstanceOf[String]+" and "
    }
    where_st = where_st.dropRight(5)
    (where_st, varSet)
  }

  def hasSharedVar(cols1:util.List[String],cols2:util.List[String]): Int ={
    for(x<-cols1)
      if(cols2.contains(x))
        return 1
    0
  }
}
