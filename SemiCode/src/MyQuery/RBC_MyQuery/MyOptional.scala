package MyQuery.RBC_MyQuery

import MyQuery.Utils
import MyQuery.Utils._
import MyQuery.nodeClass.BGP_Node
import org.apache.jena.sparql.expr.Expr
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, expr}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.sketch.BloomFilter

import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable
/**
 * Created by jiguangxi on 2021/11/30 下午10:09
 * WDOPT查询处理逻辑:  基于CWDPT树,自上而下遍历获得每个节点的结果,   自下而上结果连接到一起;
 *    (1) 全局性布隆过滤器, 动态构造布隆过滤器, 从根到当前节点路径上只会计算一次;
 *    (2) BGP内代价模型, 根据估计元组数排序,获得最优连接顺序
 *    (3) optional 兄弟节点间代价模型, 最优顺序发现算法
 */
object MyOptional {

//  def upDown_WDPT_Executor(root: BGP_Node,varBfMap2: mutable.HashMap[String, BloomFilter]): DataFrame = {
//    val varBfMap=varBfMap2.clone()                 //  拷贝一个新的用于后续更新,(更新不会带到root的父节点与兄弟节点)
//    // 1. 计算当前CWDPT节点的查询结果
//    val query: String = Utils.constructSPARQL(root)//  构造当前BGP节点对应的SPARQL查询
//    var DFroot: DataFrame = MyBGP.executeBF(root,query,root.necessaryVarSet, root.optionVarGroup,
//            root.filterVarGroup,root.filterVarGroupMapping,root.gbExprs,varBfMap,root.isLeaf) //获得当前节点对应的结果
//    // 2. 计算各孩子节点查询结果, 并获得相关信息
//    var pos=0
//    root.getCh2ComVar()                   // 孩子节点们根据变量集分块
//    val liveNode=root.getLiveChildren()   // 孩子节点数量
//    val DFchs: Array[DFchoose] = new Array[DFchoose](root.children.size())    // 用以维护所有孩子表相关统计信息
//    for(chs:(String, util.ArrayList[BGP_Node])<-root.comVar2Ch){    // 对每一个分块进行处理, 每个分块判断一次Bloom Filter创建
//      // 当前分块的共享变量集  以及第一个共享变量(大部分情况下(CQof)都仅有一个)
//      val shareVar:util.HashSet[String]=new util.HashSet[String](chs._1.split(",").toSet) //将共享变量逗号分开,转化为集合set
//      val firstShareVar:String=shareVar.iterator().next() //迭代变量共享变量, 首先是第一个共享变量 (师兄这边节点之间的共享变量会很多吗)
//      if(Utils.bfOpt==true){
//        // 动态布隆过滤器   比较父子节点: 对于firstShareVar谓词, 其对应最小化 其最大可能取值
//        val rootMaxValue = root.getMaxValue(firstShareVar, preSval, preOval) //就是变量的候选数,从谓词的主语和谓词的宾语来寻找 // todo: 获得某变量的最小可能取值数
//        var childMaxValue=0
//        for(ch<-chs._2) //遍历有这些共享变量的孩子节点,找到这些变量的候选在所有孩子节点中的最大候选数,判断最后候选数是不是满足bloom filter的阈值
//          childMaxValue=Math.max(childMaxValue,ch.getMaxValue(firstShareVar,preSval, preOval))
//        if (!varBfMap.keySet.contains(firstShareVar)&&childMaxValue / (rootMaxValue * 1.0) >= 10) { //满足条件构造的bloom filter
//          varBfMap.put(firstShareVar,Utils.explodeJoinColumn(DFroot.select(firstShareVar), util.Arrays.asList(firstShareVar)).stat.bloomFilter(firstShareVar, rootMaxValue.toLong, 0.01))
//        }
//      }
//      for(ch <- chs._2){    // 遍历当前分块内所有孩子节点, 计算各孩子节点的结果以及对应统计信息   (同一分块内所有节点共享相同的变量集)
//        var DFch = upDown_WDPT_Executor(ch,varBfMap)    // 递归处理当前孩子节点
//        DFch = Utils.explodeJoinColumn(DFch, shareVar.toList).persist(StorageLevel.MEMORY_AND_DISK)
//        // todo: 只有一个孩子节点或者未启用代价模型优化时   直接赋值1,1即可,  否则计算代价模型
//        if(liveNode==1||Utils.costOpt==false)
//          DFchs(pos) = DFchoose(DFch, 1, 1, shareVar,pos)
//        else{
//          println("发现一个多孩子的节点   "+root.toString)
//          val cnt = DFch.count()
//          var firstcnt:Long=cnt
//          if(cnt>1000 && !DFch.schema.fields(0).name.equals(firstShareVar)) // firstShareVar列非空单元数目
//            firstcnt=DFch.select(firstShareVar).distinct().count()
//          DFchs(pos) = DFchoose(DFch, cnt, cnt / (1.0 * firstcnt), shareVar,pos)
//        }
//        pos=pos+1
//      }
//    }
//
//    // 3. 根据统计信息对孩子节点排序, 基于代价模型和有界分支限定  找到最优查询计划,  然后进行optional左外连接
//    // 未启用代价模型优化时,所有数据表信息都为1,1   BestPlanSearch()跑出来的顺序也是0,1,2...
//    val dd: Array[Array[Double]] = Array.ofDim[Double](root.children.size(), 3)
//    for (i <- 0 until root.children.size()) {
//      dd(i)=Array(DFchs(i).ts,100,i)
//      if(!DFchs(i).ev.isNaN)
//        dd(i)(1)=DFchs(i).ev
//    }
//    val pos_min2=new BestPlanSearch().findPlan(dd)
//    for(i <- 0 until pos_min2.size) {
//      DFroot = Utils.explodeJoinColumnByJoinVariable(DFroot, DFchs(i).shareVars)
//      DFroot = DFroot.join(DFchs(i).df , DFchs(i).shareVars.toList, "left") //"left_semi"
////      DFroot = DFroot.join(broadcast(DFchs(i).df) , DFchs(i).shareVars.toList, "left") //"left_semi"
//    }
//
//    // 4. GroupNull条件维护
//    for(optVars<-root.optionVarGroup){
//      if(optVars.size()>1&&DFroot.columns.toSet.containsAll(optVars)){
//        println("一个group null语句　　"+optVars+"    ")
//        var exprs = "CASE WHEN "
//        for (varr <- optVars)
//          exprs = exprs + varr + " IS NULL OR "
//        exprs = exprs.dropRight(3) + " THEN NULL ELSE "
//        println(exprs)
//        DFroot = optVars.foldLeft(DFroot)((acc, c) => acc.withColumn(c, expr(exprs + c + " END")))  // optVars中的
//      }
//    }
//
//
//
//    // 5. GroupFilter条件维护    论文中只写了单变量原子Filter条件的维护 (被合并节点必须全部为单变量原子Filter条件)
//    import org.apache.spark.sql.functions._
//    val epsRemove=new util.HashSet[Expr]()
//    for(ep:Expr<-root.filterVarGroup){
//      val optVars=Utils.getVarsExpr(ep)
//      val optCons=Utils.getConsExpr(ep)
//      val optSymbol=ep.getFunction.getFunctionSymbol.toString
//      println("======$$======"+optVars+"  "+optCons+"  "+optSymbol+"  "+ep.toString)
//      // todo: 单变量相关操作,可以直接进行处理(通过array数组内filter处理,仅保留所有满足条件的).   所有与单值相关的
//      if(optVars.size()==1){
//        // lt <;   eq =;   gt >;  le <=;   ge >=;   ne !=
//        val optVarsFirst=optVars.iterator().next()
//        println("类型  "+DFroot.dtypes.toMap.get(optVarsFirst)+"  "+DFroot.dtypes.toMap.get(optVarsFirst).get.contains("Array"))
//        // todo: 单主语变量原子Filter条件     如果变量即为主语(单值属性列) 且包含了该分组内所有元素  则case when处理   维护分组非空性
//        if(!DFroot.dtypes.toMap.get(optVarsFirst).get.contains("Array")){
//          if(DFroot.columns.toSet.containsAll(root.filterVarGroupMapping.get(ep))){
//            val exprs = "CASE WHEN !(" + translator.translate(ep)(0).asInstanceOf[String] + ") THEN NULL ELSE "
//            DFroot = root.filterVarGroupMapping.get(ep).foldLeft(DFroot)((acc, c) => acc.withColumn(c, expr(exprs + c + " END")))
//            epsRemove.add(ep)
//          }
//        } else{   // todo: 单变量非主语原子Filter条件    多值属性列过滤,直接基于udf函数进行
//          println("条件  "+optSymbol+"   "+ep)
//          val optConsFirst=optCons.iterator().next()
//          if(optSymbol.equals("symbol:lt")) {     // lt <;
//            epsRemove.add(ep)
//            DFroot=DFroot.withColumn(optVarsFirst,Utils.afLessValue(DFroot(optVarsFirst),lit(optConsFirst)))
//          } else if(optSymbol.equals("symbol:le")) {  // le <=;
//            epsRemove.add(ep)
//            DFroot=DFroot.withColumn(optVarsFirst,Utils.afLessEqualValue(DFroot(optVarsFirst),lit(optConsFirst)))
//          } else if(optSymbol.equals("symbol:eq")) {  // eq =;
//            epsRemove.add(ep)
//            DFroot=DFroot.withColumn(optVarsFirst,Utils.afEqualValue(DFroot(optVarsFirst),lit(optConsFirst)))
//          } else if(optSymbol.equals("symbol:ne")) { // ne !=
//            epsRemove.add(ep)
//            DFroot=DFroot.withColumn(optVarsFirst,Utils.afNotEqualValue(DFroot(optVarsFirst),lit(optConsFirst)))
//          } else if(optSymbol.equals("symbol:gt")) {// gt >
//            epsRemove.add(ep)
//            DFroot=DFroot.withColumn(optVarsFirst,Utils.afGraterValue(DFroot(optVarsFirst),lit(optConsFirst)))
//          } else if(optSymbol.equals("symbol:ge")) { // ge >=
//            epsRemove.add(ep)
//            DFroot=DFroot.withColumn(optVarsFirst,Utils.afGraterEqualValue(DFroot(optVarsFirst),lit(optConsFirst)))
//          }
//        }
//      } else if(optVars.size()==2&&optCons.size()==0&&ep.getFunction.getFunctionSymbol.toString == "symbol:eq"){
//        // todo: 两个变量相等情况
//        val optVarsFirst=ep.getVarsMentioned.toList(0).toString().replace("?","_")
//        val optVarsSecond=ep.getVarsMentioned.toList(1).toString().replace("?","_")
//        if(DFroot.dtypes.toMap.get(optVarsFirst).get.contains("Array")&&DFroot.dtypes.toMap.get(optVarsSecond).get.contains("Array")){
//          epsRemove.add(ep)
//          DFroot=DFroot.withColumn(optVarsFirst,Utils.afEqualVarAA(DFroot(optVarsFirst),DFroot(optVarsSecond)))
//          DFroot=DFroot.withColumn(optVarsFirst,Utils.afEqualVarAA(DFroot(optVarsSecond),DFroot(optVarsFirst)))
//        }else if(!DFroot.dtypes.toMap.get(optVarsFirst).get.contains("Array")){
//          epsRemove.add(ep)
//          DFroot=DFroot.withColumn(optVarsFirst,Utils.afEqualVarAA(DFroot(optVarsSecond),DFroot(optVarsFirst)))
//        }else {
//          epsRemove.add(ep)
//          DFroot=DFroot.withColumn(optVarsFirst,Utils.afEqualVarAA(DFroot(optVarsFirst),DFroot(optVarsSecond)))
//        }
//      }
//    }
//    println("已经处理了的条件aaaaaa")
//    for(ep<-epsRemove) {
//      println(ep)
//    }
//    root.filterVarGroup.removeAll(epsRemove)  //把上面已经处理了的进行删除, 后续不再进行处理
//    var needBestMatch=false
//    // todo: 对filterGroup部分的处理, 构造为case when语句(case when是在数组展开后使用的), 将所有不满足条件的都置为null ; 然后进行best-match   这里是合并后叶子节点的处理
//    //      (2) 尝试对一些复杂的进行处理
//    // 要么所有变量存在空值(),  要么需要满足条件
//    for(ep:Expr<-root.filterVarGroup){   // ep要判断的条件  optVar要置为null的分组
//      val optVars=root.filterVarGroupMapping.get(ep)
//      if(DFroot.columns.toSet.containsAll(optVars)){   // todp: 存在满足变量集包含关系的需要处理的复杂filterGroup
//        needBestMatch=true
//        val exprs="CASE WHEN "+optVars.mkString(" IS NULL OR ")+" IS NULL OR !("+translator.translate(ep)(0).asInstanceOf[String]+") THEN NULL ELSE "
//        println("一条filterGroup对应的case when语句  "+exprs+"  "+optVars)
//        // todo: 下面是先将所有列进行展开,然后再进行判断和过滤的!!! 如果不满足translator.translate(ep)(0).asInstanceOf[String]条件,
//        //  将filterVarGroupMapping.get(ep)中的变量都置为null
//        DFroot=Utils.explodeJoinColumn(DFroot,optVars.toList)
//        DFroot=optVars.foldLeft(DFroot)((acc, c) => acc.withColumn(c, expr(exprs + c + " END")))
//      }
//    }
//
//    // 接下来需要进行best-match操作,   基于共享变量排序,然后非共享部分 每个optional分组出一个变量, 窗口函数计算最大值  要么最大值和当前值都为空,要么都有值
//    if(needBestMatch==true){
//      println("开始处理一个包容数据集   展示当前BGP节点的结构")
//      DFroot.createOrReplaceTempView("tab")
//      var lastShareVar=new util.HashSet[String]()
//      val orderbyStr="over(partition by "+root.necessaryVarSet.mkString(",")+") as "
//      var windowsExpr="select "+DFroot.columns.toList.mkString(",")+" from (select "+DFroot.columns.toList.mkString(",")
//      // 每个optionVarGroup出一个变量用以进行判断吧
//      var ss1=""
//      var ss2=""
//      for(og<-root.optionVarGroup){
//        if(DFroot.columns.toSet.containsAll(og)){
//          println("一个可选分组  "+og)
//          val og11=new util.HashSet[String](og)
//          og11.removeAll(lastShareVar)
//          val og1=og11.iterator().next()
//          lastShareVar.add(og1)
//          ss1+=",max("+og1+") "+orderbyStr+"mm"+og1.substring(1)
//          ss2+="!("+og1+" is null and mm"+og1.substring(1)+" is not null) and "
//        }
//      }
//      windowsExpr+=ss1+" from tab) where ("+ss2.dropRight(4)+")"
//      if(!ss1.equals("")){
//        println("这里开始一次包容性查询 "+windowsExpr)
//        DFroot=sqlCtx.sql(windowsExpr)
//      }
//      DFroot=DFroot.distinct()
//    }
//
//    // 获得当前子树结果后, 使用剩余的optionVarGroup条件进行处理   (当前剩下的都是跨节点条件   如(?a,?b(父节点)|?c,?d(子节点))
//    for(optVars<-root.optionVarGroup){
//      if(optVars.size()>1&&DFroot.columns.toSet.containsAll(optVars)){
//        println("一个group null语句aaaaa　　"+optVars+"    ")
//        var exprs = "CASE WHEN "
//        for (varr <- optVars)
//          exprs = exprs + varr + " IS NULL OR "
//        exprs = exprs.dropRight(3) + " THEN NULL ELSE "
//        println(exprs)
//        DFroot = optVars.foldLeft(DFroot)((acc, c) => acc.withColumn(c, expr(exprs + c + " END")))  // optVars中的
//      }
//    }
//    DFroot
//  }
  def upDown_WDPT_Executor(root: BGP_Node, semiDF: DataFrame, pairOne: String, pairTwo: String, subtableName: String): DataFrame = {

    // 1. 计算当前CWDPT节点的查询结果
    val query: String = Utils.constructSPARQL(root)//  构造当前BGP节点对应的SPARQL查询
    var DFroot: DataFrame = MyBGP.executeBF(root, query, root.necessaryVarSet, root.gbExprs, semiDF, pairOne, pairTwo, subtableName, root.isSemi) //获得当前节点对应的结果
//    DFroot.show(10)
//    println(Utils.bfOpt)
  // 2. 计算各孩子节点查询结果, 并获得相关信息
    var pos=0
    root.getCh2ComVar()                   // 孩子节点们根据变量集分块
    val liveNode = root.getLiveChildren()   // 孩子节点数量
    val DFchs: Array[DFchoose] = new Array[DFchoose](root.children.size())    // 用以维护所有孩子表相关统计信息
    var cnt = 0
    for(chs:(String, util.ArrayList[BGP_Node]) <- root.comVar2Ch){    // 对每一个分块进行处理, 每个分块判断一次Bloom Filter创建
      // 当前分块的共享变量集  以及第一个共享变量(大部分情况下(CQof)都仅有一个)
      println(cnt + " cnt")
      cnt = cnt + 1
      val shareVar:util.HashSet[String] = new util.HashSet[String](chs._1.split(",").toSet) //将共享变量逗号分开,转化为集合set
      val firstShareVar:String = shareVar.iterator().next() //迭代变量共享变量, 首先是第一个共享变量 (师兄这边节点之间的共享变量会很多吗)

      var commCol: DataFrame = null
      //      var preSemi = ""
      var chToTableName: util.HashMap[BGP_Node, String] = new util.HashMap[BGP_Node, String]()
      var blockSemi = 0 // 0表示整个分块没有半连接
      if(Utils.bfOpt == true){
        println("开启需要半连接的模式")
        // 动态布隆过滤器   比较父子节点: 对于firstShareVar谓词, 其对应最小化 其最大可能取值
        val res = root.getMaxValue(firstShareVar, preSval, preOval)
        val rootMaxValue = res.get(0).toInt //就是变量的候选数,从谓词的主语和谓词的宾语来寻找 // todo: 获得某变量的最小可能取值数
//        var childMaxValue = 0

        var maxCha = Integer.MIN_VALUE * 1.0

        println("size is " + rootMaxValue)
        for(ch <- chs._2) { //遍历孩子节点的分块
          var childMaxValue = 0 //每次遍历，孩子节点的基数都要重新算
          val res = ch.getMaxValue(firstShareVar, preSval, preOval)
          val chInt = res.get(0).toInt
          val varSemi = res.get(1)
          val valPre = res.get(2)
          val varPos = res.get(3)
          var subtableName = "NullTableName"
          println("判断是否需要版链接")
          childMaxValue = Math.max(childMaxValue, chInt)

          var tmp = childMaxValue / (rootMaxValue * 1.0)
          println(childMaxValue + " " + tmp)
//          tmp = 10
          if( tmp >= 5 && tmp > maxCha) {
            println("需要版链接" + valPre)
            ch.isSemi = true
            subtableName = pIndexSchema(valPre)(0)
            subtableName = subtableName.substring(0, Math.min(254, subtableName.length))
            println(" subtable Name is " + subtableName)
            chToTableName.put(ch, subtableName)
            blockSemi = 1 // 这次分块内有要半连接的孩子节点
            if(varPos.equals("0")) {
              ch.pairFirst = "subject"
              ch.pairSecond = varSemi
            }
            else if(varPos.equals("2")) {
              ch.pairFirst = valPre
              ch.pairSecond = varSemi
            }

            maxCha = tmp
          }
          else {
            println(" No semi join ")
            println(blockSemi + " blockSemi")
          }
        } // 遍历相同变量的分块
        if(blockSemi == 1) {
          //一个分块因为变量相同，只需要选一个半连接表就行
          commCol = DFroot.select(col(firstShareVar))
        }

      }
      for(ch <- chs._2){    // 遍历当前分块内所有孩子节点, 计算各孩子节点的结果以及对应统计信息   (同一分块内所有节点共享相同的变量集)
//        println("zhe bian  chu bug  l ")
        var tableName2: String = ""
        if(blockSemi == 0) {
          println(" No semi join ")
          tableName2 = "NullTableName"
        }else {
          println(" have semi join ")
          tableName2 = chToTableName.get(ch)
        }
//        val tableName2 = chToTableName.getOrElse(ch, "NullTableName")
        var DFch = upDown_WDPT_Executor(ch, commCol, ch.pairFirst, ch.pairSecond, tableName2)    // 递归处理当前孩子节点
        DFch = Utils.explodeJoinColumn(DFch, shareVar.toList).persist(StorageLevel.MEMORY_AND_DISK)
        // todo: 只有一个孩子节点或者未启用代价模型优化时   直接赋值1,1即可,  否则计算代价模型
        if(liveNode==1 || Utils.costOpt==false)
          DFchs(pos) = DFchoose(DFch, 1, 1, shareVar,pos)
        else{
          println("发现一个多孩子的节点   "+root.toString)
          val cnt = DFch.count()
          var firstcnt:Long=cnt
          if(cnt>1000 && !DFch.schema.fields(0).name.equals(firstShareVar)) // firstShareVar列非空单元数目
            firstcnt=DFch.select(firstShareVar).distinct().count()
          DFchs(pos) = DFchoose(DFch, cnt, cnt / (1.0 * firstcnt), shareVar,pos)
        }
        pos=pos+1
      }
    }

    // 3. 根据统计信息对孩子节点排序, 基于代价模型和有界分支限定  找到最优查询计划,  然后进行optional左外连接
    // 未启用代价模型优化时,所有数据表信息都为1,1   BestPlanSearch()跑出来的顺序也是0,1,2...
    val dd: Array[Array[Double]] = Array.ofDim[Double](root.children.size(), 3)
    for (i <- 0 until root.children.size()) {
      dd(i)=Array(DFchs(i).ts,100,i)
      if(!DFchs(i).ev.isNaN)
        dd(i)(1)=DFchs(i).ev
    }
    val pos_min2=new BestPlanSearch().findPlan(dd)
    for(i <- 0 until pos_min2.size) {
      DFroot = Utils.explodeJoinColumnByJoinVariable(DFroot, DFchs(i).shareVars)
      DFroot = DFroot.join(DFchs(i).df , DFchs(i).shareVars.toList, "left") //"left_semi"
    }
    DFroot
  }
}
