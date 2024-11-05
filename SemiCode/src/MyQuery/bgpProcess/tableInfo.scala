package MyQuery.bgpProcess

import org.apache.spark.sql.DataFrame

/**
 * Created by jiguangxi on 2021/5/20 下午5:20
 */
case class tableInfo(tab:DataFrame, cnt:Double, query: subQueryComponent, isCurrentSemi:Boolean) extends Ordered[tableInfo] {
  override def compare(that: tableInfo): Int = (cnt-that.cnt).toInt
}