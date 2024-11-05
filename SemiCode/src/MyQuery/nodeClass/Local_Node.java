package MyQuery.nodeClass;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.expr.Expr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by jiguangxi on 2022/1/6 下午12:11
 *
 *change by hongshen on 2022/8/23 20:26
 */
public class Local_Node {
    // 主语，　　数据表，　　三元组模式
    //  方法：　获得主语　数据表；　　获得三元组模式；　　判断是否存在常量
    public String  subject;
    public String table;
//    public ArrayList<Triple> tps;
    public  HashSet<Triple> tps;
    public ArrayList<Expr> exprs;

    public ArrayList<Triple> tpsUp; // 来自上面节点的三元组
    public ArrayList<Expr> exprsUp; //合并三元组时，带来的局部表达式
    public Set<String> varSetUp; //合并三元组中的变量,不包括subject

    public Local_Node(String sub,String tab, ArrayList<Triple> tps){
        this.subject=sub;
        this.table=tab;
        this.tps=new HashSet<>(tps);
        this.exprs=new ArrayList<>();

        this.tpsUp = new ArrayList<>(); //
        this.exprsUp = new ArrayList<>(); //
        this.varSetUp = new HashSet<>(); //
    }



    // 判断节点中是否存在常量宾语
    public Boolean noConstObject(){
        for(Triple tp:this.tps){
            if(!(tp.getObject().toString().startsWith("?")||tp.getObject().toString().startsWith("_")))
                return false;
        }
        return true;
    }
    // 获得当前Local Node的所有三元组模式
//    public ArrayList<Triple>  getTPs(){
//        return this.tps;
//    }
    public HashSet<Triple> getTPs() {
        return this.tps;
    }
    // 获得当前Local Node的所有变量集
    public HashSet<String> getVars(){
        HashSet<String> vars=new HashSet<>();
        for(Triple tp:this.tps){
            String sub=tp.getSubject().toString().replace("?","_");
            String obj=tp.getObject().toString().replace("?","_");
            if(sub.startsWith("_"))
                vars.add(sub);
            if(obj.startsWith("_"))
                vars.add(obj);
        }
        return vars;
    }
    // 判断模式是否与指定模式相同
    public Boolean sameSchema(String sub,String tab){
        return this.subject.equals(sub)&&this.table.equals(tab);
    }

}
