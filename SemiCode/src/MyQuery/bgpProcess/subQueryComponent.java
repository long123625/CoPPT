package MyQuery.bgpProcess;

import javafx.util.Pair;

import java.io.Serializable;
import java.util.*;

//  分解得到的单个子查询类
// 每个子查询主要包含以下几部分：(1)子查询对应的SPT表(表名) (2)SPT表中列名到SPARQL查询变量名的映射，
// (3)SPARQL查询where条件中列名到常量值的映射  (4)SPARQL查询中包含的所有变量   (5)SPARQL查询中的连接变量
public class subQueryComponent  implements Serializable {
    public String tableName;
    public String subject;
    private ArrayList<Pair<String, String>> selectAs = new ArrayList<>();
    private ArrayList<Pair<String, String>> where = new ArrayList<>();
    private Set<String> joinVariables = new HashSet<>();
    public HashMap<String,Integer> varNum=new HashMap<>();     // 每个变量对应的最小取值数

    //  构建一个subQueryComponent  子查询对象
    //   输入主语、DSP对应的所有三元组模式/边， DSP对应的模式， 之前查询已涉及到的变量, 谓词对应主语实体数,  谓词对应宾语实体数
    public subQueryComponent(String subject, ArrayList<String[]> edges, List<String> schema, Set<String> totalQueryVariables,
                             HashMap<String, Integer> subNumPre, HashMap<String, Integer>  objNumPre) {
        //  获得实际表名
        this.tableName = schema.get(0).substring(0, Math.min(254, schema.get(0).length()));
        //  该子查询中出现的所有变量
        Set<String> subQueryVariables = new HashSet<>();
        //  对主语分为变量、常量进行操作    selectAs字段 第一个元素为"subject"列名，第二个是变量或值
        if (subject.startsWith("?")) {
            subject = subject.replace("?","_");
            selectAs.add(new Pair<>("subject", subject));
            subQueryVariables.add(subject);
            joinVariables.add(subject);
        } else {  // 主语是常量
            selectAs.add(new Pair<>("subject", "subject"));
            where.add(new Pair<>("subject", subject));
        }
        this.subject = subject;
        //  遍历涉及的所有边       todo: ========在这里将每个变量对应的取值获得吧=======
        for (String[] edge : edges) {
            if(edge[0].startsWith("?")){
                edge[0] = edge[0].replace("?","_");
                varNum.put(edge[0], Math.min(varNum.getOrDefault(edge[0], Integer.MAX_VALUE), subNumPre.getOrDefault(edge[1],10000)));// 更新主语的最小基数[主语变量的候选数]
            }
            //  对宾语进行操作   selectAs字段 第一个元素为  谓词对应的列名，第二个是变量或值
            if (edge[2].startsWith("?")) {
                edge[2] = edge[2].replace("?","_");
                selectAs.add(new Pair<>(edge[1], edge[2]));
                subQueryVariables.add(edge[2]);
                joinVariables.add(edge[2]);
                // 可能是非关系谓词   就没有的
                if(objNumPre.containsKey(edge[1]))
                    varNum.put(edge[2],Math.min(varNum.getOrDefault(edge[2],Integer.MAX_VALUE),objNumPre.getOrDefault(edge[1],10000)));  //更新宾语的最小基数
            } else {
                edge[1] = edge[1].replace("?","_");
                int flag = 0;
                for (Pair<String, String> selectA : selectAs) {
                    if(edge[1].equals(selectA.getKey()))
                        flag=1;
                }
                if(flag==0)
                    selectAs.add(new Pair<>(edge[1], edge[1]));
                where.add(new Pair<>(edge[1], edge[2])); // 因为是常量,
            }
        }
        //获得join变量   与之前子查询中的变量进行交操作，得到其包含的连接变量
        this.joinVariables.retainAll(totalQueryVariables);
        //更新目前为止  所有子查询的变量
        totalQueryVariables.addAll(subQueryVariables);
    }

    public String getTableName() {
        return this.tableName;
    }
    public void setTableName(String tableName){
        this.tableName=tableName;
    }

    public void setVarNum(HashMap<String,Integer> mapp){ this.varNum=mapp; }
    public HashMap<String,Integer> getVarNum(){return this.varNum;}

    public ArrayList<Pair<String, String>> getSelectAs() {
        return this.selectAs;
    }
    public int getVarPos(String bfName){
        int i=0;
        for(Pair<String, String> x: this.getSelectAs()){
            if(x.getValue().equals(bfName))
                return i;
            i++;
        }
        return -1;
    }

    public HashSet<String> getSelect(){
        HashSet<String> sels=new HashSet<>();
        for (Pair<String, String> selectA : this.selectAs)
            sels.add(selectA.getValue());
        return sels;
    }
    public ArrayList<Pair<String, String>> getWhere() {
        if (this.where.size() != 0) return this.where;
        else return null;
    }

    public List<String> getJoinVariables() {
        return new ArrayList<>(this.joinVariables);
    }

    public List<String> getAllVariables() {
        HashSet<String> se=new HashSet<>();
        for(int i=0;i<this.selectAs.size();i++){
            se.add(this.selectAs.get(i).getValue());
        }
        return new ArrayList<>(se);
    }

    public void printSubQuery() {
        System.out.print("selectAs:");
        for (Pair<String, String> pair : selectAs) System.out.print(pair.getKey() + " as " + pair.getValue() + ",");
        System.out.println();
        System.out.print("Where:");
        for (Pair<String, String> pair : where) System.out.print(pair.getKey() + " = " + pair.getValue() + ",");
        System.out.println();
        System.out.print("joinVariables:");
        for (String jv : joinVariables) System.out.print(jv + ",");
        System.out.println();
    }

    public  void printVarNum(){
        for(String key:varNum.keySet()){
            System.out.print(key+"  "+varNum.get(key));
        }
        System.out.println();
    }

}