package MyQuery.nodeClass;

import org.apache.jena.graph.Triple;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by jiguangxi on 2022/1/6 下午12:11
 */
public class Super_Node {
    // 主语，   所有Local Node(由映射的到)
    public String subject;
//    public ArrayList<Triple> tps;
    public ArrayList<Local_Node> localnodes;

    public Super_Node(String sub,HashMap<String,ArrayList<Triple>> tab2lns){
        this.subject=sub;
        this.localnodes=constructLocalNodes(tab2lns);
    }

    //  为一个Super Node创建其对应的所有Local Node
    public ArrayList<Local_Node> constructLocalNodes(HashMap<String,ArrayList<Triple>> tab2lns){
        ArrayList<Local_Node> lns=new ArrayList<>();
        for(String tab:tab2lns.keySet()){
            Local_Node ln=new Local_Node(this.subject,tab,tab2lns.get(tab));
            lns.add(ln);
        }
        return lns;
    }

}
