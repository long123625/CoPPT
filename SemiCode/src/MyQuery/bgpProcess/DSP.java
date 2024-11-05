package MyQuery.bgpProcess;

import java.util.ArrayList;
import java.util.List;

//  DSP一个局部点类
public class DSP {
    //  该DSP对应的基数值
    private double cardinality;
    //  该DSP中包含的所有顶点
    private  ArrayList<String> DSPvertexes;
    //  该DSP局部点连接的所有超点 (一个DSP中可能包含很多顶点，通过这些顶点和很多超点相连接)
    private  ArrayList<SuperNode> nextSuperNodeList=new ArrayList<>();
    //  该DSP对应的主语
    private String subject;
    //  该DSP包含的所有边(三元组模式)
    private ArrayList<String[]> edges;
    //  schema是对应的SPT表模式
    private List<String> schema;
    //  表名什么意思？？？？
    private String tableName;
    //
    private Long tableSize;

    //  构建DSP局部点对象
    public DSP(String subject, ArrayList<String[]> edges, List<String> schema, ArrayList<String> DSPvertexes){
        //   对应的实际存储的SPT表？？？？
        this.tableName=schema.get(0).substring(0,Math.min(254,schema.get(0).length()));
        this.subject=subject;
        this.edges=edges;
        this.DSPvertexes=DSPvertexes;
        this.schema=schema;
    }

    public String getTableName(){ return this.tableName;}

    public String getSubject(){
        return this.subject;
    }

    public ArrayList<String[]> getEdges(){
        return this.edges;
    }

    public List<String> getSchema(){
        return this.schema;
    }

    //  与下一层的超点进行连线
    public void addNext(SuperNode sp){
        this.nextSuperNodeList.add(sp);
    }

    public ArrayList<SuperNode> getNextSuperNodeList(){
        return this.nextSuperNodeList;
    }

    public  ArrayList<String> getDSPvertexes(){
        return this.DSPvertexes;
    }

    //  设置局部点的基数值    输入常量值，表大小？
    public void setCardinality(int constantNumber, String dpsTableSize){
        this.tableSize=Long.parseLong(dpsTableSize);
        cardinality=constantNumber==0?Double.MAX_VALUE:tableSize/constantNumber;
    }

    public double getCardinality(){
        return this.cardinality;
    }

    public double getTableSize(){return this.tableSize;}

}