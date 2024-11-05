package MyQuery.bgpProcess;


import java.util.*;
import java.util.regex.Pattern;

public class QueryDecomposer {
    private HashMap<String,ArrayList<String[]>> vmap_s = new HashMap<>();     //以vertex为subject的三元组模式
    private ArrayList<subQueryComponent> subQueryList = new ArrayList<>();   //BGP查询分解的结果
    private scala.collection.Map<String, String[]> pIndexSchema = null;       // 谓词到数据表的映射
    public Set<String> variables=new HashSet<>();
    private Queue<String> vertexQueue = new ArrayDeque<>();
    private ArrayList<String[]> BGP = new ArrayList<>();
    private Set<String> objects = new HashSet<>();
    private HashMap<String,Integer> subNumPre = null;     //  谓词对应的主语实体数
    private HashMap<String,Integer> objNumPre = null;     //  谓词对应的宾语实体数

    public QueryDecomposer(String query, scala.collection.Map<String, String[]> pIndexSchema,
                           HashMap<String,Integer> subNumPre, HashMap<String,Integer> objNumPre) {
        this.subNumPre=subNumPre;
        this.objNumPre=objNumPre;
        this.pIndexSchema=pIndexSchema;
        decomposeQuery(query);
    }

    public ArrayList<subQueryComponent> getSubQueryList(){
        return this.subQueryList;
    }

    private void decomposeQuery(String query){
        queryToSubjectStar(query);      // 分解为星模式?
        subjectStarToSubqueries();      // 为每个主语构造查询?
    }

    private void queryToSubjectStar(String query){
        String condition = query.split("where\\s*\\{\\s*|WHERE\\s*\\{\\s*")[1];
        String[] conditionElements = condition.substring(0, condition.length() - 1).trim().split("\\s+\\.");
        Pattern MatchPattern = Pattern.compile("[^a-zA-Z0-9]");
        for(String pattern : conditionElements){  // 遍历每条三元组模式,
            String[] patternElements = pattern.trim().split("\\s+");
            patternElements[1] = MatchPattern.matcher(patternElements[1]).replaceAll("").toLowerCase();
            ArrayList<String[]> relevantDataPattern = vmap_s.getOrDefault(patternElements[0], new ArrayList<>());
            relevantDataPattern.add(patternElements);
            BGP.add(patternElements);
            objects.add(patternElements[2]);
            vmap_s.put(patternElements[0], relevantDataPattern);
        }
    }

    private void subjectStarToSubqueries(){
        initialRoot();
        while(!vertexQueue.isEmpty()){
            String subject = vertexQueue.poll();
            ArrayList<String[]> edgesAndAdjVertex = vmap_s.get(subject);
            if(edgesAndAdjVertex!=null) decomposeSubjectStar(subject,edgesAndAdjVertex);
        }
    }

    private void initialRoot(){
        ArrayList<String> subjectWithoutInEdges=new ArrayList<>(vmap_s.keySet());
        subjectWithoutInEdges.removeAll(objects);
        vertexQueue.add(subjectWithoutInEdges.get(0));
    }

    private void decomposeSubjectStar(String subject,ArrayList<String[]> starEdges){
        int starEdgesSize = starEdges.size();
        while(starEdgesSize != 0){
            String adjVar;
            String[] starEdge;
            int i = 0;
            do {
                starEdge = starEdges.get(i);
                adjVar = starEdge[2];
                i++;
            }while(!variables.contains(adjVar) && variables.size() != 0 && i < starEdgesSize);
            String p1 = starEdge[1];
            List<String> schema = Arrays.asList(pIndexSchema.get(p1).get());
            ArrayList<String[]> candidateEdges = new ArrayList<>();
            for(String[] bgp : BGP){
                if(bgp[2].equals(subject)) vertexQueue.add(bgp[0]);
            }
            for(String[] edge: starEdges){
                if(schema.contains(edge[1])){
                    candidateEdges.add(edge);
                    for(String[] bgp:BGP){
                        if(bgp[0].equals(edge[2])) vertexQueue.add(edge[2]);
                        else if(bgp[2].equals(edge[2]) && !bgp[0].equals(subject)) vertexQueue.add(bgp[0]);
                    }
                }
            } //总之就连接的三元组都要处理
            starEdges.removeAll(candidateEdges); //剔除一个子查询的三元组
            starEdgesSize -= candidateEdges.size();
            subQueryList.add(new subQueryComponent(subject, candidateEdges, schema, variables, subNumPre, objNumPre)); //获得主语，主语子查询中的三元组，数据表，变量，主语个数，宾语个
        } //while循环一次生成一个子查询对象，添加到list中，直到一个主语的所有子查询都构造完对象
    }
}