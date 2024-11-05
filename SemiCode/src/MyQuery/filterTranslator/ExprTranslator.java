package MyQuery.filterTranslator;

import MyQuery.Utils;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.expr.*;

import java.util.HashSet;
import java.util.Stack;

//  todo: 给定一个Expr表达式,获得其对应的sql where语句
public class ExprTranslator implements ExprVisitor {
    private final Stack<String> stack; // 中间结果栈
    private final PrefixMapping prefixes;
    private final HashSet<String> variables;

	public ExprTranslator(PrefixMapping _prefixes) {
        stack = new Stack<>();
        prefixes = _prefixes;
        variables=new HashSet<>();
    }

    // 输入:Expr条件表达式,每个Expr都是Function函数
    // 一个Function 由若干变量,常量以及它们之间的关系构成
    public Object[] translate(Expr expr) {
        expr.visit(new ExprWalker(this));
        if(stack.isEmpty())
            return null;
        String condition=stack.pop().replace("?","_");
        if(!condition.startsWith("("))
            condition+="("+condition+")";
        return new Object[]{condition,variables};
    }

    //  下面几个函数  对ExprFunction进行访问,这里自动根据传递的ExprFunction孩子个数 找到对应的函数(重载)
    @Override
    public void visit(ExprFunction0 exprFunction0) { }

    //  访问一个ExprFunction1类型的节点,该节点有一个孩子(变量或常量)  从栈中出栈一个元素进行处理
    @Override
    public void visit(ExprFunction1 func) {
	    boolean before = true;
        String sub = stack.pop();
        sub=sub.replace("(","");
        String operator = Utils.NO_SUPPORT();
        if (func instanceof E_LogicalNot) {   // 逻辑非  取反操作
            if (func.getArg() instanceof E_Bound) { // 如果内层是bound,则这里要求的就是为null
                operator = Utils.NOT_BOUND();
                sub = sub.substring(0, sub.indexOf(Utils.BOUND()));
                before = false;
            } else
                operator = Utils.LOGICAL_NOT();  // 否则的话就是直接逻辑非
        } else if (func instanceof E_Bound) {    // 绑定
            operator = Utils.BOUND();
            before = false;
        } else if (func instanceof E_Str || func instanceof E_Lang){   //字符串或者是Lang?
        	operator = "";
        }

        if (operator.equals(Utils.NO_SUPPORT())) {
            throw new UnsupportedOperationException("Filter expression not supported yet!");
        } else {
            if (before) {
                stack.push( "("+operator + sub+")" );
            } else {
                stack.push("("+sub + operator +")");
            }
        }
    }

    // 访问一个ExprFunction2类型的节点,该节点有两个孩子(变量或常量)   从栈中出栈两个元素进行处理
    @Override
    public void visit(ExprFunction2 func) {
	    String right = stack.pop();
        String left = stack.pop();
        String operator = Utils.NO_SUPPORT();
        if (func instanceof E_GreaterThan) {   // 大于关系
//            operator = Tags.GREATER_THAN;
            operator= Utils.GREATER_THAN();
        } else if (func instanceof E_GreaterThanOrEqual) {  // 大于等于关系
            operator = Utils.GREATER_THAN_OR_EQUAL();
        } else if (func instanceof E_LessThan) {  // 小于关系
            operator = Utils.LESS_THAN();
        } else if (func instanceof E_LessThanOrEqual) {   // 小于等于关系
            operator = Utils.LESS_THAN_OR_EQUAL();
        } else if (func instanceof E_Equals) {   //等于关系
            operator = Utils.EQUALS();
        } else if (func instanceof E_NotEquals) {  // 不等于关系
            operator = Utils.NOT_EQUALS();
        } else if (func instanceof E_LogicalAnd) {  // 逻辑与
            operator = Utils.LOGICAL_AND();
        } else if (func instanceof E_LogicalOr) {  //  逻辑或
            operator = Utils.LOGICAL_OR();
        } else if(func instanceof E_Add){       // 加
        	operator = Utils.ADD();
        }else if(func instanceof E_Subtract){   // 减
        	operator = Utils.SUBTRACT();
        } else if (func instanceof E_LangMatches){   // like匹配
        	operator = Utils.LANG_MATCHES();
        }
        if (operator.equals(Utils.NO_SUPPORT())) {
            throw new UnsupportedOperationException("Filter expression not supported yet!");
        }  else if(operator.equals(Utils.LANG_MATCHES())){   //  like匹配时这里要单独处理
        	right = "%@" + right.split("\"")[1];
        	stack.push("(" + left + operator + "'"+ right + "'"+ ")");
        } else {
            stack.push("(" + left + operator + right+ ")");
        }
    }

    @Override
    public void visit(ExprFunction3 func) {
        throw new UnsupportedOperationException("ExprFunction3 not supported yet.");
    }

    // 访问一个ExprFunctionN类型的节点  该节点有多个孩子(变量或常量)    目前只考虑了正则关系  E_Regex
    @Override
    public void visit(ExprFunctionN func) {
        if(func instanceof E_Regex){   // 这里是正则关系
            String operator = Utils.NO_SUPPORT();
            String right = stack.pop();
            String left = stack.pop();
            if (right.startsWith("\'"))
                right=right.substring(1,right.length()-1);
            operator = Utils.LIKE();
            stack.push("(" + left + operator + "\'"+right + "\')");
        } else{
            throw new UnsupportedOperationException("ExprFunctionN not supported yet!");
        }
    }

    //  对NodeValue常量访问:需要判断常量的类型(整型/字符串)
    @Override
    public void visit(NodeValue nv) {
	    if(nv.isIRI()){
            stack.push("\'"+nv.toString()+"\'");
        }
	    else if(nv.isLiteral()&&nv.getDatatypeURI().contains("string")){
            stack.push("\'\\\""+nv.toString().substring(1,nv.toString().length()-1)+"\\\"\'");
        } else
	        stack.push(nv.toString());
	}

    // 对ExprVar变量访问:   将变量入栈即可
    @Override
    public void visit(ExprVar nv) { stack.push("?"+nv.getVarName()); variables.add("_"+nv.getVarName());}

    @Override
    public void visit(ExprFunctionOp funcOp) { throw new UnsupportedOperationException("ExprFunctionOp not supported yet."); }

    @Override
    public void visit(ExprAggregator eAgg) { throw new UnsupportedOperationException("ExprAggregator not supported yet."); }

    @Override
    public void visit(ExprNone exprNone) { }
}
