import antlr.DslLexer;
import antlr.DslParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

public class Main {
    public static void main(String[] args) {
        String sql = "Select 'abc' as a, `hahah` as c  From a aS table;";
        //将输入转成antlr的input流
        ANTLRInputStream input = new ANTLRInputStream(sql);
        //词法分析
        DslLexer lexer = new DslLexer(input);
        //转成token流
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        // 语法分析
        DslParser parser = new DslParser(tokens);
        //获取某一个规则树，这里获取的是最外层的规则，也可以通过sql()获取sql规则树......
        DslParser.StaContext tree = parser.sta();
        //打印规则数
        System.out.println(tree.toStringTree(parser));
    }
}
