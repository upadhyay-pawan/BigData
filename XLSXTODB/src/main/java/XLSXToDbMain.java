import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class XLSXToDbMain {

    public static void main(String[] args) {

        XLSXParser<XLSXTemplate> postsXLSXParser = new XLSXParser<>();

        InputStream xlsxFile = postsXLSXParser.getClass()
                .getClassLoader()
                .getResourceAsStream("TableTemplate.xlsx");
        try {
            DbConnect dbConnect = new DbConnect();
            dbConnect.ConnectDb();
            List<XLSXTemplate> list = postsXLSXParser.parse(xlsxFile, XLSXTemplate.class);
            //System.out.println(list);
            QueryBuilder queryBuilder = new QueryBuilder();
            List <String> queries = queryBuilder.BuildQuery(list);
            QueryExecutor queryExecutor = new QueryExecutor();
            for (String query: queries) {
                queryExecutor.ExecuteQuery(query);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                DbConnect.con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}