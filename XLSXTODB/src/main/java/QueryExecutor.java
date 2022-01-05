import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutor {

    public void ExecuteQuery(String query) {
        Statement stmt = null;
        try {
            //System.out.println(query);
            stmt = DbConnect.con.createStatement();
            stmt.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
