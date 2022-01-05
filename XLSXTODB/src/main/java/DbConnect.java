import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DbConnect {

    public static Connection con = null;
    public void ConnectDb() {

        try {
            /* Step 01 :Loading Drivers */
            Class.forName(HiveConstants.DRIVER_NAME);
            System.out.println("Driver Loaded successfully");

            // Step 02 : Obtaining Connection Object
            con = DriverManager.getConnection(
                    HiveConstants.CONNECTION_URL,
                    HiveConstants.USERNAME,
                    HiveConstants.PASSWORD);
            System.out.println("Obtained Connection Object");

            // Step 03 : Obtaining Statement object
            // Obtaining the Statement Object
            Statement stmt = con.createStatement();

//            // Step 04 : Executing the query
//            stmt.execute(SqlStmt);
//            System.out.println("DB stmt run successfully");

        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found");
        } catch (SQLException e) {
            System.out.println("Problem Connecting DB");
            e.printStackTrace();
        }
//        finally {
//            try {
//                con.close();
//            } catch (SQLException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
    }
}
