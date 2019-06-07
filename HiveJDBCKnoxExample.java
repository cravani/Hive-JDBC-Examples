
import org.apache.hive.jdbc.HiveDriver;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
public class HiveJDBCKnoxExample {
        public static void main(String[] args) throws SQLException{
                try {
                        Class.forName("org.apache.hive.jdbc.HiveDriver");
                } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        System.exit(1);
                }
                Connection cnct = DriverManager.getConnection(args[0],args[1],args[2]);
                Statement stmt = cnct.createStatement();
                String sql = "show tables";
                System.out.println("Running: " + sql);
                ResultSet res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.println(res.getString(1));
                }
        }
}

