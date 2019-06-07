import org.apache.hive.jdbc.HiveDriver;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.Utils;

public class HiveJDBCDTExample {
        public static void main(String[] args) throws SQLException, Exception{
            String proxyUser = args[1];
            String serverPrincipal = args[2];
            String url=args[0];
                try {
                        Class.forName("org.apache.hive.jdbc.HiveDriver");
                } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        System.exit(1);
                }
                url = args[0]+";principal="+serverPrincipal;
                Connection cnct = DriverManager.getConnection(url);
                Statement stmt = cnct.createStatement();
                System.out.println("Kerberos Auth Connected successfully to " + url);
                
                String token = ((HiveConnection)cnct).getDelegationToken(proxyUser, serverPrincipal);
                cnct.close();
                
                System.out.println("Sleeping 30 Seconds, remove TGT\n You may run kdestroy by cloning the terminal session.");
                Thread.sleep(30000);
                
                /*
                 * Connect using the delegation token passed via configuration object
                 */
                System.out.println("Store token into ugi and try");
                storeTokenInJobConf(token);
                url = args[0]+";auth=delegationToken";
                Connection cnctdt = DriverManager.getConnection(url);
                Statement stmt1 = cnctdt.createStatement();
                System.out.println("Delegation token Connecting to " + url);
                
                String sql = "show tables";
                System.out.println("Running: " + sql);
                ResultSet res1 = stmt1.executeQuery(sql);
                while (res1.next()) {
                        System.out.println(res1.getString(1));
                }
                cnctdt.close();
        }

          private static void storeTokenInJobConf(String tokenStr) throws Exception {
            Utils.setTokenStr(Utils.getUGI(),tokenStr, HiveAuthFactory.HS2_CLIENT_TOKEN);
            System.out.println("Stored token " + tokenStr);
          }

}

