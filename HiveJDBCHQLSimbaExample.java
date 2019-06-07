import java.util.*;
import java.io.*;
import com.simba.hive.jdbc4.HS2Driver;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
public class HiveJDBCHQLSimbaExample {
        public static void main(String[] args) throws Exception{
		System.out.println("Calling classname");
                try {
                        Class.forName("com.simba.hive.jdbc4.HS2Driver");
                } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        System.exit(1);
                }
		System.out.println("Attempting to connect");
                Connection cnct = DriverManager.getConnection(args[0]);
                InputStream inputstream = new FileInputStream(args[1]);
		System.out.println("Connected.");
                importSQL(cnct,inputstream);
                /*Statement stmt = cnct.createStatement();
                String sql = "show tables";
                System.out.println("Running: " + sql);
                ResultSet res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.println(res.getString(1));
                }*/
        }
	public static void importSQL(Connection conn, InputStream in) throws SQLException	{
	    Scanner s = new Scanner(in);
	    s.useDelimiter("(;(\r)?\n)|((\r)?\n)?(--)?.*(--(\r)?\n)");
	    Statement st = null;
	    try
	    {
	        st = conn.createStatement();
	        while (s.hasNext())
	        {
	            String line = s.next();
	            if (line.startsWith("/*!") && line.endsWith("*/"))
	            {
	                int i = line.indexOf(' ');
	                line = line.substring(i + 1, line.length() - " */".length());
	            }

	            if (line.trim().length() > 0)
	            {
			System.out.println("Executing SQL: "+line);
	                st.execute(line);
	            }
	        }
	    }
	    finally
	    {
	        if (st != null) st.close();
	    }
	}}
