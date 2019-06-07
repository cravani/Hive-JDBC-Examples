import java.sql. * ;
public class HiveKRBJDBC {
	public static void main(String[] args) throws Exception {
		Class.forName("com.simba.hive.jdbc4.HS2Driver");
		System.setProperty("java.security.auth.login.config", args[1]);
		Connection con = DriverManager.getConnection(args[0]);
		System.out.println("Connecting to Hive Server2 with URL: “ + args[0]);
            Statement stmt = con.createStatement(); String tableName = "
		testHiveDriverTable "; System.out.println("\nExecuting statement: drop table
		if exists " + tableName); stmt.execute("
		drop table
		if exists " + tableName); System.out.println("\nExecuting statement: create table " + tableName + " (key int, value string)"); stmt.execute("
		create table " + tableName + " (key int, value string)");
            // show tables
            String sql = "
		show tables '" + tableName + "'"; System.out.println("\nRunning: " + sql); ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
                System.out.println(res.getString(1));
            }
            // insert data into table
            sql = "
		insert into " + tableName + "
		values(1, 'Name1'), (2, 'Name2')"; System.out.println("\nRunning: " + sql); stmt.execute(sql);
            // select * query
            sql = "
		select * from " + tableName; System.out.println("\nRunning: " + sql + "\n "); System.out.println("
		key " + "\t " + "
		value "); res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t " + res.getString(2));
            }
            System.out.println("\n Closing connection...\n "); con.close();
        }
    }
