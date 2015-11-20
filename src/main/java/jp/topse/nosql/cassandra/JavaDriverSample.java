package jp.topse.nosql.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Hello world!
 *
 */
public class JavaDriverSample {
    public static String CLUSTER_NAME = "Test Cluster";
    public static String DB_SERVER = "localhost";
    public static String KEYSPACE_NAME = "topsexx";
    public static String COLUMN_FAMILY_NAME = "logs";


    public static void main(String[] args) {
    	Cluster cluster = Cluster.builder().addContactPoint(DB_SERVER).build();
 
    	Session session = cluster.newSession();
    	session.execute("use " + KEYSPACE_NAME);
    	ResultSet resultSet = session.execute("SELECT * FROM " + COLUMN_FAMILY_NAME);
    	for (Row row : resultSet.all()) {
    		System.out.println(row);
    	}
    	
    	session.execute("INSERT INTO logs (id, target, referer) VALUES (?, ?, ?)", 3, "target3", "referer3");
    	session.execute("INSERT INTO logs (id, target, referer) VALUES (?, ?, ?)", 4, "target4", "referer4");
    	
    	cluster.close();
   }
    
}
    
