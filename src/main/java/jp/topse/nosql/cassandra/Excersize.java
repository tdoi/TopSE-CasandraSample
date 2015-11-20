package jp.topse.nosql.cassandra;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Hello world!
 *
 */
public class Excersize {
    public static String CLUSTER_NAME = "Test Cluster";
    public static String DB_SERVER = "localhost";
    public static String KEYSPACE_NAME = "topsexx";
    public static String COLUMN_FAMILY_NAME = "access";

    public static String ACCESS_LOG = "./src/main/resources/access.log";

    private Cluster cluster;
    private KeyspaceDefinition keyspaceDef;
    private Keyspace keyspace;
    private ColumnFamilyTemplate<String, String> template;

    public static void main(String[] args) {
        Excersize app = new Excersize();

        app.dropKeyspace();
        app.prepare();

        app.insert();
    }
    
    public Excersize() {
        cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, DB_SERVER + ":9160");
    }

    private void prepare() {
        cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, DB_SERVER + ":9160");

        keyspaceDef = cluster.describeKeyspace(KEYSPACE_NAME);
        if (keyspaceDef == null) {
            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(KEYSPACE_NAME, ThriftKsDef.DEF_STRATEGY_CLASS, 1, null);
            cluster.addKeyspace(newKeyspace, true);

            ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE_NAME, COLUMN_FAMILY_NAME, ComparatorType.BYTESTYPE);
            cluster.addColumnFamily(cfDef);
        }
        keyspace = HFactory.createKeyspace(KEYSPACE_NAME, cluster);

        template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUMN_FAMILY_NAME, StringSerializer.get(), StringSerializer.get());
    }

    private void insert() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(ACCESS_LOG))));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                String[] logItems = line.split(",");
                String target = logItems[0];
                Map<String, String> params = new HashMap<String, String>();
                String referer = logItems.length > 1 ? logItems[1] : null;
                String[] targetItems = target.split("\\?");
                if (targetItems.length == 2) {
                	target = targetItems[0];
                	String[] paramItems = targetItems[1].split("&");
                	for (int i = 0; i < paramItems.length; ++i) {
                		String[] items = paramItems[i].split("=");
                		params.put(items[0],  items[1]);
                	}
                }
                
                // Please Implement

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private void dropKeyspace() {
    	try {
    		cluster.dropKeyspace(KEYSPACE_NAME);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }

}