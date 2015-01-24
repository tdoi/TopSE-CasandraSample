package jp.topse.nosql.cassandra;

import java.util.List;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
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
    // public static String DB_SERVER = "157.1.206.2";
    public static String KEYSPACE_NAME = "TopSEKeyspace";
    public static String COLUMN_FAMILY_NAME = "TopSEColumnFamily";
    // public static String COLUMN_FAMILY_NAME = "KEYSPACE_NAME";

    public static String LOG_FILE = "./src/main/resources/access.log";

    private Cluster cluster;
    private KeyspaceDefinition keyspaceDef;
    private Keyspace keyspace;
    private ColumnFamilyTemplate<Integer, String> template;

    public static void main(String[] args) {
        Excersize app = new Excersize();

        app.dropKeyspace();
        app.prepare();

        AccessLogLoader loader = new AccessLogLoader();
        List<Access> accesses = loader.load(LOG_FILE);
        app.insertAccesses(accesses);
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

        template = new ThriftColumnFamilyTemplate<Integer, String>(keyspace, COLUMN_FAMILY_NAME, IntegerSerializer.get(), StringSerializer.get());
    }

    private void insertAccesses(List<Access> accesses) {
        for (int i = 0; i < accesses.size(); ++i) {
            // DO SOMETHING HERE...
        }
    }
    
    private void dropKeyspace() {
        cluster.dropKeyspace(KEYSPACE_NAME);
    }

}