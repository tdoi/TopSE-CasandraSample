package jp.topse.nosql.cassandra;

import java.util.Iterator;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Hello world!
 *
 */
public class Sample {
    public static String CLUSTER_NAME = "Test Cluster";
    public static String DB_SERVER = "157.1.206.2";
    public static String KEYSPACE_NAME = "TopSEKeyspace";
    public static String COLUMN_FAMILY_NAME = "AccessLog";

    public static String LOG_FILE = "./src/main/resources/access.log";

    private Cluster cluster;
    private KeyspaceDefinition keyspaceDef;
    private Keyspace keyspace;
    private ColumnFamilyTemplate<Integer, String> template;

    public static void main(String[] args) {
        Sample app = new Sample();

        app.dropKeyspace();
        app.prepare();

        app.insertSample();
        app.findAllSample1();
        app.updateSample();
        app.findAllSample2();
        app.deleteSample();
        app.findAllSample3();
        app.findAllKeys();
    }
    
    public Sample() {
        cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, DB_SERVER + ":9160");
    }

    private void prepare() {
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

    private void dropKeyspace() {
        cluster.dropKeyspace(KEYSPACE_NAME);
    }

    private void insertSample() {
        System.out.println("----- insertSample -----");
        for (int i = 0; i < 10; ++i) {
            ColumnFamilyUpdater<Integer, String> updater = template.createUpdater(i);
            updater.setString("c1", "" + i);
            updater.setString("c2", "" + i * 2);
            updater.setString("c3", "z");
            updater.setString("v", "abc");
            template.update(updater);
            System.out.println("Inserted : " + i);
        }
    }

    private void updateSample() {
        System.out.println("----- updateSample -----");
        for (int i = 0; i < 5; ++i) {
            int id = (int) (Math.random() * 10);
            ColumnFamilyUpdater<Integer, String> updater = template.createUpdater(id);
            updater.setString("c3", "" + id * id * id);
            updater.setString("v", "abc" + id);
            template.update(updater);
            System.out.println("Updated : " + id);
        }
    }

    private void deleteSample() {
        System.out.println("----- deleteSample -----");
        for (int i = 0; i < 3; ++i) {
            int id = (int) (Math.random() * 10);
            template.deleteColumn(id, "c3");
            System.out.println("Deleted : " + id + ".c3");
        }
        int id = (int) (Math.random() * 10);
        template.deleteRow(id);
        System.out.println("Deleted : " + id);
    }

    private void findAllSample1() {
        System.out.println("----- findAllSample1 -----");
        for (int i = 0; i < 10; ++i) {
            int id = i;
            ColumnFamilyResult<Integer, String> res = template.queryColumns(id);
            String value = "" + res.getKey() + " : " + "c1=" + res.getString("c1") + " " + "c3=" + res.getString("c3") + " " + "v=" + res.getString("v");
            System.out.println(value);
        }
    }

    private void findAllSample2() {
        System.out.println("----- findAllSample2 -----");
        SliceQuery<Integer, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(), StringSerializer.get(), StringSerializer.get());
        sliceQuery.setColumnFamily(COLUMN_FAMILY_NAME);
        for (int i = 0; i < 10; ++i) {
            int id = i;
            sliceQuery.setKey(id);
            System.out.print(id + " : ");
            ColumnSliceIterator<Integer, String, String> iterator = new ColumnSliceIterator<Integer, String, String>(sliceQuery, null, "", false);
            while (iterator.hasNext()) {
                HColumn<String, String> column = iterator.next();
                String name = column.getName();
                Object value = column.getValue();
                System.out.print(name + "=" + value.toString() + " ");
            }
            System.out.println();
        }
    }

    private void findAllSample3() {
        System.out.println("----- findAllSample3 -----");
        SliceQuery<Integer, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(), StringSerializer.get(), StringSerializer.get());
        sliceQuery.setColumnFamily(COLUMN_FAMILY_NAME);
        for (int i = 0; i < 10; ++i) {
            int id = i;
            if (template.isColumnsExist(id)) {
                sliceQuery.setKey(id);
                System.out.print("" + id + " : ");
                ColumnSliceIterator<Integer, String, String> iterator = new ColumnSliceIterator<Integer, String, String>(sliceQuery, "c2", "d", false);
                while (iterator.hasNext()) {
                    HColumn<String, String> column = iterator.next();
                    String name = column.getName();
                    Object value = column.getValue();
                    System.out.print(name + "=" + value + " ");
                }
                System.out.println();
            } else {
                System.out.println("" + id + " : NOT FOUND");
            }

        }
    }

    private void findAllKeys() {
        System.out.println("----- findAllKeys -----");
        @SuppressWarnings("deprecation")
        KeyIterator<Integer> keyIterator = new KeyIterator<Integer>(keyspace, COLUMN_FAMILY_NAME, IntegerSerializer.get());
        Iterator<Integer> itr = keyIterator.iterator();
        while (itr.hasNext()) {
            Integer key = itr.next();
            System.out.println(key);
        }
    }
}