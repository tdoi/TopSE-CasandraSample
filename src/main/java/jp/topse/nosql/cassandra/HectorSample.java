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
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Hello world!
 *
 */
public class HectorSample {
    public static String CLUSTER_NAME = "Test Cluster";
    public static String DB_SERVER = "localhost";
    public static String KEYSPACE_NAME = "topsexx";
    public static String COLUMN_FAMILY_NAME = "demo";

    private Cluster cluster;
    private KeyspaceDefinition keyspaceDef;
    private Keyspace keyspace;
    private ColumnFamilyTemplate<Integer, String> template;

    public static void main(String[] args) {
        HectorSample app = new HectorSample();

  //      app.dropKeyspace();
        app.prepare();
    
        app.insertSample();
        app.findAllSample1();
        app.updateSample();
        app.findAllSample2();
        app.deleteSample();
        app.findAllSample3();
        app.findAllSample4();
        app.findAllKeys();        
    }
    
    public HectorSample() {
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
    	try {
    		cluster.dropKeyspace(KEYSPACE_NAME);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
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
            for (String name : res.getColumnNames()) {
            	System.out.println(name + ": " + res.getString(name));
            }
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
    
    private void findAllSample4() {
        System.out.println("----- findAllSample4 -----");
        int rowCount = 100;
        int colCount = 5;
        RangeSlicesQuery<Integer, String, String> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, IntegerSerializer.get(), StringSerializer.get(), StringSerializer.get())
                        .setColumnFamily(COLUMN_FAMILY_NAME)
                        .setRange(null, null, false, colCount)
                        .setRowCount(rowCount);
        Integer lastKey = null;
        while (true) {
            rangeSlicesQuery.setKeys(lastKey, null);
            QueryResult<OrderedRows<Integer, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<Integer, String, String> rows = result.get();
            Iterator<Row<Integer, String, String>> rowsIterator = rows.iterator();
            if (lastKey != null && rowsIterator != null) {
                rowsIterator.next();
            }
            while (rowsIterator.hasNext()) {
                Row<Integer, String, String> row = rowsIterator.next();
                System.out.println(row.getKey());
                for (HColumn<String, String> column : row.getColumnSlice().getColumns()) {
                	System.out.println(column.getName() + "=" + column.getValue());
                }
            }
            if (rows.getCount() < rowCount) {
                break;
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