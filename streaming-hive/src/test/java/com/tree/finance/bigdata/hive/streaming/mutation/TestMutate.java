package com.tree.finance.bigdata.hive.streaming.mutation;

import com.tree.finance.bigdata.hive.streaming.constants.TestConstants;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.AvroObjectInspector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.streaming.mutate.client.*;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 14:23
 */
public class TestMutate {

    private Configuration conf = new Configuration();

    Table table = new Table("acid_test", "test_acid",
            null, 0, 0, 0, null, null, null, null, null, null);

    private String metaStoreUri = "thrift://cdh2:9084";

    private Schema schema;

    @Before
    public void setUp() {
    }

    @Test
    public void testInsert() throws Exception {

        schema = new Schema.Parser().parse(TestConstants.schemaStr);

        MutatorFactory factory = new AvroMutationFactory(conf, new AvroObjectInspector(null, null, schema));
        MutatorClient client = new MutatorClientBuilder()
                .addSinkTable(table.getDbName(), table.getTableName(), true)
                .metaStoreUri(metaStoreUri)
                .build();
        client.connect();
        List<AcidTable> destinations = client.getTables();
        Transaction mutateTransaction = client.newTransaction();

        try {
            System.out.println("begin transaction");

            mutateTransaction.begin();

            MutatorCoordinator mutateCoordinator = new MutatorCoordinatorBuilder()
                    .metaStoreUri(metaStoreUri)
                    .table(destinations.get(0))
                    .mutatorFactory(factory)
                    .build();

            for (long i = 10; i < 20; i++) {
                mutateCoordinator.insert(Arrays.asList("1"), buildInsertRecord(i, "insert " + i, i));
            }
            mutateCoordinator.close();
            mutateTransaction.commit();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
            mutateTransaction.abort();
        } finally {
            client.close();
        }

    }

    @Test
    public void testUpdate() throws Exception {

        System.out.println(TestConstants.updateSchemaStr);

        schema = new Schema.Parser().parse(TestConstants.updateSchemaStr);

        MutatorFactory factory = new AvroMutationFactory(conf, new AvroObjectInspector(null, null, schema));
        MutatorClient client = new MutatorClientBuilder()
                .addSinkTable(table.getDbName(), table.getTableName(), true)
                .metaStoreUri(metaStoreUri)
                .build();
        client.connect();
        List<AcidTable> destinations = client.getTables();
        Transaction mutateTransaction = client.newTransaction();

        try {
            System.out.println("begin transaction");

            mutateTransaction.begin();

            MutatorCoordinator mutateCoordinator = new MutatorCoordinatorBuilder()
                    .metaStoreUri(metaStoreUri)
                    .table(destinations.get(0))
                    .mutatorFactory(factory)
                    .build();

            mutateCoordinator.update(Arrays.asList("1"), buildUpdateRecord(10, "update 10", 0, 206));

            mutateCoordinator.close();
            mutateTransaction.commit();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
            mutateTransaction.abort();
        } finally {
            client.close();
        }
    }

    private GenericData.Record buildUpdateRecord(long id, String msg, long rowId, long originalTxnField) {



        GenericRecordBuilder afterBuilder = new GenericRecordBuilder(schema.getField("after").schema());
        afterBuilder.set("id", id);
        afterBuilder.set("msg", msg);

        //id value
        GenericRecordBuilder keyBuilder = new GenericRecordBuilder(schema.getField("key").schema());
        keyBuilder.set("Id", rowId);
        keyBuilder.set("__dbz__physicalTableIdentifier", "acid_test.test_acid");

        keyBuilder.set("originalTxnField", originalTxnField);
        keyBuilder.set("bucketField", 0);
        keyBuilder.set("rowIdField", rowId);

        //
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("after", afterBuilder.build());
        builder.set("key", keyBuilder.build());
        builder.set("op", "insert");
        return builder.build();
    }

    private GenericData.Record buildInsertRecord(long id, String msg, long rowId) {

        schema = new Schema.Parser().parse(TestConstants.updateSchemaStr);

        GenericRecordBuilder afterBuilder = new GenericRecordBuilder(schema.getField("after").schema());
        afterBuilder.set("id", id);
        afterBuilder.set("msg", msg);

        //id value
        GenericRecordBuilder keyBuilder = new GenericRecordBuilder(schema.getField("key").schema());
        keyBuilder.set("Id", rowId);
        keyBuilder.set("__dbz__physicalTableIdentifier", "acid_test.test_acid");

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("after", afterBuilder.build());
        builder.set("key", keyBuilder.build());
        builder.set("op", "insert");
        return builder.build();
    }

}
