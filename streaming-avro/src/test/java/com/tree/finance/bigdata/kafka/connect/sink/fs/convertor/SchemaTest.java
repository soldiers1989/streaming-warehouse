package com.tree.finance.bigdata.kafka.connect.sink.fs.convertor;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/16 16:51
 */
public class SchemaTest {
    String subject = "basisdata.t_credit_education-value";
    String baseUrl = "http://application1:8081,http://application2:8081,http://application3:8081,http://application4:8081,http://application5:8081";

    @Test
    public void test(String[] args) throws Exception{
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(baseUrl, 30);
        System.out.println(client.getAllSubjects());

        SchemaMetadata schema = client.getLatestSchemaMetadata(subject);

//        SchemaConvertor.fromConnectSchema()
    }
}
