package com.tree.finance.bigdata.hive.streaming.mutation;

import com.tree.finance.bigdata.hive.streaming.mutation.inspector.AvroObjectInspector;
import org.apache.avro.Schema;
import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/3 17:09
 */
public class TestAvroObjectInspector {
    @Test
    public void testAvroObjectInspector(){
        Schema schema = new Schema.Parser().parse(schemaStr);
        System.out.println(schema);
        new AvroObjectInspector(null, null, schema);
    }
    String schemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"test\",\n" +
            "  \"namespace\": \"test\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"after\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"Value\",\n" +
            "        \"namespace\": \"rules_engine.t_rule_flow_instance_parameter\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"Id\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"BatchId\",\n" +
            "            \"type\": [\n" +
            "              \"null\",\n" +
            "              \"long\"\n" +
            "            ],\n" +
            "            \"default\": null\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"RuleFlowInstanceId\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"ParameterName\",\n" +
            "            \"type\": [\n" +
            "              \"null\",\n" +
            "              \"string\"\n" +
            "            ],\n" +
            "            \"default\": null\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"ParameterValue\",\n" +
            "            \"type\": [\n" +
            "              \"null\",\n" +
            "              \"string\"\n" +
            "            ],\n" +
            "            \"default\": null\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"ParameterType\",\n" +
            "            \"type\": \"string\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"CreatedDatetime\",\n" +
            "            \"type\": \"string\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"LastUpdatedDatetime\",\n" +
            "            \"type\": \"string\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"op\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"key\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"Key\",\n" +
            "        \"namespace\": \"rules_engine.t_rule_flow_instance_parameter\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"Id\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"__dbz__physicalTableIdentifier\",\n" +
            "            \"type\": \"string\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
}
