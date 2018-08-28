package com.tree.finance.bigdata.hive.streaming.constants;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 15:04
 */
public interface TestConstants {
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
            "        \"namespace\": \"test_acid.acid_test\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"id\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"msg\",\n" +
            "            \"type\": [\n" +
            "              \"null\",\n" +
            "              \"string\"\n" +
            "            ],\n" +
            "            \"default\": null\n" +
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

    String updateSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"test\",\n" +
            "  \"namespace\": \"test\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"after\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"Value\",\n" +
            "        \"namespace\": \"test_acid.acid_test\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"id\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"msg\",\n" +
            "            \"type\": [\n" +
            "              \"null\",\n" +
            "              \"string\"\n" +
            "            ],\n" +
            "            \"default\": null\n" +
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
            "          },\n" +




            "          {\n" +
            "            \"name\": \"originalTxnField\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"bucketField\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"rowIdField\",\n" +
            "            \"type\": \"long\"\n" +
            "          }\n" +






            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
}
