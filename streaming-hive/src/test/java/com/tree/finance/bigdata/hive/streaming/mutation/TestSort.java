package com.tree.finance.bigdata.hive.streaming.mutation;

import org.apache.avro.Schema;
import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/21 14:42
 */
public class TestSort {

    @Test
    public void test(){
    }

    @Test
    public void testSchema(){
        Schema schema = new Schema.Parser().parse(schemaStr);
        Schema dayKey = schema.getField("after").schema().getField("day_key").schema();
        System.out.println(dayKey.getName());
    }

    String schemaStr = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"atf_data_queryresult_common\",\n" +
            "  \"namespace\" : \"antifraud\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"after\",\n" +
            "    \"type\" : {\n" +
            "      \"type\" : \"record\",\n" +
            "      \"name\" : \"Value\",\n" +
            "      \"namespace\" : \"antifraud.atf_data_queryresult_common\",\n" +
            "      \"fields\" : [ {\n" +
            "        \"name\" : \"id\",\n" +
            "        \"type\" : \"long\"\n" +
            "      }, {\n" +
            "        \"name\" : \"day_key\",\n" +
            "        \"type\" : [ \"null\", {\n" +
            "          \"type\" : \"int\",\n" +
            "          \"logicalType\" : \"date\"\n" +
            "        } ],\n" +
            "        \"default\" : null\n" +
            "      }, {\n" +
            "        \"name\" : \"hour\",\n" +
            "        \"type\" : [ \"null\", \"string\" ],\n" +
            "        \"default\" : null\n" +
            "      }, {\n" +
            "        \"name\" : \"data\",\n" +
            "        \"type\" : [ \"null\", \"string\" ],\n" +
            "        \"default\" : null\n" +
            "      }, {\n" +
            "        \"name\" : \"data_type\",\n" +
            "        \"type\" : [ \"null\", \"string\" ],\n" +
            "        \"default\" : null\n" +
            "      }, {\n" +
            "        \"name\" : \"mingzhong_words\",\n" +
            "        \"type\" : [ \"null\", \"string\" ],\n" +
            "        \"default\" : null\n" +
            "      }, {\n" +
            "        \"name\" : \"description\",\n" +
            "        \"type\" : [ \"null\", \"string\" ],\n" +
            "        \"default\" : null\n" +
            "      }, {\n" +
            "        \"name\" : \"result_type\",\n" +
            "        \"type\" : [ \"null\", \"int\" ],\n" +
            "        \"default\" : null\n" +
            "      }, {\n" +
            "        \"name\" : \"create_date\",\n" +
            "        \"type\" : {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"logical_type\" : \"io.debezium.time.ZonedTimestamp\"\n" +
            "        }\n" +
            "      }, {\n" +
            "        \"name\" : \"last_update_date\",\n" +
            "        \"type\" : [ \"null\", {\n" +
            "          \"type\" : \"string\",\n" +
            "          \"logical_type\" : \"io.debezium.time.ZonedTimestamp\"\n" +
            "        } ],\n" +
            "        \"default\" : null\n" +
            "      } ]\n" +
            "    }\n" +
            "  }, {\n" +
            "    \"name\" : \"op\",\n" +
            "    \"type\" : \"string\"\n" +
            "  }, {\n" +
            "    \"name\" : \"key\",\n" +
            "    \"type\" : {\n" +
            "      \"type\" : \"record\",\n" +
            "      \"name\" : \"Key\",\n" +
            "      \"namespace\" : \"antifraud.atf_data_queryresult_common\",\n" +
            "      \"fields\" : [ {\n" +
            "        \"name\" : \"id\",\n" +
            "        \"type\" : \"long\"\n" +
            "      }, {\n" +
            "        \"name\" : \"__dbz__physicaltableidentifier\",\n" +
            "        \"type\" : \"string\"\n" +
            "      } ]\n" +
            "    }\n" +
            "  } ]\n" +
            "}";
}
