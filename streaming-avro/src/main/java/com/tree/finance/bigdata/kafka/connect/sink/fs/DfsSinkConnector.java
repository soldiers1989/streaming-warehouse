package com.tree.finance.bigdata.kafka.connect.sink.fs;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ZhengShengJun
 * Description
 * Created in 2018/6/29 10:03
 */
public class DfsSinkConnector extends SinkConnector {

    private static final Logger LOG = LoggerFactory.getLogger(DfsSinkConnector.class);

    private Map<String, String> props;

    public String version() {
        return "streaming-warehouse-2.0";
    }

    public void start(Map<String, String> props) {
        this.props = props;
    }

    public Class<? extends Task> taskClass() {
        return DfsSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOG.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            Map<String, String> clone = copyConfig(props);
            clone.put(SinkConfig.KEY.SINK_TASK_ID, Integer.toString(i));
            configs.add(clone);
        }
        return configs;
    }

    public void stop() {

    }

    public ConfigDef config() {
        return SinkConfig.CONFIG_DEF;
    }

    private Map<String, String> copyConfig(Map<String, String> map){
        Map<String, String> result = new HashMap<>();
        map.forEach(result::put);
        return result;
    }
}
