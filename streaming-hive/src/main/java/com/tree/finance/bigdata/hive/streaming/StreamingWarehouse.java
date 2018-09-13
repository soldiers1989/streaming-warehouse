package com.tree.finance.bigdata.hive.streaming;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.service.TaskGenerator;
import com.tree.finance.bigdata.hive.streaming.service.TaskDispatcher;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricServer;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 11:02
 */
public class StreamingWarehouse {

    private static Logger LOG = LoggerFactory.getLogger(StreamingWarehouse.class);

    private TaskGenerator taskGenerator;

    private TaskDispatcher processor;

    private MetricServer metricServer;

    private AppConfig config;

    public StreamingWarehouse(AppConfig config) {
        this.processor = new TaskDispatcher(config);
        this.taskGenerator = new TaskGenerator(config, processor);
        this.config = config;
        this.metricServer = new MetricServer(config.getPrometheusServerPort());
    }

    public void init() throws Exception{
        this.processor.init();
        this.taskGenerator.init();
        this.metricServer.init();
    }

    public void stop() throws InterruptedException {
        LOG.info("start to stop program");
        this.taskGenerator.stop();
        this.processor.stop();
        this.metricServer.stop();
        RabbitMqUtils.getInstance(config.getRabbitHost(), config.getRabbitPort()).close();
        LOG.info("program stopped");
    }

    public static void main(String[] args) throws Exception{

        AppConfig conf = ConfigHolder.getConfig();

        StreamingWarehouse warehouse = new StreamingWarehouse(conf);
        warehouse.init();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOG.info("call shutdown hook ...");
                warehouse.stop();
            } catch (Exception e) {
                LOG.error("", e);
            }
            LOG.info("stopped service");
        }, "shutdown hook"));

    }


}
