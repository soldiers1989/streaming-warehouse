package com.tree.finance.bigdata.hive.streaming;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.lock.LockManager;
import com.tree.finance.bigdata.hive.streaming.service.TaskDispatcher;
import com.tree.finance.bigdata.hive.streaming.service.TaskGenerator;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricServer;
import com.tree.finance.bigdata.service.Service;
import com.tree.finance.bigdata.service.ShutDownSocketListener;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 11:02
 */
public class StreamingWarehouse implements Service {

    private static Logger LOG = LoggerFactory.getLogger(StreamingWarehouse.class);

    private TaskGenerator taskGenerator;

    private TaskDispatcher taskDispatcher;

    private MetricServer metricServer;

    private AppConfig config;

    private ShutDownSocketListener listener;

    private static AtomicBoolean stopping = new AtomicBoolean(false);

    public StreamingWarehouse(AppConfig config) {
        this.taskDispatcher = new TaskDispatcher(config);
        this.taskGenerator = new TaskGenerator(config, taskDispatcher);
        this.config = config;
        this.metricServer = new MetricServer(config.getPrometheusServerPort());
        this.listener = new ShutDownSocketListener(this, config.getShutDownSocketPort());
    }

    public void init() throws Exception {
        this.listener.init();
        this.taskDispatcher.init();
        this.taskGenerator.init();
        this.metricServer.init();
    }

    @Override
    public void stop() throws InterruptedException {
        if (stopping.compareAndSet(false, true)) {
            LOG.info("start to stop program");
            this.taskGenerator.stop();
            this.taskDispatcher.stop();
            this.metricServer.stop();
            RabbitMqUtils.getInstance(config.getRabbitHost(), config.getRabbitPort()).close();
            LockManager.getSingeleton().close();
            LOG.info("program stopped");
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            AppConfig conf = ConfigHolder.getConfig();

            StreamingWarehouse warehouse = new StreamingWarehouse(conf);
            warehouse.init();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    warehouse.stop();
                } catch (Exception e) {
                    LOG.error("", e);
                }
                LOG.info("stopped service");
            }, "shutdown hook"));
        }catch (Exception e) {
            LOG.error("failed to start program");
            System.exit(1);
        }
    }

}
