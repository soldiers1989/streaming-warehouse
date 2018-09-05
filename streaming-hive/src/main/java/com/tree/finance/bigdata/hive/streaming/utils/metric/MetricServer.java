package com.tree.finance.bigdata.hive.streaming.utils.metric;

import com.tree.finance.bigdata.hive.streaming.service.Service;
import io.prometheus.client.exporter.HTTPServer;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/26 15:36
 */
public class MetricServer implements Service {

    private HTTPServer server;

    private Integer port;

    public MetricServer(Integer port) {
        this.port = port;
    }

    @Override
    public void stop() {
        this.server.stop();
    }

    @Override
    public void init() throws Exception{
        server = new HTTPServer(port);
    }
}
