package com.tree.finace.bigdata.metrics;

import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/25 16:02
 */
public class MetricFactoryTest {
    @Test
    public void testMetrics() throws Exception {
        HTTPServer server = new HTTPServer(8069);

        Summary requestLatency = Summary.build()
                .name("aaa_latency_seconds").help("Request latency in seconds.").register();
        while (true) {
            Summary.Timer timer = requestLatency.startTimer();
            Thread.sleep(2000);
            timer.observeDuration();
        }

        /*Counter requests = Counter.build().name("bbb_counter")
                .help("Total coutners ...").register();
        while (true) {
            Thread.sleep(1000);
            requests.inc();
        }*/

//        Summary summary = Summary.build().name("a_summary_test").
//                help("request summary"). register();
//        Random random = new Random();
//        while (true) {
//            Thread.sleep(1000);
//            int n = random.nextInt(10);
//            System.out.println(n);
//            summary.observe(n);
//        }

    }
}
