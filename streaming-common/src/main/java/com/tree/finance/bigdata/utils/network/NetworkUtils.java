package com.tree.finance.bigdata.utils.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/26 19:29
 */
public class NetworkUtils {

    static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class);

    public static final String localIp = getHostName();

    private static String getHostName() {
        String result = "";
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            result = localHost.getHostAddress();
        } catch (UnknownHostException e) {
            LOG.error("failed to get host ip", e);
        }
        return result;
    }
}
