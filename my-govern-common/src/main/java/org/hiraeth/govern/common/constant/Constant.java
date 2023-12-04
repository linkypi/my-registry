package org.hiraeth.govern.common.constant;

/**
 * @author: leo
 * @description:
 * @ClassName: org.hiraeth.govern.common.constant
 * @date: 2023/11/27 12:30
 */
public class Constant {

    public static final int SLOTS_COUNT = 16384;

    public static final String IS_CONTROLLER_CANDIDATE = "is.controller.candidate";
    public static final String CONTROLLER_CANDIDATE_SERVERS = "controller.candidate.servers";

    public static final String DATA_DIR = "data.dir";
    public static final String LOG_DIR = "log.dir";
    public static final String CLUSTER_NODE_COUNT = "cluster.node.count";

    // 普通股master配置参数
    public static final String NODE_IP = "node.ip";
    // 普通股master内部通信端口
    public static final String NODE_INTERNAL_PORT = "node.internal.port";
    // 普通股master与客户端通信的端口
    public static final String NODE_CLIENT_HTTP_PORT = "node.client.http.port";
    public static final String NODE_CLIENT_TCP_PORT = "node.client.tcp.port";

    public static final String SERVICE_NAME = "service.name";
    public static final String NUMBER_OF_SHARDS = "number.of.shards";
    public static final int DEFAULT_NUMBER_OF_SHARDS = 1;
    public static final String NUMBER_OF_REPLICAS = "number.of.replicas";
    public static final String MIN_SYNC_REPLICAS = "min.sync.replicas";
    public static final int DEFAULT_NUMBER_OF_REPLICAS = 1;
    public static final String SERVICE_INSTANCE_IP = "service.instance.ip";
    public static final String SERVICE_INSTANCE_PORT = "service.instance.port";

    public static final String HEARTBEAT_INTERVAL = "heartbeat.interval.second";
    public static final String HEARTBEAT_CHECK_INTERVAL = "heartbeat.check.interval";
    // 心跳超时时间
    public static final String HEARTBEAT_TIMEOUT_PERIOD = "heartbeat.timeout.period";

    public static final int DEFAULT_HEARTBEAT_CHECK_INTERVAL = 3;
    // 超过该事件仍未连接到则移除实例
    public static final int DEFAULT_HEARTBEAT_TIMEOUT_PERIOD = 30;

    public static final int REQUEST_HEADER_LENGTH = 4;
    public static final int REQUEST_WAIT_SLEEP_INTERVAL = 300;


}
