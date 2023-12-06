package org.hiraeth.registry.server.config;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.ConfigurationException;
import org.hiraeth.registry.common.util.CommonUtil;
import org.hiraeth.registry.common.util.StringUtil;
import org.hiraeth.registry.common.domain.ServerAddress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.hiraeth.registry.common.constant.Constant.*;

/**
 * @author: leo
 * @description: 服务治理平台配置类
 * @ClassName: org.hiraeth.govern.server.config
 * @date: 2023/11/27 11:57
 */
@Slf4j
@Getter
@Setter
public class Configuration {

    private Configuration(){
    }
    @Parameter(names = {"-f", "--config"}, description = "config file path," +
            " ex: /path/to/config.properties, /path/to/config.cnf...")
    private String configPath;

    private boolean isControllerCandidate;

    private String dataDir;
    private String logDir;
    private String nodeIP;
    private int nodeInternalPort;
    private int nodeClientHttpPort;
    private int nodeClientTcpPort;
    private int clusterNodeCount;
    private int heartbeatTimeoutPeriod;
    private int heartbeatCheckInterval;
    private int numberOfReplicas;
    private int numberOfShards;
    private int minSyncReplicas;

    private ServerAddress serverAddress;

    private Map<String, ServerAddress> controllerServers = new HashMap<>();

    private static class Singleton {
        private static Configuration instance = new Configuration();
    }

    public static Configuration getInstance() {
        return Singleton.instance;
    }

    public void parse() throws Exception {
        try {
            Properties configProperties = loadConfigFile();

            // 解析 controller.candidate
            this.isControllerCandidate = CommonUtil.parseBoolean(configProperties, IS_CONTROLLER_CANDIDATE, false);

            parseControllerServers(configProperties);

            this.dataDir = CommonUtil.getString(configProperties, DATA_DIR);
            this.logDir = CommonUtil.getString(configProperties, LOG_DIR);

            this.nodeIP = CommonUtil.parseIP(configProperties, NODE_IP);

            this.nodeInternalPort = CommonUtil.parseInt(configProperties, NODE_INTERNAL_PORT);
            this.nodeClientHttpPort = CommonUtil.parseInt(configProperties, NODE_CLIENT_HTTP_PORT);
            this.nodeClientTcpPort = CommonUtil.parseInt(configProperties, NODE_CLIENT_TCP_PORT);
            this.clusterNodeCount = CommonUtil.parseInt(configProperties, CLUSTER_NODE_COUNT);
            this.heartbeatTimeoutPeriod = CommonUtil.parseInt(configProperties, HEARTBEAT_TIMEOUT_PERIOD, DEFAULT_HEARTBEAT_TIMEOUT_PERIOD);
            this.heartbeatCheckInterval = CommonUtil.parseInt(configProperties, HEARTBEAT_CHECK_INTERVAL, DEFAULT_HEARTBEAT_CHECK_INTERVAL);
            this.numberOfReplicas = CommonUtil.parseInt(configProperties, NUMBER_OF_REPLICAS, DEFAULT_NUMBER_OF_REPLICAS);
            this.numberOfShards = CommonUtil.parseInt(configProperties, NUMBER_OF_SHARDS, DEFAULT_NUMBER_OF_SHARDS);
            this.minSyncReplicas = CommonUtil.parseInt(configProperties, MIN_SYNC_REPLICAS);

            serverAddress = new ServerAddress(nodeIP, nodeInternalPort, nodeClientHttpPort, nodeClientTcpPort);

            for (String key : controllerServers.keySet()) {
                ServerAddress address = controllerServers.get(key);
                if (key.equals(nodeIP + ":" + nodeInternalPort)) {
                    address.setHost(nodeIP);
                    address.setClientTcpPort(nodeClientTcpPort);
                    address.setClientHttpPort(nodeClientHttpPort);
                }
            }

        } catch (IllegalArgumentException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (FileNotFoundException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (IOException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        }
    }

    private Properties loadConfigFile() throws ConfigurationException, IOException {
        if (StringUtil.isEmpty(configPath)) {
            throw new ConfigurationException("config file path cannot be empty.");
        }

        log.info("parsing config file: {}", configPath);
        File configFile = new File(configPath);
        if (!configFile.exists()) {
            throw new IllegalArgumentException("config file " + configPath + " doesn't exist...");
        }

        Properties configProperties = new Properties();
        FileInputStream fileInputStream = new FileInputStream(configFile);
        try {
            configProperties.load(fileInputStream);
        } finally {
            fileInputStream.close();
        }
        return configProperties;
    }

    private void parseControllerServers(Properties configProperties) {
        List<String> list = CommonUtil.parseIpPortList(configProperties, CONTROLLER_CANDIDATE_SERVERS);
        for (String item : list) {
            ServerAddress serverAddress = new ServerAddress(item);
            controllerServers.put(serverAddress.getNodeId(), serverAddress);
        }
    }

    public ServerAddress getCurrentNodeAddress() {
        if(isControllerCandidate) {
            return controllerServers.get(getNodeId());
        }
        return serverAddress;
    }

    public String getNodeId() {
        return nodeIP + ":" + nodeInternalPort;
    }

    /**
     * 获取配置文件中排在当前节点之前的节点地址列表
     *
     * @return
     */
    public List<ServerAddress> getBeforeControllerAddress() {
        ArrayList<ServerAddress> serverAddresses = new ArrayList<>(controllerServers.values());
        List<ServerAddress> result = new ArrayList<>();
        for (ServerAddress address : serverAddresses) {
            if (!address.getNodeId().equals(getNodeId())) {
                result.add(address);
            }else{
                break;
            }
        }
        return result;
    }

    /**
     * 从给定的节点列表中获取最后的一个节点的nodeId
     * @param nodeIds
     * @return
     */
    public String getBetterControllerAddress(Set<String> nodeIds) {
        List<ServerAddress> serverAddresses = new ArrayList<>(controllerServers.values());
        Collections.reverse(serverAddresses);

        for (ServerAddress address : serverAddresses) {
            Optional<String> first = nodeIds.stream().filter(a -> a.equals(address.getNodeId())).findFirst();
            if (first.isPresent()) {
                return first.get();
            }
        }
        // 若都未找到则随机返回一个
        int index = new Random().nextInt();
        return new ArrayList<>(nodeIds).get(index);
    }

    public List<ServerAddress> getOtherControllerAddresses() {
        return controllerServers.values().stream()
                .filter(a -> !a.getNodeId().equals(this.getNodeId()))
                .collect(Collectors.toList());
    }

    public List<String> getAllTheOtherControllerNodeIds() {
        return controllerServers.values().stream().map(ServerAddress::getNodeId)
                .filter(a -> !Objects.equals(a, getNodeId())).collect(Collectors.toList());
    }
}
