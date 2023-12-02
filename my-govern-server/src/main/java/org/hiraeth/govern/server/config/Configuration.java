package org.hiraeth.govern.server.config;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.constant.Constant;
import org.hiraeth.govern.common.domain.ConfigurationException;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.common.util.StringUtil;
import org.hiraeth.govern.common.domain.ServerAddress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hiraeth.govern.common.constant.Constant.*;

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

    private ServerAddress serverAddress;

    private Map<String, ServerAddress> controllerServers = new HashMap<>();

    private static class Singleton {
        private static Configuration instance = new Configuration();
    }

    public static Configuration getInstance() {
        return Singleton.instance;
    }

    /**
     * for example:
     * 192.168.10.100:2156,192.168.10.110:2156
     */
    private static Pattern CLUSTER_REGEX_COMPILE = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)");
    private static Pattern IP_REGEX_COMPILE = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");

    public void parse() throws Exception {
        try {
            Properties configProperties = loadConfigFile();

            // 解析 controller.candidate
            String isControllerCandidateStr = configProperties.getProperty(IS_CONTROLLER_CANDIDATE);
            validateIsControllerCandidate(isControllerCandidateStr);
            this.isControllerCandidate = Boolean.parseBoolean(isControllerCandidateStr);
            log.debug("parameter {} = {}", IS_CONTROLLER_CANDIDATE, isControllerCandidateStr);

            parseControllerServers(configProperties);

            String dir = configProperties.getProperty(DATA_DIR);
            if (StringUtil.isEmpty(dir)) {
                throw new IllegalArgumentException(DATA_DIR + " cannot empty.");
            }
            this.dataDir = dir;
            log.debug("parameter {} = {}", DATA_DIR, dir);

            String logDir = configProperties.getProperty(LOG_DIR);
            if (StringUtil.isEmpty(logDir)) {
                throw new IllegalArgumentException(LOG_DIR + " cannot empty.");
            }
            this.logDir = logDir;
            log.debug("parameter {} = {}", LOG_DIR, logDir);

            this.nodeIP = parseNodeIP(configProperties);

            this.nodeInternalPort = CommonUtil.parseInt(configProperties, NODE_INTERNAL_PORT);
            this.nodeClientHttpPort = CommonUtil.parseInt(configProperties, NODE_CLIENT_HTTP_PORT);
            this.nodeClientTcpPort = CommonUtil.parseInt(configProperties, NODE_CLIENT_TCP_PORT);
            this.clusterNodeCount = CommonUtil.parseInt(configProperties, CLUSTER_NODE_COUNT);

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

    private static String parseNodeIP(Properties configProperties) {
        String nodeIp = configProperties.getProperty(NODE_IP);
        if (StringUtil.isEmpty(nodeIp)) {
            throw new IllegalArgumentException(NODE_IP + " cannot empty.");
        }
        Matcher matcher = IP_REGEX_COMPILE.matcher(nodeIp);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(Constant.NODE_IP + " parameters " + nodeIp + " is invalid.");
        }
        return nodeIp;
    }

    private boolean validateIsControllerCandidate(String isControllerCandidate) {
        if(StringUtil.isEmpty(isControllerCandidate)){
            return true;
        }

        if("true".equals(isControllerCandidate) || "false".equals(isControllerCandidate)){
            return true;
        }
        throw new IllegalArgumentException("controller.candidate must be true or false, not "+ isControllerCandidate);
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
        String nodeServers = configProperties.getProperty(Constant.CONTROLLER_CANDIDATE_SERVERS);
        if (StringUtil.isEmpty(nodeServers)) {
            throw new IllegalArgumentException(Constant.CONTROLLER_CANDIDATE_SERVERS + " cannot be empty.");
        }
        String[] arr = nodeServers.split(",");
        if (arr.length == 0) {
            throw new IllegalArgumentException(Constant.CONTROLLER_CANDIDATE_SERVERS + " cannot be empty.");
        }
        for (String item : arr) {
            Matcher matcher = CLUSTER_REGEX_COMPILE.matcher(item);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(Constant.CONTROLLER_CANDIDATE_SERVERS + " parameters " + item + " is invalid.");
            }
        }
        for (String item : arr) {
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
