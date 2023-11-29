package org.hiraeth.govern.server.config;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.constant.Constant;
import org.hiraeth.govern.server.node.entity.NodeType;
import org.hiraeth.govern.common.util.StringUtil;
import org.hiraeth.govern.server.node.entity.NodeAddress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
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

    /**
     * 节点类型: master / slave
     */
    private NodeType nodeType;
    private int nodeId;
    private boolean isControllerCandidate;

    // slave 节点参数
    private String masterServerAddress;
    private int masterServerPort;

    private List<NodeAddress> masterNodeServers = new ArrayList<>();

    private static class Singleton {
        private static Configuration instance = new Configuration();
    }

    public static Configuration getInstance() {
        return Singleton.instance;
    }

    /**
     * for example:
     * 1:192.168.10.100:2156:2356:2556
     */
    private static Pattern CLUSTER_REGEX_COMPILE = Pattern.compile("(\\d+):(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+):(\\d+):(\\d+)");
    private static Pattern IP_REGEX_COMPILE = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");

    public void parse() throws Exception {
        try {
            Properties configProperties = loadConfigFile();

            // 解析 node.type
            String nodeType = configProperties.getProperty(NODE_TYPE);
            if (StringUtil.isEmpty(nodeType)) {
                throw new IllegalArgumentException("node.type cannot be empty.");
            }
            NodeType nodeTypeEnum = NodeType.of(nodeType);
            if (nodeTypeEnum == null) {
                throw new IllegalArgumentException("node.type must be master or slave.");
            }
            log.debug("parameter {} = {}", NODE_TYPE, nodeType);
            this.nodeType = nodeTypeEnum;

            // 解析 controller.candidate
            String isControllerCandidateStr = configProperties.getProperty(IS_CONTROLLER_CANDIDATE);
            validateIsControllerCandidate(isControllerCandidateStr);
            this.isControllerCandidate = Boolean.parseBoolean(isControllerCandidateStr);
            log.debug("parameter {} = {}", IS_CONTROLLER_CANDIDATE, isControllerCandidateStr);

            // 解析 node.id
            String nodeIdStr = configProperties.getProperty(NODE_ID);
            if (validateNodeId(nodeIdStr)) {
                this.nodeId = Integer.parseInt(nodeIdStr);
            }
            log.debug("parameter {} = {}", NODE_ID, nodeIdStr);

            if(this.nodeType == NodeType.Master) {
                parseMasterNodeServer(configProperties);
            }else{
                parseSlaveNodeConfig(configProperties);
            }
        } catch (IllegalArgumentException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (FileNotFoundException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (IOException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        }
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

    private boolean validateNodeId(String nodeId) {
        if (StringUtil.isEmpty(nodeId)) {
            throw new IllegalArgumentException(NODE_ID + " cannot be empty");
        }

        boolean matches = Pattern.matches("\\d+", nodeId);
        if (!matches) {
            throw new IllegalArgumentException(NODE_ID + " must be a number, not string -> " + nodeId);
        }
        return true;
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

    private void parseMasterNodeServer(Properties configProperties) {
        String nodeServers = configProperties.getProperty(Constant.MASTER_NODE_SERVERS);
        if (StringUtil.isEmpty(nodeServers)) {
            throw new IllegalArgumentException(Constant.MASTER_NODE_SERVERS + " cannot be empty.");
        }
        String[] arr = nodeServers.split(",");
        if (arr.length == 0) {
            throw new IllegalArgumentException(Constant.MASTER_NODE_SERVERS + " cannot be empty.");
        }
        for (String item : arr) {
            Matcher matcher = CLUSTER_REGEX_COMPILE.matcher(item);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(Constant.MASTER_NODE_SERVERS + " parameters " + item + " is invalid.");
            }
        }
        for (String item : arr) {
            masterNodeServers.add(new NodeAddress(item));
        }
    }

    private void parseSlaveNodeConfig(Properties configProperties) {
        this.masterServerAddress = configProperties.getProperty(MASTER_NODE_ADDRESS);
        if(StringUtil.isEmpty(masterServerAddress)){
            throw new IllegalArgumentException(Constant.MASTER_NODE_ADDRESS + " cannot be empty.");
        }
        Matcher matcher = IP_REGEX_COMPILE.matcher(masterServerAddress);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(Constant.MASTER_NODE_ADDRESS + " parameters "
                    + masterServerAddress + " is invalid, must be an ip address.");
        }

        String serverPort = configProperties.getProperty(MASTER_NODE_PORT);
        if(StringUtil.isEmpty(serverPort)){
            throw new IllegalArgumentException(Constant.MASTER_NODE_PORT + " cannot be empty.");
        }

        try {
            this.masterServerPort = Integer.parseInt(serverPort);
        }catch (Exception ex){
            throw new IllegalArgumentException(Constant.MASTER_NODE_PORT + " must be a number.");
        }
    }

    public NodeAddress getCurrentNodeAddress() {
        for (NodeAddress item : masterNodeServers) {
            if (nodeId == item.getNodeId()) {
                return item;
            }
        }
        return null;
    }

    /**
     * 获取比当前node.id较小的节点地址
     *
     * @return
     */
    public List<NodeAddress> getLowerIdMasterAddress() {
        return masterNodeServers.stream().filter(a -> a.getNodeId() < nodeId).collect(Collectors.toList());
    }

    public Integer getMasterNodeIdByIpPort(String ip, int port) {
        Optional<NodeAddress> first = masterNodeServers.stream()
                .filter(a -> (a.getHost() + a.getMasterPort()).equals(ip + port)).findFirst();
        if (first.isPresent()) {
            return first.get().getNodeId();
        }
        return null;
    }

    public List<Integer> getAllTheOtherNodeIds() {
        return masterNodeServers.stream().map(NodeAddress::getNodeId)
                .filter(a -> a != nodeId).collect(Collectors.toList());
    }
}
