package org.hiraeth.govern.server.config;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.constant.Constant;
import org.hiraeth.govern.common.constant.NodeType;
import org.hiraeth.govern.common.util.StringUtil;
import org.hiraeth.govern.server.node.master.MasterNode;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hiraeth.govern.common.constant.Constant.NODE_ID;
import static org.hiraeth.govern.common.constant.Constant.NODE_TYPE;

/**
 * @author: leo
 * @description: 服务治理平台配置类
 * @ClassName: org.hiraeth.govern.server.config
 * @date: 2023/11/27 11:57
 */
@Slf4j
@Component
@Getter
@Setter
public class Configuration {

    @Parameter(names = {"-f", "--config"}, description = "config file path," +
            " ex: /path/to/config.properties, /path/to/config.cnf...")
    private String configPath;

    /**
     * 节点类型: master / slave
     */
    private NodeType nodeType;
    private int nodeId;
    private List<String> masterNodeServers;

    /**
     * for example:
     * 1:192.168.10.100:2156:2356:2556
     * 1:localhost:2156:2356:2556
     */
    private static Pattern REGEX_COMPILE = Pattern.compile("(\\d+):((\\d+\\.\\d+\\.\\d+\\.\\d+)|\\w+):(\\d+):(\\d+):(\\d+)");

    public void parse() throws Exception {
        try {
            Properties configProperties = loadConfigFile();

            String nodeType = configProperties.getProperty(NODE_TYPE);
            if (StringUtil.isEmpty(nodeType)) {
                throw new IllegalArgumentException("node.type cannot be empty.");
            }

            NodeType nodeTypeEnum = NodeType.of(nodeType);
            if (nodeTypeEnum == null) {
                throw new IllegalArgumentException("node.type must be master or slave.");
            }

            this.nodeType = nodeTypeEnum;

            String nodeIdStr = configProperties.getProperty(NODE_ID);
            if(validateNodeId(nodeIdStr)) {
                this.nodeId = Integer.parseInt(nodeIdStr);
            }

            parseMasterNodeServer(configProperties);
        } catch (IllegalArgumentException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (FileNotFoundException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (IOException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        }
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
            Matcher matcher = REGEX_COMPILE.matcher(item);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(Constant.MASTER_NODE_SERVERS + " parameters " + item + " is invalid.");
            }
        }
    }
}
