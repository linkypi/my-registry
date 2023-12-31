package com.hiraeth.registry.client.config;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.constant.Constant;
import org.hiraeth.registry.common.domain.ConfigurationException;
import org.hiraeth.registry.common.domain.ServerAddress;
import org.hiraeth.registry.common.util.CommonUtil;
import org.hiraeth.registry.common.util.StringUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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


    private String dataDir;
    private String serviceInstanceIp;
    private int serviceInstancePort;
    private String serviceName;
    private int heartbeatInterval;

    private List<ServerAddress> controllerCandidateServers = new ArrayList<>();

    private static class Singleton {
        private static Configuration instance = new Configuration();
    }

    public static Configuration getInstance() {
        return Singleton.instance;
    }

    /**
     * for example:
     * 192.168.10.100:2556
     */
    private static Pattern CLUSTER_REGEX_COMPILE = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)");

    public void parse() throws Exception {
        try {
            Properties configProperties = loadConfigFile(configPath);

            parseControllerCandidateServers(configProperties);

            String dir = configProperties.getProperty(DATA_DIR);
            if (StringUtil.isEmpty(dir)) {
                throw new IllegalArgumentException(DATA_DIR + " cannot empty.");
            }
            this.dataDir = dir;

            String serviceName = configProperties.getProperty(SERVICE_NAME);
            if (StringUtil.isEmpty(serviceName)) {
                throw new IllegalArgumentException(SERVICE_NAME + " cannot empty.");
            }
            this.serviceName = serviceName;

            String serviceIP = configProperties.getProperty(SERVICE_INSTANCE_IP);
            if (StringUtil.isEmpty(serviceIP)) {
                throw new IllegalArgumentException(SERVICE_INSTANCE_IP + " cannot empty.");
            }
            this.serviceInstanceIp = serviceIP;

            this.serviceInstancePort = CommonUtil.parseInt(configProperties, SERVICE_INSTANCE_PORT);
            this.heartbeatInterval = CommonUtil.parseInt(configProperties, HEARTBEAT_INTERVAL);

        } catch (IllegalArgumentException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (FileNotFoundException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        } catch (IOException ex) {
            throw new ConfigurationException("parsing config file occur error. ", ex);
        }
    }

    private Properties loadConfigFile(String file) throws ConfigurationException, IOException {
        if (StringUtil.isEmpty(file)) {
            throw new ConfigurationException("config file path cannot be empty.");
        }

        log.info("parsing config file: {}", file);
        File configFile = new File(file);
        if (!configFile.exists()) {
            throw new IllegalArgumentException("config file " + file + " doesn't exist...");
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

    private void parseControllerCandidateServers(Properties configProperties) {
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
            ServerAddress nodeAddress = new ServerAddress(item);
            controllerCandidateServers.add(nodeAddress);
        }
    }

}
