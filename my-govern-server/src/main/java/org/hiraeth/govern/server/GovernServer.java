package org.hiraeth.govern.server;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.constant.NodeType;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.config.ConfigurationException;
import org.hiraeth.govern.server.node.NodeStatus;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.master.MasterNode;
import org.hiraeth.govern.server.node.master.Node;
import org.hiraeth.govern.server.node.master.SlaveNode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 服务治理平台 Server 端
 * @author: leo
 * @description:
 * @ClassName: org.hiraeth.govern.server
 * @date: 2023/11/27 11:54
 */
@Slf4j
public class GovernServer {

    private static final int SHUTDOWN_CHECK_INTERVAL = 500;

    public static void main(String[] args) {
        try {

            Configuration configuration = Configuration.getInstance();
            JCommander.newBuilder()
                    .addObject(configuration)
                    .build()
                    .parse(args);

            NodeStatusManager.setNodeStatus(NodeStatus.INITIALIZING);
            configuration.parse();

            NodeStatusManager.setNodeStatus(NodeStatus.RUNNING);
            if(configuration.getNodeType() == NodeType.Master){
                new MasterNode().start();
            }else{
                new SlaveNode().start();
            }
            NodeStatusManager.setNodeStatus(NodeStatus.RUNNING);

        } catch (ConfigurationException ex) {
            log.error("configuration exception", ex);
            System.exit(2);
        }catch (Exception ex){
            log.error("start govern server occur error", ex);
            System.exit(1);
        }

        if(NodeStatusManager.getNodeStatus() == NodeStatus.SHUTDOWN){
            log.info("system is going to shutdown normally.");
        }else if(NodeStatusManager.getNodeStatus() == NodeStatus.FATAL){
            log.error("system is going to shutdown because of fatal error.");
        }
    }

    private void waitForShutdown() throws InterruptedException {
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING){
            Thread.sleep(SHUTDOWN_CHECK_INTERVAL);
        }
    }

}
