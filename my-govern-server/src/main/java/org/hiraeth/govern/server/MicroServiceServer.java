package org.hiraeth.govern.server;

import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.ConfigurationException;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.core.ServerNode;
import org.hiraeth.govern.server.entity.NodeStatus;
import org.hiraeth.govern.server.node.core.NodeInfoManager;

/**
 * 服务治理平台 Server 端
 * @author: leo
 * @description:
 * @ClassName: org.hiraeth.govern.server
 * @date: 2023/11/27 11:54
 */
@Slf4j
public class MicroServiceServer {

    private static final int SHUTDOWN_CHECK_INTERVAL = 500;

    public static void main(String[] args) {
        try {

            Configuration configuration = Configuration.getInstance();
            JCommander.newBuilder()
                    .addObject(configuration)
                    .build()
                    .parse(args);

            NodeInfoManager.setNodeStatus(NodeStatus.INITIALIZING);
            configuration.parse();

            NodeInfoManager.setNodeStatus(NodeStatus.RUNNING);

            startNodeServer();

        } catch (ConfigurationException ex) {
            log.error("configuration exception", ex);
            System.exit(2);
        }catch (Exception ex){
            log.error("start govern server occur error", ex);
            System.exit(1);
        }

        if(NodeInfoManager.getNodeStatus() == NodeStatus.SHUTDOWN){
            log.info("system is going to shutdown normally.");
        }else if(NodeInfoManager.getNodeStatus() == NodeStatus.FATAL){
            log.error("system is going to shutdown because of fatal error.");
        }
    }

    private static void startNodeServer() {
        new ServerNode().start();
    }

    private void waitForShutdown() throws InterruptedException {
        while (NodeInfoManager.getNodeStatus() == NodeStatus.RUNNING){
            Thread.sleep(SHUTDOWN_CHECK_INTERVAL);
        }
    }

}
