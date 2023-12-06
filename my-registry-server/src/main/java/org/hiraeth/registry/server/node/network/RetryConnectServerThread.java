package org.hiraeth.registry.server.node.network;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.ServerAddress;
import org.hiraeth.registry.server.entity.NodeStatus;
import org.hiraeth.registry.server.node.core.NodeInfoManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/5 14:52
 */
@Slf4j
public class RetryConnectServerThread extends Thread {

    private static final long RETRY_CONNECT_INTERVAL = 30 * 1000L;

    private final CopyOnWriteArrayList<ServerAddress> retryConnectControllers;
    public RetryConnectServerThread(CopyOnWriteArrayList<ServerAddress> retryConnectControllers){
        this.retryConnectControllers = retryConnectControllers;
    }

    @Override
    public void run() {
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        while (NodeStatus.RUNNING == NodeInfoManager.getNodeStatus()) {
            try {
                Thread.sleep(RETRY_CONNECT_INTERVAL);
            } catch (InterruptedException e) {
                log.error("thread interrupted cause of unknown reasons.", e);
            }

            // 重试连接, 连接成功后移除相关节点
            List<ServerAddress> retrySuccessAddresses = new ArrayList<>();
            for (ServerAddress serverAddress : retryConnectControllers) {
                log.info("scheduled retry connect controller node {}.", serverAddress.getNodeId());
                if (serverNetworkManager.connectControllerNode(serverAddress)) {
                    log.info("scheduled retry connect controller node success {}.", serverAddress.getNodeId());
                    retrySuccessAddresses.add(serverAddress);
                }
            }

            for (ServerAddress address : retrySuccessAddresses) {
                retryConnectControllers.remove(address);
            }
        }
    }
}