package org.hiraeth.registry.server.ha;

import cn.hutool.core.date.LocalDateTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.server.entity.RemoteServer;
import org.hiraeth.registry.server.entity.ServerRequestType;
import org.hiraeth.registry.server.entity.request.BeAliveAskRequest;
import org.hiraeth.registry.server.entity.response.ResponseMessage;
import org.hiraeth.registry.server.node.core.RemoteNodeManager;
import org.hiraeth.registry.server.node.core.ServerMessageQueue;
import org.hiraeth.registry.server.node.network.ServerNetworkManager;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 检测某个节点是否已真正失联, 主观失联不能决定客观情况, 故需要通过其他节点来观察结果
 * @author: leo
 * @description:
 * @ClassName: org.hiraeth.registry.server.ha
 * @date: 2023/12/25 22:43
 */
@Slf4j
public class AliveDetector {

    // 检测重试时间
    private static final long DETECT_RETRY_TIMES = 3;
    // 等待响应返回超时时间
    private static final long WAIT_FRO_TIMEOUT = 5000;

    /**
     * 通过其他候选节点判断某个节点是否已失联
     * @param disconnectedNodeId
     * @param remoteNodeManager
     * @param serverNetworkManager
     * @return
     * @throws InterruptedException
     */
    public static boolean detectAliveFromOtherCandidates(String disconnectedNodeId, RemoteNodeManager remoteNodeManager,
                                                          ServerNetworkManager serverNetworkManager) throws InterruptedException {

        BeAliveAskRequest.Status status = detectAlive(disconnectedNodeId, remoteNodeManager, serverNetworkManager);

        // 若超时返回 null 则重试
        int retryTime = 0;
        while (status == null && retryTime < DETECT_RETRY_TIMES) {
            status = detectAlive(disconnectedNodeId, remoteNodeManager, serverNetworkManager);
            retryTime++;
        }

        return status == null || BeAliveAskRequest.Status.Down == status;
    }

    private static BeAliveAskRequest.Status detectAlive(String disconnectedNodeId, RemoteNodeManager remoteNodeManager,
                                                        ServerNetworkManager serverNetworkManager) throws InterruptedException {
        BeAliveAskRequest beAliveAskRequest = new BeAliveAskRequest(disconnectedNodeId);
        ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();

        // 向其他节点询问远程节点是否存活的请求
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        for (RemoteServer remoteServer : otherControllerCandidates) {
            serverNetworkManager.sendRequest(remoteServer.getNodeId(), beAliveAskRequest);
        }

        Set<String> downSet = new HashSet<>(otherControllerCandidates.size());
        Set<String> aliveSet = new HashSet<>(otherControllerCandidates.size());
        LocalDateTime startTime = LocalDateTimeUtil.now();

        while (true) {
            Thread.sleep(200);

            // 超出 WAIT_FRO_TIMEOUT 时间仍未获得响应则返回null
            LocalDateTime now = LocalDateTimeUtil.now();
            Duration duration = LocalDateTimeUtil.between(startTime, now).minusMillis(WAIT_FRO_TIMEOUT);
            if (!duration.isNegative()) {
                return null;
            }

            int count = messageQueue.countResponseMessage(ServerRequestType.BeAliveAsk);
            if (count == 0) {
                continue;
            }
            ResponseMessage response = messageQueue.takeResponseMessage(ServerRequestType.BeAliveAsk);
            if (response != null && response.isSuccess()) {
                int quorum = otherControllerCandidates.size() / 2 + 1;
                if (response.getCode() == BeAliveAskRequest.Status.Down.getValue()) {
                    log.debug("the disconnected remote node {} is down by {} detection.", disconnectedNodeId, response.getFromNodeId());
                    downSet.add(response.getFromNodeId());
                    if (downSet.size() >= quorum) {
                        return BeAliveAskRequest.Status.Down;
                    }
                } else {
                    log.debug("the disconnected remote node {} is alive by {} detection.", disconnectedNodeId, response.getFromNodeId());
                    aliveSet.add(response.getFromNodeId());
                    if (aliveSet.size() >= quorum) {
                        return BeAliveAskRequest.Status.Alive;
                    }
                }
            }
        }
    }

}
