package org.hiraeth.govern.server.node.core;

import cn.hutool.core.date.LocalDateTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.entity.ClusterBaseMessage;
import org.hiraeth.govern.server.entity.ClusterMessageType;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description: 服务端消息读取队列
 * @date: 2023/12/2 23:39
 */
@Slf4j
public class ServerMessageQueue {

    private static final long WAIT_FRO_RESPONSE_TIMEOUT = 5 * 1000L;

    private static class Singleton {
        static ServerMessageQueue instance = new ServerMessageQueue();
    }

    public static ServerMessageQueue getInstance() {
        return ServerMessageQueue.Singleton.instance;
    }

    // stage -> messageType -> messages
    private static final Map<Integer,Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>>> QUEUES = new ConcurrentHashMap<>();

    public void initQueue() {
        for (ElectionStage.ELStage stage :ElectionStage.ELStage.values()) {
            Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>> map = new ConcurrentHashMap<>();
            for (ClusterMessageType clusterMessageType : ClusterMessageType.values()) {
                map.put(clusterMessageType.getValue(), new LinkedBlockingQueue<>());
            }
            QUEUES.put(stage.getValue(), map);
        }
    }

    public void addMessage(ClusterBaseMessage message) {
        int messageType = message.getClusterMessageType().getValue();
        QUEUES.get(message.getStage()).get(messageType).add(message);
    }

    /**
     * 获取选举阶段消息队列
     * @return
     */
    public Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>> getElectingMessageQueue() {
        return QUEUES.get(ElectionStage.ELStage.ELECTING.getValue());
    }

    /**
     * 获取领导阶段消息队列
     * @return
     */
    public Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>> getLeadingMessageQueue() {
        return QUEUES.get(ElectionStage.ELStage.LEADING.getValue());
    }

    public ClusterBaseMessage takeElectingMessage(ClusterMessageType clusterMessageType){
        return takeMessage(ElectionStage.ELStage.ELECTING, clusterMessageType);
    }

    public ClusterBaseMessage takeLeadingMessage(ClusterMessageType clusterMessageType){
        return takeMessage(ElectionStage.ELStage.LEADING, clusterMessageType);
    }

    public ClusterBaseMessage takeMessage(ElectionStage.ELStage stage, ClusterMessageType clusterMessageType){
        try {
            Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>> queueMap = QUEUES.get(stage);
            return queueMap.get(clusterMessageType.getValue()).take();
        }catch (Exception ex){
            log.error("take message from receive queue failed, stage: {}, message type: {}", stage, clusterMessageType, ex);
            return null;
        }
    }

    public int countElectingMessage(ClusterMessageType clusterMessageType){
        return countMessage(ElectionStage.ELStage.ELECTING, clusterMessageType);
    }

    public int countLeadingMessage(ClusterMessageType clusterMessageType){
        return countMessage(ElectionStage.ELStage.LEADING, clusterMessageType);
    }

    public int countMessage(ElectionStage.ELStage stage, ClusterMessageType clusterMessageType){
        Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>> queueMap = QUEUES.get(stage.getValue());
        BlockingQueue<ClusterBaseMessage> queue = queueMap.get(clusterMessageType.getValue());
        if(queue == null){
            return 0;
        }
        return queue.size();
    }

    /**
     * 超市等待消息返回
     * @param clusterMessageType
     * @return
     */
    public ClusterBaseMessage waitForLeadingMessage(ClusterMessageType clusterMessageType) {
        return waitForMessage(ElectionStage.ELStage.LEADING, clusterMessageType);
    }

    public ClusterBaseMessage waitForElectingMessage(ClusterMessageType clusterMessageType) {
        return waitForMessage(ElectionStage.ELStage.ELECTING, clusterMessageType);
    }

    public ClusterBaseMessage waitForMessage(ElectionStage.ELStage stage, ClusterMessageType clusterMessageType) {
        try {
            boolean timeout = false;
            LocalDateTime startTime = LocalDateTimeUtil.now();
            while (countMessage(stage, clusterMessageType) == 0) {
                Thread.sleep(100);
                LocalDateTime now = LocalDateTimeUtil.now();
                if (!LocalDateTimeUtil.between(startTime, now).minusMillis(WAIT_FRO_RESPONSE_TIMEOUT).isNegative()) {
                    timeout = true;
                    break;
                }
            }
            if (timeout) {
                return null;
            }
            Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>> queueMap = QUEUES.get(stage.getValue());
            return queueMap.get(clusterMessageType.getValue()).take();
        } catch (Exception ex) {
            log.error("wait response timeout from receive queue failed, stage: {}, meesage type: {}.", stage, clusterMessageType, ex);
            return null;
        }
    }

}
