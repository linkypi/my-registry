package org.hiraeth.govern.server.node.core;

import org.hiraeth.govern.common.domain.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description: 为客户端推送请求或响应的全局队列
 * @date: 2023/12/2 23:39
 */
public class ClientMessageQueue {
    private static class Singleton {

        static ClientMessageQueue instance = new ClientMessageQueue();

    }

    public static ClientMessageQueue getInstance() {
        return ClientMessageQueue.Singleton.instance;
    }

    // connectionId -> messages
    private static final Map<String,LinkedBlockingQueue<Message>> QUEUES = new ConcurrentHashMap<>();

    public void initQueue(String clientConnectionId) {
        QUEUES.put(clientConnectionId, new LinkedBlockingQueue<>());
    }

    public void addMessage(String clientConnectionId, Message message) {
        QUEUES.get(clientConnectionId).add(message);
    }

    public LinkedBlockingQueue<Message> getMessageQueue(String clientConnectionId) {
        return QUEUES.get(clientConnectionId);
    }

}
