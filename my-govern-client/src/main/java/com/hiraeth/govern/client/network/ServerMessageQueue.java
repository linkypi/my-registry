package com.hiraeth.govern.client.network;

import org.hiraeth.govern.common.domain.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/3 14:19
 */
public class ServerMessageQueue {
    private static class Singleton {

        static ServerMessageQueue instance = new ServerMessageQueue();
    }

    public static ServerMessageQueue getInstance() {
        return ServerMessageQueue.Singleton.instance;
    }

    // connectionId -> messages
    private static final Map<String, LinkedBlockingQueue<Message>> QUEUES = new ConcurrentHashMap<>();


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
