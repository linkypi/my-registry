package org.hiraeth.govern.server.core;

import lombok.Getter;
import org.hiraeth.govern.common.domain.Response;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 22:43
 */
@Getter
public class ClientSubscribeQueue {
    private ClientSubscribeQueue(){}

    private static class Singleton {

        static ClientSubscribeQueue instance = new ClientSubscribeQueue();

    }

    public static ClientSubscribeQueue getInstance() {
        return Singleton.instance;
    }

    // 客户端订阅的待发送队列
    private Map<String, LinkedBlockingQueue<Response>> requestQueues = new ConcurrentHashMap<>();

    public void initRequestQueue(String clientConnectionId) {
        requestQueues.put(clientConnectionId, new LinkedBlockingQueue<>());
    }

    public void offerRequest(String clientConnectionId, Response request) {
        LinkedBlockingQueue<Response> requestQueue = requestQueues.get(clientConnectionId);
        requestQueue.offer(request);
    }

}
