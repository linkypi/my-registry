package org.hiraeth.govern.server.core;

import org.hiraeth.govern.common.domain.Response;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 23:39
 */
public class ClientRequestQueue {
    private static class Singleton {

        static ClientRequestQueue instance = new ClientRequestQueue();

    }

    public static ClientRequestQueue getInstance() {
        return ClientRequestQueue.Singleton.instance;
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
