package com.hiraeth.govern.client.network;

import com.hiraeth.govern.client.config.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.BaseResponse;
import org.hiraeth.govern.common.domain.HeartbeatRequest;
import org.hiraeth.govern.common.domain.Request;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 16:39
 */
@Slf4j
public class HeartbeatThread extends Thread{

    private Map<String, LinkedBlockingQueue<Request>> requestQueue;
    private ConcurrentHashMap<Long, BaseResponse> responses;
    private String connectionId;
    private static final int REQUEST_WAIT_SLEEP_INTERVAL = 200;
    public HeartbeatThread(Map<String, LinkedBlockingQueue<Request>> requestQueue,
                           ConcurrentHashMap<Long, BaseResponse> responses,
                           String connectionId){
        this.requestQueue = requestQueue;
        this.connectionId = connectionId;
        this.responses = responses;
    }
    @Override
    public void run() {
        Configuration configuration = Configuration.getInstance();
        int heartbeatInterval = configuration.getHeartbeatInterval();
        String serviceName = configuration.getServiceName();
        String serviceInstanceIp = configuration.getServiceInstanceIp();
        int serviceInstancePort = configuration.getServiceInstancePort();

        while (true){
            try {
                HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
                heartbeatRequest.setServiceName(serviceName);
                heartbeatRequest.setServiceInstancePort(serviceInstancePort);
                heartbeatRequest.setServiceInstanceIp(serviceInstanceIp);
                requestQueue.get(connectionId).add(heartbeatRequest.toRequest());

                // server端处理心跳请求，返回响应
                while(responses.get(heartbeatRequest.getRequestId()) == null) {
                    responses.remove(heartbeatRequest.getRequestId());
                    Thread.sleep(REQUEST_WAIT_SLEEP_INTERVAL);
                }

                log.info("heartbeat success, instance: {}/{}/{}.", serviceName, serviceInstanceIp, serviceInstancePort);

                Thread.sleep(heartbeatInterval * 1000L);
            }catch (Exception ex){
                log.error("heartbeat thread occur error", ex);
            }
        }
    }
}
