package com.hiraeth.registry.client.network;

import com.hiraeth.registry.client.config.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.response.Response;
import org.hiraeth.registry.common.domain.request.HeartbeatRequest;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 16:39
 */
@Slf4j
public class HeartbeatThread extends Thread{

    private ConcurrentHashMap<Long, Response> responses;
    private String connectionId;
    private static final int REQUEST_WAIT_SLEEP_INTERVAL = 200;
    public HeartbeatThread(ConcurrentHashMap<Long, Response> responses,
                           String connectionId){
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
                heartbeatRequest.buildBuffer();
                ServerMessageQueue serverMessageQueue = ServerMessageQueue.getInstance();
                serverMessageQueue.getMessageQueue(connectionId).add(heartbeatRequest);

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
