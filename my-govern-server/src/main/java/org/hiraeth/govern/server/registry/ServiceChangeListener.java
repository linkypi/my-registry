package org.hiraeth.govern.server.registry;

import org.hiraeth.govern.common.domain.ServiceInstanceInfo;
import org.hiraeth.govern.common.domain.request.ServiceChangedRequest;
import org.hiraeth.govern.server.core.ClientMessageQueue;

import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 22:11
 */
public class ServiceChangeListener {
    private final String clientConnectionId;

    public ServiceChangeListener(String clientConnectionId) {
        this.clientConnectionId = clientConnectionId;
    }

    public void onChange(String serviceName, List<ServiceInstanceInfo> serviceInstanceInfos) {
        ClientMessageQueue messageQueue = ClientMessageQueue.getInstance();
        ServiceChangedRequest serviceChangedRequest = new ServiceChangedRequest(serviceName, serviceInstanceInfos);
        serviceChangedRequest.buildBuffer();
        messageQueue.getMessageQueue(clientConnectionId).add(serviceChangedRequest);
    }
}
