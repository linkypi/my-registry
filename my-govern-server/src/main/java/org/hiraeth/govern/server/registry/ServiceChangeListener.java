package org.hiraeth.govern.server.registry;

import org.hiraeth.govern.common.domain.ServiceInstanceInfo;

import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 22:11
 */
public class ServiceChangeListener {
    private String clientConnectionId;

    public ServiceChangeListener(String clientConnectionId) {
        this.clientConnectionId = clientConnectionId;
    }

    public void onChange(List<ServiceInstanceInfo> serviceInstanceInfos) {

    }
}
