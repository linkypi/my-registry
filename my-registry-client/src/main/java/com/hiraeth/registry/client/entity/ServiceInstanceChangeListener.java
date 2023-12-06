package com.hiraeth.registry.client.entity;

import org.hiraeth.registry.common.domain.ServiceInstanceInfo;

import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 20:19
 */
public interface ServiceInstanceChangeListener {
    void onChange(List<ServiceInstanceInfo> addressList);
}
