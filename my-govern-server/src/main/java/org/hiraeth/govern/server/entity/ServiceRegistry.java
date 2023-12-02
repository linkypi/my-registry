package org.hiraeth.govern.server.entity;

import org.hiraeth.govern.common.domain.ServiceInstance;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 服务注册表
 * @author: lynch
 * @description:
 * @date: 2023/12/2 16:11
 */
public class ServiceRegistry {
    private ServiceRegistry(){}

    static class Singleton{
        private static final ServiceRegistry instance = new ServiceRegistry();
    }

    public static ServiceRegistry getInstance(){
        return Singleton.instance;
    }

    // 服务注册表
    private Map<String, List<ServiceInstance>> serviceRegistrys = new ConcurrentHashMap<>();
    // 服务实例集合
    private Map<String, ServiceInstance> serviceInstancesMap = new ConcurrentHashMap<>();

    public void register(ServiceInstance instance){
        List<ServiceInstance> list = serviceRegistrys.getOrDefault(instance.getServiceName(), new CopyOnWriteArrayList<>());
        list.add(instance);
        serviceInstancesMap.put(instance.getServiceInstanceId(), instance);
    }

    public void heartbeat(ServiceInstance instance) {
        String serviceInstanceId = getServiceInstanceId(instance);
        ServiceInstance serviceInstance = serviceInstancesMap.get(serviceInstanceId);
        serviceInstance.setLatestHeartbeatTime(new Date().getTime());
    }

    public static String getServiceInstanceId(ServiceInstance instance) {
        return instance.getServiceName() + "/" + instance.getServiceInstanceIp() + "/" + instance.getServiceInstancePort();
    }
}
