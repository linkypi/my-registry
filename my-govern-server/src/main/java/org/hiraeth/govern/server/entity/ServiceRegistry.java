package org.hiraeth.govern.server.entity;

import org.hiraeth.govern.common.domain.ServiceInstance;

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

    private Map<String, List<ServiceInstance>> services = new ConcurrentHashMap<>();

    public void register(ServiceInstance instance){
        List<ServiceInstance> list = services.getOrDefault(instance.getServiceName(), new CopyOnWriteArrayList<>());
        list.add(instance);
    }
}
