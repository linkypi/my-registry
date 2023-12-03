package org.hiraeth.govern.server.registry;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.ServiceInstanceInfo;
import org.hiraeth.govern.common.util.CollectionUtil;

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
@Slf4j
public class ServiceRegistry {
    private ServiceRegistry(){
        new HeartbeatThread().start();
    }

    static class Singleton{
        private static final ServiceRegistry instance = new ServiceRegistry();
    }

    public static ServiceRegistry getInstance(){
        return Singleton.instance;
    }

    public Map<String, ServiceInstanceInfo> getServiceInstances(){
        return serviceInstancesMap;
    }

    public Map<String, List<ServiceInstanceInfo>> getServiceRegistry(){
        return serviceRegistry;
    }

    // 服务注册表
    private final Map<String, List<ServiceInstanceInfo>> serviceRegistry = new ConcurrentHashMap<>();
    // 服务实例集合
    private final Map<String, ServiceInstanceInfo> serviceInstancesMap = new ConcurrentHashMap<>();

    // 服务监听器
    private final Map<String, List<ServiceChangeListener>> serviceChangeListeners = new ConcurrentHashMap<>();

    public void register(ServiceInstanceInfo instance) {
        List<ServiceInstanceInfo> serviceInstanceInfos = serviceRegistry.get(instance.getServiceName());
        if (serviceInstanceInfos == null) {
            serviceInstanceInfos = new CopyOnWriteArrayList<>();
            serviceRegistry.put(instance.getServiceName(), serviceInstanceInfos);
        }

        boolean changed = false;
        boolean exists = serviceInstanceInfos.stream()
                .anyMatch(a -> a.getServiceInstanceId().equals(instance.getServiceInstanceId()));
        if (!exists) {
            changed = true;
            serviceInstanceInfos.add(instance);
            serviceInstancesMap.put(instance.getServiceInstanceId(), instance);
        }

        // 注册表有变更则通知客户端
        if (changed) {
            List<ServiceChangeListener> listeners = serviceChangeListeners.get(instance.getServiceName());
            if (CollectionUtil.notEmpty(listeners)) {
                log.info("service instance changed: {}, notify client.", instance.getServiceName());
                for (ServiceChangeListener listener : listeners) {
                    listener.onChange(instance.getServiceName(), serviceInstanceInfos);
                }
            }
        }
    }

    public void heartbeat(ServiceInstanceInfo instance) {
        String serviceInstanceId = getServiceInstanceId(instance);
        ServiceInstanceInfo serviceInstanceInfo = serviceInstancesMap.get(serviceInstanceId);
        serviceInstanceInfo.setLatestHeartbeatTime(new Date().getTime());
    }

    public List<ServiceInstanceInfo> subscribe(String clientConnectionId, String serviceName) {
        List<ServiceChangeListener> changeListeners = serviceChangeListeners.get(serviceName);
        if (changeListeners == null) {
            synchronized (this) {
                if (changeListeners == null) {
                    changeListeners = new CopyOnWriteArrayList<>();
                    serviceChangeListeners.put(serviceName, changeListeners);
                }
            }
        }

        changeListeners.add(new ServiceChangeListener(clientConnectionId));
        return serviceRegistry.get(serviceName);
    }

    public static String getServiceInstanceId(ServiceInstanceInfo instance) {
        return instance.getServiceName() + "/" + instance.getInstanceIp() + "/" + instance.getInstancePort();
    }
}
