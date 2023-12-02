package org.hiraeth.govern.server.core;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.server.entity.Slot;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 0:28
 */
@Slf4j
public class ClientRequestHandler {

    private RemoteNodeManager remoteNodeManager;
    private SlotManager slotManager;

    public ClientRequestHandler(RemoteNodeManager remoteNodeManager, SlotManager slotManager){
        this.remoteNodeManager = remoteNodeManager;
        this.slotManager = slotManager;
    }

    public Response handle(BaseRequest request) {
        if (request.getRequestType() == RequestType.FetchMetaData) {
            FetchMetaDataRequest fetchMetaDataRequest = BeanUtil.copyProperties(request, FetchMetaDataRequest.class);
            FetchMetaDataResponse fetchMetaDataResponse = createMetaData(fetchMetaDataRequest);
            return fetchMetaDataResponse.toResponse();
        }
        if (request.getRequestType() == RequestType.RegisterService) {
            BaseResponse response = saveServiceInstance(request);
            return response.toResponse();
        }
        if (request.getRequestType() == RequestType.Heartbeat) {
            BaseResponse response = handleHeartbeat(request);
            return response.toResponse();
        }

        return null;
    }

    private BaseResponse handleHeartbeat(BaseRequest request) {
        BaseResponse response = new BaseResponse(RequestType.Heartbeat, true);
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.parseFrom(request);

        try {

            String serviceName = heartbeatRequest.getServiceName();
            int servicePort = heartbeatRequest.getServiceInstancePort();
            String instanceIp = heartbeatRequest.getServiceInstanceIp();

            ServiceInstance serviceInstance = new ServiceInstance(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(slotNum);
            slot.heartbeat(serviceInstance);
            log.info("heartbeat service instance success: {}", JSON.toJSONString(serviceInstance));
        }catch (Exception ex){
            log.error("heartbeat service instance occur error: {}", JSON.toJSONString(heartbeatRequest), ex);
            response = new BaseResponse(RequestType.RegisterService, false);
        }
        response.setRequestId(request.getRequestId());
        return response;
    }

    private BaseResponse saveServiceInstance(BaseRequest request) {
        BaseResponse response = new BaseResponse(RequestType.RegisterService, true);
        RegisterServiceRequest registerServiceRequest = RegisterServiceRequest.parseFrom(request);
        try {

            String serviceName = registerServiceRequest.getServiceName();
            int servicePort = registerServiceRequest.getServicePort();
            String instanceIp = registerServiceRequest.getInstanceIp();

            ServiceInstance serviceInstance = new ServiceInstance(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(slotNum);
            slot.registerServiceInstance(serviceInstance);
            log.info("register service instance success: {}", JSON.toJSONString(serviceInstance));
        }catch (Exception ex){
            log.error("register service instance occur error: {}", JSON.toJSONString(registerServiceRequest), ex);
            response = new BaseResponse(RequestType.RegisterService, false);
        }
        response.setRequestId(request.getRequestId());
        return response;
    }

    private FetchMetaDataResponse createMetaData(FetchMetaDataRequest request){
        NodeStatusManager nodeStatusManager = NodeStatusManager.getInstance();
        FetchMetaDataResponse fetchMetaDataResponse = new FetchMetaDataResponse();
        fetchMetaDataResponse.setRequestId(request.getRequestId());
        fetchMetaDataResponse.setSlots(nodeStatusManager.getSlots());
        fetchMetaDataResponse.setServerAddresses(remoteNodeManager.getAllOnlineServerAddresses());
        return fetchMetaDataResponse;
    }
}
