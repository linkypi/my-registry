package org.hiraeth.govern.server.node.server;

import cn.hutool.core.bean.BeanUtil;
import org.hiraeth.govern.common.domain.*;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.master.RemoteNodeManager;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 0:28
 */
public class ClientRequestHandler {

    private RemoteNodeManager remoteNodeManager;

    public ClientRequestHandler(RemoteNodeManager remoteNodeManager){
        this.remoteNodeManager = remoteNodeManager;
    }

    public Response handle(BaseRequest request) {
        if (request.getRequestType() == RequestType.FetchMetaData) {
            FetchMetaDataRequest fetchMetaDataRequest = BeanUtil.copyProperties(request, FetchMetaDataRequest.class);
            FetchMetaDataResponse fetchMetaDataResponse = createMetaData(fetchMetaDataRequest);
            return fetchMetaDataResponse.toResponse();
        }

        return null;
    }

    private FetchMetaDataResponse createMetaData(FetchMetaDataRequest request){
        NodeStatusManager nodeStatusManager = NodeStatusManager.getInstance();
        FetchMetaDataResponse fetchMetaDataResponse = new FetchMetaDataResponse();
        fetchMetaDataResponse.setRequestId(request.getRequestId());
        fetchMetaDataResponse.setSlots(nodeStatusManager.getSlots());
        fetchMetaDataResponse.setMasterAddresses(remoteNodeManager.getAllOnlineMasterAddresses());
        return fetchMetaDataResponse;
    }
}
