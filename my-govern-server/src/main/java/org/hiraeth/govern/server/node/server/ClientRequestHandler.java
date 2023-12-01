package org.hiraeth.govern.server.node.server;

import org.hiraeth.govern.common.domain.*;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.master.SlotManager;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 0:28
 */
public class ClientRequestHandler {

    private SlotManager slotManager;

    public ClientRequestHandler( SlotManager slotManager){
        this.slotManager = slotManager;
    }

    public Response handle(BaseRequest request) {
        if (request instanceof FetchSlotsRequest) {
            FetchSlotsResponse fetchSlotsResponse = handelFetchSlots((FetchSlotsRequest) request);
            return fetchSlotsResponse.toResponse();
        }

        return null;
    }

    private FetchSlotsResponse handelFetchSlots(FetchSlotsRequest request){
        NodeStatusManager nodeStatusManager = NodeStatusManager.getInstance();
        FetchSlotsResponse fetchSlotsResponse = new FetchSlotsResponse();
        fetchSlotsResponse.setRequestId(request.getRequestId());
        fetchSlotsResponse.setSlots(nodeStatusManager.getSlots());
        return fetchSlotsResponse;
    }
}
