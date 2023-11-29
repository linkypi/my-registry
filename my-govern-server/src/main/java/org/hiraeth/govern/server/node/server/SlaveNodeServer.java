package org.hiraeth.govern.server.node.server;

import org.hiraeth.govern.server.node.slave.SlaveNetworkManager;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:37
 */
public class SlaveNodeServer extends NodeServer {

    private SlaveNetworkManager slaveNetworkManager;

    public SlaveNodeServer(){
        this.slaveNetworkManager = new SlaveNetworkManager();
    }
    @Override
    public void start() {
        if(!slaveNetworkManager.connectToMasterNode()){
            return;
        }

    }
}
