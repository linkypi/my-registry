package org.hiraeth.govern.server.node.master;

import java.net.Socket;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 22:07
 */
public class MasterNetworkWriteThread extends Thread{
    /**
     * master节点之间的网络连接
     */
    private Socket socket;
    public MasterNetworkWriteThread(Socket socket){
        this.socket = socket;
    }
    @Override
    public void run() {

    }
}
