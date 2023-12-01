package org.hiraeth.govern.server.node.server;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 20:34
 */
public class ClientConnectManager {
    private final List<ClientConnection> clientConnections = new ArrayList<>();

    public void add(ClientConnection connection){
        clientConnections.add(connection);
    }
}
