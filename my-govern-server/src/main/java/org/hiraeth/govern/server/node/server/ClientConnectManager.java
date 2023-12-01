package org.hiraeth.govern.server.node.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 20:34
 */
public class ClientConnectManager {
    private final Map<String,ClientConnection> clientConnections = new ConcurrentHashMap<>();

    public void add(ClientConnection connection){
        clientConnections.put(connection.getConnectionId(), connection);
    }

    public void remove(String connectionId){
        clientConnections.remove(connectionId);
    }
}
