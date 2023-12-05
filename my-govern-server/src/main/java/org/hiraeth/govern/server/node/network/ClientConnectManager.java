package org.hiraeth.govern.server.node.network;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 20:34
 */
public class ClientConnectManager {

    private ClientConnectManager() {
    }

    public static class Singleton {
        private static final ClientConnectManager instance = new ClientConnectManager();
    }
    public static ClientConnectManager getInstance(){
        return Singleton.instance;
    }
    private final Map<String,ClientConnection> clientConnections = new ConcurrentHashMap<>();

    public void add(ClientConnection connection){
        clientConnections.put(connection.getConnectionId(), connection);
    }

    public void remove(String connectionId){
        clientConnections.remove(connectionId);
    }
}
