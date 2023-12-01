package org.hiraeth.govern.server.core;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.RemoteServer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/29 11:52
 */
@Slf4j
public class NetworkManager {

    /**
     * 向master机器发送自身 nodeId
     * @param socket
     * @return
     */
    public static boolean sendCurrentNodeInfo(Socket socket) {
        Configuration configuration = Configuration.getInstance();
        String nodeId = configuration.getNodeId();
        boolean isControllerCandidate = configuration.isControllerCandidate();

        RemoteServer remoteServer = new RemoteServer(nodeId, isControllerCandidate);
        ByteBuffer buffer = remoteServer.toBuffer();

        DataOutputStream outputStream = null;
        try {
            outputStream = new DataOutputStream(socket.getOutputStream());
            outputStream.writeInt(buffer.array().length);
            outputStream.write(buffer.array());
            outputStream.flush();
        }catch (IOException ex){
            log.error("send self node info to other master node failed.", ex);
            try {
                socket.close();
            }catch (IOException e){
                log.error("close socket failed when sending self node info to other master node.", e);
            }
            return false;
        }catch (Exception ex){
            log.error("send self node info to other master node failed.", ex);
        }
        return true;
    }

    public RemoteServer readRemoteNodeInfo(Socket socket) {
        try {
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());

            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.readFully(bytes);
            return RemoteServer.parseFrom(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            log.error("reading remote node id failed", e);

            try {
                socket.close();
            } catch (IOException ex) {
                log.error("closing socket failed when reading remote node id failed......", ex);
            }
        }
        return null;
    }
}
