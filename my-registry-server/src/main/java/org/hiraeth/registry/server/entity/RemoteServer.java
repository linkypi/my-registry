package org.hiraeth.registry.server.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.ServerAddress;
import org.hiraeth.registry.common.util.CommonUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 18:40
 */
@Getter
@Setter
public class RemoteServer {
    // 由 IP + internalPort 组成
    private String nodeId;

    /**
     * 是否为controller候选节点
     */
    private boolean isControllerCandidate;

    private String host;
    private int clientHttpPort;
    private int clientTcpPort;
    private int internalPort;

    public RemoteServer(){}

    public RemoteServer(ServerAddress serverAddress, boolean isCandidate) {
        this.nodeId = serverAddress.getNodeId();
        this.isControllerCandidate = isCandidate;
        this.clientTcpPort = serverAddress.getClientTcpPort();
        this.clientHttpPort = serverAddress.getClientHttpPort();
        this.internalPort = serverAddress.getInternalPort();
        this.host = serverAddress.getHost();
    }

    public ByteBuffer toBuffer() {

        int length = CommonUtil.getStrLength(nodeId) + CommonUtil.getStrLength(host);
        ByteBuffer buffer = ByteBuffer.allocate(28 + length);
        buffer.putInt(ServerRequestType.NodeInfo.getValue());
        buffer.putInt(isControllerCandidate ? 1 : 0);
        buffer.putInt(clientHttpPort);
        buffer.putInt(clientTcpPort);
        buffer.putInt(internalPort);
        CommonUtil.writeStr(buffer, nodeId);
        CommonUtil.writeStr(buffer, host);
        return buffer;
    }

    public static RemoteServer parseFrom(ByteBuffer buffer) {
        int messageType = buffer.getInt();
        if (ServerRequestType.NodeInfo.getValue() == messageType) {
            int isCandidate = buffer.getInt();
            int clientHttpPort = buffer.getInt();
            int clientTcpPort = buffer.getInt();
            int internPort = buffer.getInt();
            String nodeId = CommonUtil.readStr(buffer);
            String host = CommonUtil.readStr(buffer);

            RemoteServer remoteServer = new RemoteServer();
            remoteServer.setHost(host);
            remoteServer.setNodeId(nodeId);
            remoteServer.setControllerCandidate(isCandidate == 1);
            remoteServer.setClientHttpPort(clientHttpPort);
            remoteServer.setClientTcpPort(clientTcpPort);
            remoteServer.setInternalPort(internPort);
            return remoteServer;
        }
        return null;
    }
}
