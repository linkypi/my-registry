package com.hiraeth.govern.client;

import com.beust.jcommander.JCommander;
import com.hiraeth.govern.client.config.Configuration;
import com.hiraeth.govern.client.entity.MasterAddress;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.BaseResponse;
import org.hiraeth.govern.common.domain.FetchSlotsRequest;
import org.hiraeth.govern.common.domain.Request;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 18:36
 */
@Slf4j
public class ClientServer {

    // io 多路复用组件
    private Selector selector;
    // 客户端与服务端的selectionKey
    private SelectionKey selectionKey;

    private Map<String, LinkedBlockingQueue<Request>> requestQueue = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, BaseResponse> responses = new ConcurrentHashMap<>();
    private ServerConnectionManager serverConnectionManager;

    public ClientServer() {
        try {
            selector = Selector.open();
            serverConnectionManager = new ServerConnectionManager();

            new IOThread(selector, requestQueue).start();
        }catch (IOException ex){
            log.error("start nio selector error", ex);
        }catch (Exception ex){
            log.error("start nio selector error", ex);
        }
    }

    /**
     * 随机选择一个controller候选节点, 与其建立长连接
     * 发送请求到controller候选节点获取slots数据
     */
    public void init() throws IOException {
        MasterAddress masterAddress = chooseMasterServer();
        String connectionId = connectServer(masterAddress);
        fetchSlotsFromServer(connectionId);
    }

    private void fetchSlotsFromServer(String connectionId){
        FetchSlotsRequest fetchSlotsRequest = new FetchSlotsRequest();
        requestQueue.get(connectionId).add(fetchSlotsRequest.toRequest());

        while (responses.get(fetchSlotsRequest.getRequestId())!=null){
            try {
                Thread.sleep(500);
            }catch (Exception ex){
            }
        }
    }

    private String connectServer(MasterAddress address) throws IOException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(address.getHost(), address.getPort());
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.socket().setSoLinger(false, -1);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setReuseAddress(true);
        selectionKey = channel.register(selector, SelectionKey.OP_CONNECT);

       channel.connect(inetSocketAddress);

       log.info("try to connect server: {}", address);

       return waitConnect();
    }

    private String waitConnect() throws IOException {
        boolean finishedConnect = false;
        while (!finishedConnect){
            selector.select();
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            if(selectionKeys.isEmpty()){
                continue;
            }
            for (SelectionKey selectionKey: selectionKeys){
                if((selectionKey.readyOps() & SelectionKey.OP_CONNECT) !=0 ){
                    SocketChannel socketChannel = (SocketChannel)selectionKey.channel();
                    if(socketChannel.finishConnect()){
                        finishedConnect = true;
                        selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                        ServerConnection serverConnection = new ServerConnection(selectionKey, socketChannel);
                        serverConnectionManager.add(serverConnection);

                        selectionKey.attach(serverConnection);
                        requestQueue.put(serverConnection.getConnectionId(), new LinkedBlockingQueue<>());

                        log.info("established connection with server, connection id: {}", serverConnection.getConnectionId());
                        return serverConnection.getConnectionId();
                    }
                }
            }
        }
        return null;
    }

    private static final Random random = new Random();
    private MasterAddress chooseMasterServer() {
        Configuration configuration = Configuration.getInstance();
        List<MasterAddress> masterServers = configuration.getMasterServers();
        int index = random.nextInt(masterServers.size());
        MasterAddress masterAddress = masterServers.get(index);
        log.info("choose master server: {}", masterAddress);
        return masterAddress;
    }

    /**
     * 根据服务名称路由到一个slot槽位, 找到slot槽位所在master节点
     * 跟指定master节点建立长连接, 然后发送请求到master节点进行服务注册
     */
    public void register(){

    }

    public void startHeartbeatSchedule(){

    }
}
