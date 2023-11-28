package org.hiraeth.govern.server.node.master;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.node.MasterRole;
import org.hiraeth.govern.server.node.master.entity.RemoteMasterNode;

import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:49
 */
@Getter
@Setter
@Slf4j
public class ControllerCandidate {
    /**
     * 集群内部节点之间进行网络通信的组件
     */
    private MasterNetworkManager masterNetworkManager;
    private RemoteMasterNodeManager remoteMasterNodeManager;
    /**
     * 投票轮次
     */
    private int voteRound;

    public ControllerCandidate(MasterNetworkManager masterNetworkManager,
                               RemoteMasterNodeManager remoteMasterNodeManager){
        this.masterNetworkManager = masterNetworkManager;
        this.remoteMasterNodeManager = remoteMasterNodeManager;
    }

    public MasterRole voteForControllerElection(){

        List<RemoteMasterNode> otherControllerCandidates = remoteMasterNodeManager.getOtherControllerCandidates();

        for (RemoteMasterNode remoteMasterNode: otherControllerCandidates) {

        }

        return MasterRole.Controller;
    }

    /**
     * 开启下一轮投票
     */
    private void startNextRoundVote(){
        voteRound ++;


    }
}
