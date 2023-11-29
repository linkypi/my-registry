package org.hiraeth.govern.server.node.master;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.master.entity.MasterRole;
import org.hiraeth.govern.server.node.master.entity.NodeAddress;
import org.hiraeth.govern.server.node.master.entity.RemoteMasterNode;
import org.hiraeth.govern.server.node.master.entity.Vote;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private int voteRound = 1;
    private Vote currentVote;

    public ControllerCandidate(MasterNetworkManager masterNetworkManager,
                               RemoteMasterNodeManager remoteMasterNodeManager) {
        this.masterNetworkManager = masterNetworkManager;
        this.remoteMasterNodeManager = remoteMasterNodeManager;
    }

    public MasterRole voteForControllerElection() {

        List<RemoteMasterNode> otherControllerCandidates = remoteMasterNodeManager.getOtherControllerCandidates();

        log.info("other controller candidate include: {}", JSON.toJSONString(otherControllerCandidates));

        Configuration configuration = Configuration.getInstance();
        int nodeId = configuration.getNodeId();
        this.currentVote = new Vote(nodeId, nodeId, voteRound);

        Integer controllerId = startNextRoundVote(otherControllerCandidates);
        // 本轮选举仍未选举出来controller 则继续下一轮选举
        while (controllerId == null) {
            controllerId = startNextRoundVote(otherControllerCandidates);
        }
        MasterRole masterRole = MasterRole.Candidate;
        if (nodeId == controllerId) {
            masterRole = MasterRole.Controller;
        }
        log.info("decide current master node's role is {}, controller node id is {}", masterRole, controllerId);
        return masterRole;
    }

    /**
     * 开启下一轮投票
     */
    private Integer startNextRoundVote(List<RemoteMasterNode> otherControllerCandidates) {

        log.info("start {} round vote.", voteRound);
        for (RemoteMasterNode remoteNode : otherControllerCandidates) {
            Integer remoteNodeId = remoteNode.getNodeId();
            masterNetworkManager.sendRequest(remoteNodeId, currentVote.toBuffer());
            log.info("send vote to remote node: {}, vote info: {}", remoteNodeId, JSON.toJSONString(currentVote));
        }

        int totalCandidates = otherControllerCandidates.size() + 1;
        // 定义 quorum 数量，如若controller候选节点有三个，则quorum = 3 / 2 + 1 = 2
        int quorum = totalCandidates / 2 + 1;

        List<Vote> votes = new ArrayList<>();
        votes.add(currentVote);
        // 在当前这轮投票中开始等待其他master的选票
        while (NodeStatusManager.isRunning()) {
            ByteBuffer buffer = masterNetworkManager.takeMessage();
            Vote vote = Vote.parseFrom(buffer);

            if (vote == null || vote.getVoteNodeId() == null) {
                continue;
            }

            // 若其他master发起的投票轮次小于当前轮次则
            if(vote.getVoteRound() < voteRound){
                log.info("ignore current receive vote, because the vote round {}" +
                        " is smaller than the current round {}", vote.getVoteRound(), voteRound);
                continue;
            }
            // 对收到的选票进行归票
            votes.add(vote);
            log.info("receive vote from remote node: {}", JSON.toJSONString(vote));

            // 若发现票数大于等于 quorum 的票数, 此时可以判定
            if (votes.size() >= quorum) {
                Integer controllerNodeId = getControllerFromVotes(votes, quorum);
                // 已经选出controller
                if (controllerNodeId != null) {
                    if (votes.size() == totalCandidates) {
                        log.info("decided controller node id: {}, received all votes: {}", controllerNodeId, JSON.toJSONString(votes));
                        return controllerNodeId;
                    }
                    log.info("decided controller node id: {}, current vote size is {}, waiting for all votes received.",
                           votes.size(), controllerNodeId);
                }else{
                    log.info("cannot decide who is controller: {}", JSON.toJSONString(votes));
                }
            }

            if (votes.size() == totalCandidates) {
                // 所有候选人的选票都以收到, 此时仍未选举出controller, 则认为该轮选举失败
                // 此时需要调整下一轮选举, 选择投票给当前候选人中nodeId最大的一位
                voteRound++;
                Integer betterControllerNodeId = getBetterControllerNodeId(votes);
                Configuration configuration = Configuration.getInstance();
                this.currentVote = new Vote(configuration.getNodeId(), betterControllerNodeId, voteRound);
                log.info("this vote round failed, try to better candidate vote: {}", JSON.toJSONString(currentVote));
                return null;
            }
        }
        return null;
    }

    /**
     * 从候选节点中获取nodeId最大的节点id
     *
     * @return
     */
    private Integer getBetterControllerNodeId(List<Vote> votes) {

        Integer controllerNodeId = 0;
        for (Vote vote : votes) {
            if (vote.getVoteForNodeId() > controllerNodeId) {
                controllerNodeId = vote.getVoteForNodeId();
            }
        }
        return controllerNodeId;
    }

    private Integer getControllerFromVotes(List<Vote> votes, int quorum) {

        Map<Integer, Integer> voteCountMap = new ConcurrentHashMap<>();

        for (Vote vote : votes) {
            Integer controllerNodeId = vote.getVoteForNodeId();
            Integer count = voteCountMap.get(controllerNodeId);
            if (count == null) {
                count = 0;
            }

            voteCountMap.put(controllerNodeId, ++count);
        }

        for (Integer remoteNodeId : voteCountMap.keySet()) {
            if (voteCountMap.get(remoteNodeId) >= quorum) {
                return remoteNodeId;
            }
        }
        return null;
    }
}
