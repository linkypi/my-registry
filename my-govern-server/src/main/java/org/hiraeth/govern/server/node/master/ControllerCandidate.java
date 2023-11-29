package org.hiraeth.govern.server.node.master;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.ElectionStage;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.entity.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;


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
    private RemoteNodeManager remoteNodeManager;
    /**
     * 投票轮次
     */
    private int voteRound;
    private Vote currentVote;

    // 当前选票集合
    private List<Vote> votes = new ArrayList<>();

    private volatile ElectionResult electionResult;

    private CountDownLatch countDownLatch;

    public ControllerCandidate(MasterNetworkManager masterNetworkManager,
                               RemoteNodeManager remoteNodeManager) {
        this.masterNetworkManager = masterNetworkManager;
        this.remoteNodeManager = remoteNodeManager;
    }

    public MasterRole voteForControllerElection() {

        List<RemoteNode> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();

        log.debug("other controller candidate include: {}", JSON.toJSONString(otherControllerCandidates));

        log.info("start election controller...");

        Configuration configuration = Configuration.getInstance();
        int currentNodeId = configuration.getNodeId();
        this.currentVote = new Vote(1, currentNodeId, currentNodeId);

        // 本轮选举仍未选举出来controller 则继续下一轮选举
        startElection();

        int leaderId = electionResult.getControllerId();
        MasterRole masterRole = MasterRole.Candidate;
        if (currentNodeId == leaderId) {
            masterRole = MasterRole.Controller;
            log.info("current node is leader !!!");
        }

        notifyOtherCandidates(leaderId);

        try {
            countDownLatch.await();
        } catch (Exception ex) {
            log.info("count down latch occur error", ex);
        }

        log.info("decide current master node's role is {}, controller node id is {}", masterRole, leaderId);
        return masterRole;
    }

    /**
     * leader 选举完成后通知其他节点
     */
    public void notifyOtherCandidates(int controllerId) {
        List<RemoteNode> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        ElectionResult electionResult = ElectionResult.newCandidateResult(controllerId, voteRound);
        for (RemoteNode remoteNode : otherControllerCandidates) {
            masterNetworkManager.sendRequest(remoteNode.getNodeId(), electionResult.toBuffer());
        }
    }

    /**
     * 发起新一轮投票
     *
     * @param targetId 目标controller节点
     */
    private void startNewVoteRound(Integer targetId) {
        countDownLatch = new CountDownLatch(1);
        voteRound++;
        if (targetId != null) {
            log.info("start voting round {}, target controller id: {}.", voteRound, targetId);
        } else {
            log.info("start voting round {}.", voteRound);
        }

        Integer currentNodeId = Configuration.getInstance().getNodeId();
        // targetId = null 表示首轮, 仅投给自己
        if (targetId == null) {
            targetId = currentNodeId;
        }
        currentVote.setRound(voteRound);
        currentVote.setTargetNodeId(targetId);

        votes.clear();
        // 首先给自己投一票
        votes.add(currentVote);

        // 在首轮投票中当前节点向其他节点拉票, 希望其他节点都投自己
        // 若在本轮投票仍未出现结果, 则发起新一轮投票, 投票的节点是当前所有选票节点中nodeId最大的一个
        List<RemoteNode> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();

        for (RemoteNode remoteNode : otherControllerCandidates) {
            currentVote.setFromNodeId(currentNodeId);
            Integer remoteNodeId = remoteNode.getNodeId();
            masterNetworkManager.sendRequest(remoteNodeId, currentVote.toBuffer());
            log.info("send vote to remote node: {}, vote info: {}", remoteNodeId, JSON.toJSONString(currentVote));
        }
    }

    /**
     * 开启下一轮投票
     */
    private ElectionResult startElection() {

        new ElectionCompleteHandlerThread().start();

        startNewVoteRound(null);

        while (NodeStatusManager.isRunning() && ElectionStage.getStatus() == ElectionStage.ELStage.ELECTING) {
            try {
                if (masterNetworkManager.countResponseMessage(RequestType.Vote) > 0) {
                    ByteBuffer buffer = masterNetworkManager.takeResponseMessage(RequestType.Vote);
                    Integer leaderId = handleVoteResponse(buffer);
                    if (leaderId != null) {
                        electionResult = ElectionResult.newCandidateResult(leaderId, voteRound);
                        return electionResult;
                    }
                }
            } catch (Exception ex) {
                log.info("handle election message failed", ex);
            }
        }
        return null;
    }

    private volatile HashSet<Integer> confirmList = new HashSet<>();

    class ElectionCompleteHandlerThread extends Thread {
        @Override
        public void run() {
            try {
                while (NodeStatusManager.isRunning()) {
                    if (masterNetworkManager.countResponseMessage(RequestType.ElectionComplete) > 0) {

                        ByteBuffer buffer = masterNetworkManager.takeResponseMessage(RequestType.ElectionComplete);
                        ElectionResult remoteEleResult = ElectionResult.parseFrom(buffer);

                        log.info("election result notification: {}. ", JSON.toJSONString(remoteEleResult));

                        int currentNoteId = Configuration.getInstance().getNodeId();
                        if (ElectionStage.ELStage.LEADING.getValue() == remoteEleResult.getStage()) {
                            // 选举完成
                            log.info("the controller id {} is elected by voting !!!", remoteEleResult.getControllerId());
                            finishedVoting();
                            break;
                        }

                        // 当前机器没有投票结果, 可直接接收远程投票结果
                        if (electionResult == null) {
                            // accept
                            replyAcceptResult(remoteEleResult.getFromNodeId(), remoteEleResult);
                        }

                        // 若投票结果相同, 并且当前节点不是leader节点则接收该投票结果
                        if (remoteEleResult.getControllerId() == electionResult.getControllerId()) {
                            // 若当前节点不是结果中的leader节点则接收该投票结果, 否则拒绝
                            if (remoteEleResult.getControllerId() != currentNoteId) {
                                replyAcceptResult(remoteEleResult.getFromNodeId(), remoteEleResult);
                            } else {
                                replyRejectedResult(remoteEleResult);
                            }
                        }

                        // 若投票结果不同, 则取最先产生的结果 并且 当前节点不是leader节点 方才接收该投票结果
                        if (remoteEleResult.getTimestamp() <= electionResult.getTimestamp()
                                && remoteEleResult.getControllerId() != currentNoteId) {
                            replyAcceptResult(remoteEleResult.getFromNodeId(), remoteEleResult);
                        } else {
                            replyRejectedResult(remoteEleResult);
                        }
                    }

                    if (masterNetworkManager.countResponseMessage(RequestType.ElectionCompleteAck) > 0) {
                        ByteBuffer buffer = masterNetworkManager.takeResponseMessage(RequestType.ElectionCompleteAck);
                        ElectionResultAck remoteResultAck = ElectionResultAck.parseFrom(buffer);
                        if (ElectionResultAck.AckResult.Accepted.getValue() == remoteResultAck.getResult()) {
                            confirmList.add(remoteResultAck.getFromNodeId());
                        }
                        if (ElectionResultAck.AckResult.Rejected.getValue() == remoteResultAck.getResult()) {
                            log.info("use remote node {} election result: {}.", remoteResultAck.getFromNodeId(), JSON.toJSONString(remoteResultAck));
                            electionResult.setControllerId(remoteResultAck.getControllerId());
                            electionResult.setEpoch(remoteResultAck.getEpoch());

                            // 发送确认, 以便 confirmList 汇总结果
                            int controllerId = remoteResultAck.getControllerId();
                            int epoch = remoteResultAck.getEpoch();
                            replyAcceptResult(remoteResultAck.getFromNodeId(), ElectionResult.newCandidateResult(controllerId, epoch));
                        }
                        // 选举结束, 进入领导阶段
                        if (confirmList.size() >= remoteNodeManager.getQuorum()) {
                            log.info("quorum master nodes has confirmed current election result: {}.", JSON.toJSONString(electionResult));
                            ElectionStage.setStatus(ElectionStage.ELStage.LEADING);

                            // 昭告天下, 全国已确认解放, 朕已登记
                            int controllerId = remoteResultAck.getControllerId();
                            int epoch = remoteResultAck.getEpoch();
                            ElectionResult result = ElectionResult.newLeadingResult(controllerId, epoch);
                            for (RemoteNode remoteNode : remoteNodeManager.getOtherControllerCandidates()) {
                                masterNetworkManager.sendRequest(remoteNode.getNodeId(), result.toBuffer());
                            }

                            finishedVoting();

                            log.info("election is finished, all the other master nodes has been notified .");
                            break;
                        }
                    }

                    Thread.sleep(1000L);
                }
            } catch (Exception ex) {
                log.error("handle response failed", ex);
            }
        }

        /**
         * 回复拒绝当前投票结果
         *
         * @param remoteEleResult
         */
        private void replyRejectedResult(ElectionResult remoteEleResult) {
            int currentNoteId = Configuration.getInstance().getNodeId();
            ElectionResultAck completeAck = ElectionResultAck.newReject(electionResult.getControllerId(), electionResult.getEpoch(), currentNoteId);
            log.info("rejected remote election result: {}, because current election result be better: {}",
                    JSON.toJSONString(remoteEleResult), JSON.toJSONString(electionResult));
            // 发送 ACK 给 leader , 确保其他非leader节点都已收到
            masterNetworkManager.sendRequest(remoteEleResult.getFromNodeId(), completeAck.toBuffer());
        }

        /**
         * 回复接受当前投票结果
         *
         * @param remoteNodeId
         * @param remoteEleResult
         */
        private void replyAcceptResult(int remoteNodeId, ElectionResult remoteEleResult) {
            int currentNoteId = Configuration.getInstance().getNodeId();
            electionResult = remoteEleResult;
            int controllerId = remoteEleResult.getControllerId();
            int epoch = remoteEleResult.getEpoch();
            ElectionResultAck completeAck = ElectionResultAck.newAccept(controllerId, epoch, currentNoteId);
            log.info("accepted remote election result: {}", JSON.toJSONString(remoteEleResult));
            // 发送 ACK 给 leader , 确保其他非leader节点都已收到
            masterNetworkManager.sendRequest(remoteNodeId, completeAck.toBuffer());
        }

        private void finishedVoting() {
            votes.clear();
            confirmList.clear();
            countDownLatch.countDown();
        }
    }

    private Integer handleVoteResponse(ByteBuffer buffer) {

        List<RemoteNode> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        int totalCandidates = otherControllerCandidates.size() + 1;
        // 定义 quorum 数量，如若controller候选节点有三个，则quorum = 3 / 2 + 1 = 2
        int quorum = remoteNodeManager.getQuorum();

        Vote vote = Vote.parseFrom(buffer);
        log.info("receive vote from remote node: {}", JSON.toJSONString(vote));

        if (vote.getFromNodeId() == null) {
            return null;
        }

        // 判断是否存在相同节点的投票, 若存在则保留轮次较大的一次投票
        Optional<Vote> existVote = votes.stream().filter(a -> Objects.equals(a.getFromNodeId(), vote.getFromNodeId())).findFirst();
        if (existVote.isPresent()) {
            log.info("the same master node {} voting exist, select the large round: {}", vote.getFromNodeId(), JSON.toJSONString(vote));
            existVote.get().setRound(vote.getRound());
        } else {
            // 对收到的选票进行归票
            votes.add(vote);
        }

        // 若发现票数大于等于 quorum 的票数, 此时可以判定
        if (votes.size() >= quorum) {
            Integer controllerNodeId = getControllerFromVotes(votes, quorum);
            // 已经选出controller
            if (controllerNodeId != null) {
                if (votes.size() == totalCandidates) {
                    log.info("election completed, controller node id: {} !!! received all votes: {}",
                            controllerNodeId, JSON.toJSONString(votes));
                    return controllerNodeId;
                }
                log.info("decided controller node id: {}, current vote size is {}, waiting for all votes received.",
                        votes.size(), controllerNodeId);
            } else {
                log.info("cannot decide who is controller: {}", JSON.toJSONString(votes));
            }
        }

        // 若仍未选出leader, 且票数已满则发起新一轮选举
        if (votes.size() == totalCandidates) {
            restartVoteRound();
        }
        return null;
    }

    /**
     * 重启新一轮投票
     */
    private void restartVoteRound() {
        // 所有候选人的选票都以收到, 此时仍未选举出controller, 则认为该轮选举失败
        // 此时需要调整下一轮选举, 选择投票给当前候选人中nodeId最大的一位
        Integer betterControllerNodeId = getBetterControllerNodeId(votes);
        Configuration configuration = Configuration.getInstance();
        this.currentVote = new Vote(voteRound, configuration.getNodeId(), betterControllerNodeId);
        log.info("this vote round failed, try to better candidate vote: {}", JSON.toJSONString(currentVote));

        try {
            Thread.sleep(200);
        } catch (Exception ignored) {
        }
        // 开始新一轮投票
        startNewVoteRound(betterControllerNodeId);
    }

    /**
     * 从候选节点中获取nodeId最大的节点id
     *
     * @return
     */
    private Integer getBetterControllerNodeId(List<Vote> votes) {

        Integer controllerNodeId = 0;
        for (Vote vote : votes) {
            if (vote.getTargetNodeId() > controllerNodeId) {
                controllerNodeId = vote.getTargetNodeId();
            }
        }
        return controllerNodeId;
    }

    private Integer getControllerFromVotes(List<Vote> votes, int quorum) {

        Map<Integer, Integer> voteCountMap = new ConcurrentHashMap<>();

        for (Vote vote : votes) {
            Integer controllerNodeId = vote.getTargetNodeId();
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
