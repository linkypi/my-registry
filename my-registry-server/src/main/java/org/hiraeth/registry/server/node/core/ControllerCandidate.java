package org.hiraeth.registry.server.node.core;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.snowflake.SnowFlakeIdUtil;
import org.hiraeth.registry.server.config.Configuration;
import org.hiraeth.registry.server.entity.request.ElectionResult;
import org.hiraeth.registry.server.entity.request.ElectionResultAck;
import org.hiraeth.registry.server.entity.request.RequestMessage;
import org.hiraeth.registry.server.entity.request.Vote;
import org.hiraeth.registry.server.node.network.ServerNetworkManager;
import org.hiraeth.registry.server.entity.RemoteServer;
import org.hiraeth.registry.server.entity.ServerRequestType;

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

    public static class Singleton {
        private static final ControllerCandidate instance = new ControllerCandidate();
    }
    public static ControllerCandidate getInstance(){
        return Singleton.instance;
    }

    /**
     * 投票轮次
     */
    private int voteRound;
    private Vote currentVote;

    // 当前选票集合
    private List<Vote> votes = new ArrayList<>();

    private volatile ElectionResult electionResult;

    /**
     * 投票完成后放行
     */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 投票结果确认结果
     */
    private volatile HashSet<String> confirmList = new HashSet<>();

    private ControllerCandidate() {
        new ElectionCompleteHandlerThread().start();
    }

    public ElectionResult electController() {
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();

        log.debug("other controller candidate include: {}", JSON.toJSONString(otherControllerCandidates));
        log.info("start election controller...");

        Configuration configuration = Configuration.getInstance();
        String currentNodeId = configuration.getNodeId();
        this.currentVote = new Vote(voteRound, currentNodeId);

        ElectionResult eleResult = startElection();
        notifyOtherCandidates(eleResult.getControllerId());

        try {
            countDownLatch.await();
        } catch (Exception ex) {
            log.info("count down latch occur error", ex);
        }
        return electionResult;
    }

    /**
     * leader 选举完成后通知其他节点
     */
    private void notifyOtherCandidates(String controllerId) {
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        ElectionResult electionResult = ElectionResult.newElectingResult(controllerId, voteRound);
        electionResult.buildBuffer();

        for (RemoteServer remoteServer : otherControllerCandidates) {
            ServerNetworkManager.getInstance().sendRequest(remoteServer.getNodeId(), electionResult);
        }
        log.info("notified election result: {}", JSON.toJSONString(electionResult));
    }

    /**
     * 发起新一轮投票
     *
     * @param targetId 目标controller节点
     */
    private void startNewVoteRound(String targetId) {
        voteRound ++;
        if (targetId != null) {
            log.info("start voting round {}, target controller id: {}.", voteRound, targetId);
        } else {
            log.info("start voting round {}.", voteRound);
        }

        String currentNodeId = Configuration.getInstance().getNodeId();
        // targetId = null 表示首轮, 仅投给自己
        if (targetId == null) {
            targetId = currentNodeId;
        }
        currentVote.setRound(voteRound);
        currentVote.setTargetNodeId(targetId);
        currentVote.setBuffer(null);
        currentVote.buildBuffer();

        votes.clear();
        // 首先给自己投一票
        votes.add(currentVote);

        // 在首轮投票中当前节点向其他节点拉票, 希望其他节点都投自己
        // 若在本轮投票仍未出现结果, 则发起新一轮投票, 投票的节点是当前所有选票节点中nodeId最大的一个
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();

        for (RemoteServer remoteServer : otherControllerCandidates) {
            String remoteNodeId = remoteServer.getNodeId();
            currentVote.setFromNodeId(currentNodeId);
            currentVote.setToNodeId(remoteNodeId);
            currentVote.setRequestId(SnowFlakeIdUtil.getNextId());
            ServerNetworkManager.getInstance().sendRequest(remoteNodeId, currentVote);
            log.info("send vote to remote node: {}, vote round: {}", remoteNodeId, currentVote.getRound());
        }
    }

    /**
     * 开启下一轮投票
     */
    private ElectionResult startElection() {

        startNewVoteRound(null);
        ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
        // 为防止无限循环 通过 ElectionStage 控制
        // 因为很有可能当前节点因为网络问题没有收到投票, 导致在此循环等待
        while (NodeInfoManager.isRunning() && ElectionStage.getStatus() == ElectionStage.ELStage.ELECTING) {
            try {
                Thread.sleep(300);
                if (messageQueue.countRequestMessage(ServerRequestType.Vote) > 0) {
                    RequestMessage messageBase = messageQueue.takeRequestMessage(ServerRequestType.Vote);
                    Vote vote = Vote.parseFrom(messageBase);
                    String leaderId = handleVoteResponse(vote);
                    if (leaderId != null) {
                        electionResult = ElectionResult.newElectingResult(leaderId, voteRound);
                        break;
                    }
                }
            } catch (Exception ex) {
                log.info("handle election message failed", ex);
            }
        }
        return electionResult;
    }

    class ElectionCompleteHandlerThread extends Thread {

        public ElectionCompleteHandlerThread(){
            this.setName("ElectionCompleteHandlerThread");
        }
        @Override
        public void run() {
            try {
                ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
                while (NodeInfoManager.isRunning()) {
                    if (messageQueue.countRequestMessage(ServerRequestType.ElectionComplete) > 0) {
                        handleElectionResult();
                    }

                    if (messageQueue.countRequestMessage(ServerRequestType.ElectionCompleteAck) > 0) {
                        if(ackElectionResult()){
                            break;
                        }
                    }

                    if (messageQueue.countRequestMessage(ServerRequestType.Leading) > 0) {
                        if(handleLeadingResult()){
                            break;
                        }
                    }

                    Thread.sleep(1000L);
                }
            } catch (Exception ex) {
                log.error("handle response failed", ex);
            }
        }

        private boolean handleLeadingResult() {
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            RequestMessage messageBase = messageQueue.takeRequestMessage(ServerRequestType.Leading);
            log.info("receive Leading message !!! {}", JSON.toJSONString(messageBase));
            if (electionResult != null && !Objects.equals(electionResult.getControllerId(), messageBase.getControllerId())) {
                log.error("receive Leading message, but the controller id is not the same, current election result: {}, " +
                        "remote election result: {}", JSON.toJSONString(electionResult), JSON.toJSONString(messageBase));
            }
            finishedVoting();
            return true;
        }

        private boolean handleElectionResult() {
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            RequestMessage messageBase = messageQueue.takeRequestMessage(ServerRequestType.ElectionComplete);
            ElectionResult remoteEleResult = ElectionResult.parseFrom(messageBase);
            log.info("election result notification: {}. ", JSON.toJSONString(remoteEleResult));

            String currentNoteId = Configuration.getInstance().getNodeId();
            // 当前机器没有投票结果, 可直接接收远程投票结果
            if (electionResult == null) {
                replyAcceptResult(remoteEleResult.getFromNodeId(), remoteEleResult);
            }

            // 若投票结果相同, 并且当前节点不是leader节点则接收该投票结果
            if (Objects.equals(remoteEleResult.getControllerId(), electionResult.getControllerId())) {
                // 若当前节点不是结果中的leader节点则接收该投票结果, 否则拒绝
                if (!Objects.equals(remoteEleResult.getControllerId(), currentNoteId)) {
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
            return false;
        }

        private boolean ackElectionResult() {
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            RequestMessage messageBase = messageQueue.takeRequestMessage(ServerRequestType.ElectionCompleteAck);
            ElectionResultAck remoteResultAck = ElectionResultAck.parseFrom(messageBase);
            if (ElectionResultAck.AckResult.Accepted.getValue() == remoteResultAck.getResult()) {
                confirmList.add(remoteResultAck.getFromNodeId());
            }
            if (ElectionResultAck.AckResult.Rejected.getValue() == remoteResultAck.getResult()) {
                log.info("use remote node {} election result: {}.", remoteResultAck.getFromNodeId(), JSON.toJSONString(remoteResultAck));
                electionResult.setControllerId(remoteResultAck.getControllerId());
                electionResult.setEpoch(remoteResultAck.getEpoch());

                // 发送确认, 以便 confirmList 汇总结果
                String controllerId = remoteResultAck.getControllerId();
                int epoch = remoteResultAck.getEpoch();
                replyAcceptResult(remoteResultAck.getFromNodeId(), ElectionResult.newElectingResult(controllerId, epoch));
            }
            // 大多数节点已确认选举结果, 进入领导阶段
            String nodeId = Configuration.getInstance().getNodeId();
            RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
            if (Objects.equals(nodeId, electionResult.getControllerId()) && confirmList.size() >= remoteNodeManager.getQuorum()) {
                log.info("quorum controller nodes has confirmed current election result: {}.", JSON.toJSONString(electionResult));

                // 昭告天下, 全国已确认解放, 朕已登基
                String controllerId = remoteResultAck.getControllerId();
                int epoch = remoteResultAck.getEpoch();

                RequestMessage message = new RequestMessage();
                message.setRequestType(ServerRequestType.Leading.getValue());
                message.setControllerId(controllerId);
                message.setEpoch(epoch);
                message.buildBuffer();

                for (RemoteServer remoteServer : remoteNodeManager.getOtherControllerCandidates()) {
                    ServerNetworkManager.getInstance().sendRequest(remoteServer.getNodeId(), message);
                }
                log.info("election has finished, all the other controller nodes has been notified.");

                finishedVoting();
                return true;
            }
            return false;
        }

        /**
         * 回复拒绝当前投票结果
         *
         * @param remoteEleResult
         */
        private void replyRejectedResult(ElectionResult remoteEleResult) {
            ElectionResultAck completeAck = ElectionResultAck.newReject(electionResult.getControllerId(), electionResult.getEpoch());
            completeAck.buildBuffer();

            log.info("rejected remote election result: {}, because current election result be better: {}",
                    JSON.toJSONString(remoteEleResult), JSON.toJSONString(electionResult));
            // 发送 ACK 给 leader , 确保其他非leader节点都已收到
            ServerNetworkManager.getInstance().sendRequest(remoteEleResult.getFromNodeId(), completeAck);
        }

        /**
         * 回复接受当前投票结果
         *
         * @param remoteNodeId
         * @param remoteEleResult
         */
        private void replyAcceptResult(String remoteNodeId, ElectionResult remoteEleResult) {
            electionResult = remoteEleResult;
            String controllerId = remoteEleResult.getControllerId();
            int epoch = remoteEleResult.getEpoch();

            ElectionResultAck completeAck = ElectionResultAck.newAccept(controllerId, epoch);
            completeAck.buildBuffer();

            log.info("accepted remote election result: {}", JSON.toJSONString(remoteEleResult));
            // 发送 ACK 给 leader , 确保其他非leader节点都已收到
            ServerNetworkManager.getInstance().sendRequest(remoteNodeId, completeAck);
        }

        private void finishedVoting() {
            votes.clear();
            confirmList.clear();
            countDownLatch.countDown();
            // 必须设置 Leading 状态, 因为存在 while 循环 会导致某些环节无法继续
            ElectionStage.setStatus(ElectionStage.ELStage.LEADING);
            log.info("election has finished, controller id {} is elected !!!", electionResult.getControllerId());
        }
    }

    private String handleVoteResponse(Vote vote) {

        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        int totalCandidates = remoteNodeManager.getTotalCandidate();
        // 定义 quorum 数量，如若controller候选节点有三个，则quorum = 3 / 2 + 1 = 2
        int quorum = remoteNodeManager.getQuorum();
//        log.info("receive vote from remote node: {}", JSON.toJSONString(vote));

        if (vote.getFromNodeId() == null) {
            return null;
        }

        // 判断是否有来自同一个节点的多次投票, 若存在则保留轮次较大的一次投票
        Optional<Vote> existVote = votes.stream().filter(a -> Objects.equals(a.getFromNodeId(), vote.getFromNodeId())).findFirst();
        if (existVote.isPresent()) {
            Vote vote1 = existVote.get();

            log.info("the same controller node {} voting exist, select the large round. old round {}," +
                    " old target: {}, new round: {}, new target: {}", vote.getFromNodeId(), vote1.getRound(),
                    vote1.getTargetNodeId(), vote.getRound(), vote.getTargetNodeId());
            vote1.setRound(vote.getRound());
            vote1.setTargetNodeId(vote1.getTargetNodeId());
        } else {
            // 对收到的选票进行归票
            votes.add(vote);
        }

        // 若发现票数大于等于 quorum 的票数, 此时可以判定
        if (votes.size() >= quorum) {
            String controllerNodeId = detectControllerIdFromVotes(votes, quorum);
            // 已经选出controller
            if (controllerNodeId != null) {
                if (votes.size() == totalCandidates) {
                    log.info("candidate controller node id: {} !!! ", controllerNodeId);
                    return controllerNodeId;
                }
                log.info("candidate controller node id: {}, waiting for all votes received: {}.",
                        controllerNodeId,  JSON.toJSONString(votes));
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
        String betterControllerNodeId = getBetterControllerNodeId(votes);
        this.currentVote = new Vote(voteRound, betterControllerNodeId);
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
    private String getBetterControllerNodeId(List<Vote> votes) {

        Set<String> list = new HashSet<>();
        for (Vote vote : votes) {
            list.add(vote.getTargetNodeId());
        }
        return Configuration.getInstance().getBetterControllerAddress(list);
    }

    /**
     * 从投票结果中获取大多数选票，多数者胜出
     * @param votes
     * @param quorum
     * @return
     */
    private String detectControllerIdFromVotes(List<Vote> votes, int quorum) {

        Map<String, Integer> voteCountMap = new ConcurrentHashMap<>();

        for (Vote vote : votes) {
            String controllerNodeId = vote.getTargetNodeId();
            Integer count = voteCountMap.get(controllerNodeId);
            if (count == null) {
                count = 0;
            }
            voteCountMap.put(controllerNodeId, ++count);
        }

        for (String remoteNodeId : voteCountMap.keySet()) {
            if (voteCountMap.get(remoteNodeId) >= quorum) {
                return remoteNodeId;
            }
        }
        return null;
    }
}
