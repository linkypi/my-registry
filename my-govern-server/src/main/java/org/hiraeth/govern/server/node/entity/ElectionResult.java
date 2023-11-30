package org.hiraeth.govern.server.node.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.node.master.ElectionStage;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/29 14:02
 */
@Slf4j
@Getter
@Setter
public class ElectionResult extends MessageBase{

    // 角色, 无需远程传输
    private MasterRole masterRole;

    public ElectionResult() {
        super();
        this.messageType = MessageType.ElectionComplete;
    }

    public Message toMessage(){
        ByteBuffer buffer = toBuffer();
        return new Message(messageType, buffer.array());
    }
    public ElectionResult(int controllerId, int epoch) {
        super();
        this.messageType = MessageType.ElectionComplete;
        this.controllerId = controllerId;
        this.epoch = epoch;
    }

    public static ElectionResult newElectingResult(int controllerId, int epoch){
        ElectionResult electionResult = new ElectionResult(controllerId, epoch);
        electionResult.setStage(ElectionStage.ELStage.ELECTING.getValue());
        return electionResult;
    }

    public static ElectionResult newCandidateResult(int controllerId, int epoch){
        ElectionResult electionResult = new ElectionResult(controllerId, epoch);
        electionResult.setStage(ElectionStage.ELStage.CANDIDATE.getValue());
        return electionResult;
    }

    public static ElectionResult newLeadingResult(int controllerId, int epoch){
        ElectionResult electionResult = new ElectionResult(controllerId, epoch);
        electionResult.setStage(ElectionStage.ELStage.LEADING.getValue());
        return electionResult;
    }

    public ElectionResult(int controllerId, int epoch, long timestamp, int fromNodeId, int stage) {
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.timestamp = timestamp;
        this.fromNodeId = fromNodeId;
        this.stage = stage;
    }

    public ByteBuffer toBuffer() {
        return super.newBuffer(0);
    }

    public static ElectionResult parseFrom(MessageBase messageBase) {
        ElectionResult electionResult = new ElectionResult();
        electionResult.setStage(messageBase.stage);
        electionResult.setEpoch(messageBase.epoch);
        electionResult.setTimestamp(messageBase.timestamp);
        electionResult.setControllerId(messageBase.controllerId);
        electionResult.setFromNodeId(messageBase.fromNodeId);
        return electionResult;
    }
}
