package org.hiraeth.govern.server.node.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.ElectionStage;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/29 14:02
 */
@Slf4j
@Getter
@Setter
public class ElectionResult {
    private int controllerId;
    private int epoch;
    private long timestamp;
    private int fromNodeId;
    //       // 选举阶段
    //        ELECTING 1,
    //        // 候选阶段, 已有初步投票结果, 需进一步确认
    //        CANDIDATE 2,
    //        // 领导阶段, 即已选举产生 leader
    //        LEADING 3
    private int stage;

    public ElectionResult(int controllerId, int epoch) {
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.timestamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
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
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putInt(RequestType.ElectionComplete.getValue());
        buffer.putInt(controllerId);
        buffer.putInt(epoch);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(fromNodeId);
        buffer.putInt(stage);
        return buffer;
    }

    public static ElectionResult parseFrom(ByteBuffer buffer) {
        int controllerId = buffer.getInt();
        int epoch = buffer.getInt();
        long timeStamp = buffer.getLong();
        int from = buffer.getInt();
        int stage = buffer.getInt();
        return new ElectionResult(controllerId, epoch, timeStamp, from, stage);
    }
}
