package org.hiraeth.govern.server.entity.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.entity.ServerRequestType;
import org.hiraeth.govern.server.entity.ServerRole;
import org.hiraeth.govern.server.node.core.ElectionStage;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/29 14:02
 */
@Slf4j
@Getter
@Setter
public class ElectionResult extends RequestMessage {

    // 角色, 无需远程传输
    private ServerRole serverRole;

    public ElectionResult() {
        super();
        this.requestType = ServerRequestType.ElectionComplete.getValue();
    }

    public ElectionResult(String controllerId, int epoch) {
        super();
        this.requestType = ServerRequestType.ElectionComplete.getValue();
        this.controllerId = controllerId;
        this.epoch = epoch;
    }

    public static ElectionResult newElectingResult(String controllerId, int epoch){
        ElectionResult electionResult = new ElectionResult(controllerId, epoch);
        electionResult.setStage(ElectionStage.ELStage.ELECTING.getValue());
        return electionResult;
    }

    public static ElectionResult newLeadingResult(String controllerId, int epoch){
        ElectionResult electionResult = new ElectionResult(controllerId, epoch);
        electionResult.setStage(ElectionStage.ELStage.LEADING.getValue());
        return electionResult;
    }

    public ElectionResult(String controllerId, int epoch, long timestamp, String fromNodeId, int stage) {
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.timestamp = timestamp;
        this.fromNodeId = fromNodeId;
        this.stage = stage;
    }

    public void buildBuffer(){
        toBuffer();
    }

    public ByteBuffer toBuffer() {
        return super.toBuffer(0);
    }

    public static ElectionResult parseFrom(RequestMessage messageBase) {
        return BeanUtil.copyProperties(messageBase, ElectionResult.class);
    }
}
