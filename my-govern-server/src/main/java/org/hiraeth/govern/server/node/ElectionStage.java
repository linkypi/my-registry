package org.hiraeth.govern.server.node;

import lombok.Getter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/29 14:20
 */
public class ElectionStage {

    @Getter
    public enum ELStage {
        // 选举阶段
        ELECTING(1),
        // 候选阶段, 已有初步投票结果, 需进一步确认
        CANDIDATE(2),
        // 领导阶段, 即已选举产生 leader
        LEADING(3);

        ELStage(int val){
            this.value = val;
        }

        private int value;

    }
    private ElectionStage() {
    }

    public static class Singleton {
        private static final ElectionStage instance = new ElectionStage();
    }

    public static ElectionStage getInstance() {
        return ElectionStage.Singleton.instance;
    }

    private volatile ELStage status = ELStage.ELECTING;

    public static ELStage getStatus(){
        return getInstance().status;
    }

    public static void setStatus(ELStage masterRole){
        getInstance().status = masterRole;
    }
}
