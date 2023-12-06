package org.hiraeth.registry.common.domain;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 11:52
 */
@Getter
@Setter
public class SlotRange {

    private int start;
    private int end;
    public SlotRange(int start, int end){
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SlotRange slotRange = (SlotRange) o;
        return start == slotRange.start && end == slotRange.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }


}
