package com.fuzzy.common.query.feedback;

import com.alibaba.fastjson.JSONObject;
import com.fuzzy.Randomly;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class QuerySynthesisFeedback {
    // Syntax Node Sequence -> probability
    private final ConcurrentHashMap<String, Integer> sequenceRegenerateProbabilityTable =
            new ConcurrentHashMap<>();

    public void addSequenceRegenerateProbability(String sequence) {
        sequenceRegenerateProbabilityTable.compute(sequence, (k, v) -> v == null ? 1 : v + 1);
    }

    public void setSequenceRegenerateProbabilityToMax(String sequence, int maxExecutionCount) {
        sequenceRegenerateProbabilityTable.compute(sequence, (k, v) -> maxExecutionCount);
    }

    public boolean isRegenerateSequence(String sequence, int maxExecutionCount) {
        return Randomly.getBoolean(sequenceRegenerateProbabilityTable.getOrDefault(sequence, 0),
                maxExecutionCount);
    }

    public long getSequenceNumber() {
        return sequenceRegenerateProbabilityTable.size();
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(sequenceRegenerateProbabilityTable);
    }
}
