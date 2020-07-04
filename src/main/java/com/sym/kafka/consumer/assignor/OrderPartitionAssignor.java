package com.sym.kafka.consumer.assignor;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * 自定义的分区分配策略
 *
 * @author shenyanming
 * @date 2020/6/27 14:41.
 */

public class OrderPartitionAssignor extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        return null;
    }

    @Override
    public void onAssignment(Assignment assignment, int generation) {

    }

    @Override
    public String name() {
        return null;
    }
}
