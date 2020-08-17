package com.sue.kafka.api.partition.custom;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author sue
 * @date 2020/8/17 16:24
 */

public class CustomPartition implements Partitioner {

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueByts,
                         Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int size = partitionInfos.size();
        if(keyBytes == null){
            return counter.getAndIncrement() % size;
        }else{
            return Utils.toPositive(Utils.murmur2(keyBytes))%size;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
