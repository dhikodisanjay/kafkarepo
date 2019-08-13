package com.clairvoyant.timebasedconsumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class ConsumerUtility {

	public static KafkaConsumer<String, String> consumer1 = null;

	static {
		System.out.println("initializing Consumer1 config***********");
		Properties properties1 = new Properties();
		properties1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		properties1.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group1");
		consumer1 = new KafkaConsumer<String, String>(properties1);
		System.out.println("Consumer1 initialized");

	}

	public static void main(String[] args) throws NumberFormatException, Exception {
		Map<TopicPartition, List<Integer>> offsMap = null;
		String topicName = args[0];
		Long startTime = Long.parseLong(args[1]);
		Long endTime = Long.parseLong(args[2]);

		try {

			offsMap = timeBasedConsumer(topicName, startTime, endTime);

			fetchData(offsMap);

		}

		catch (Exception e) {
			System.err.print("try with lower endtimestamp value");
		}

	}

	public static Map<TopicPartition, List<Integer>> timeBasedConsumer(String topicName, Long startTimestamp,
			Long endTimestamp) throws Exception {

		// Get List of Partitions
		List<PartitionInfo> partitionInfos = consumer1.partitionsFor(topicName);

		// Transform PartitionInfo into Topic Partition

		List<TopicPartition> partitionList = partitionInfos.stream()
				.map(info -> new TopicPartition(topicName, info.partition())).collect(Collectors.toList());

		consumer1.assign(partitionList);
		partitionList.forEach(p -> System.out.println("partitions for topic " + p));

//getting start timestamp offsets value
		Map<TopicPartition, Long> startpartitionTimestampMap = partitionList.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> startTimestamp));

		Map<TopicPartition, OffsetAndTimestamp> startpartitionOffsetMap = consumer1
				.offsetsForTimes(startpartitionTimestampMap);
		System.out.println("size of startpartitionOffsetMap " + startpartitionOffsetMap.size());

//getting end timestamp offsets value

		Map<TopicPartition, Long> endpartitionTimestampMap = partitionList.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> endTimestamp));

		Map<TopicPartition, OffsetAndTimestamp> endpartitionOffsetMap = consumer1
				.offsetsForTimes(endpartitionTimestampMap);
		System.out.println("size of endpartitionOffsetMap is " + endpartitionOffsetMap.size());

		// storing start and end offset position for specific partitions
		List<Integer> offsets = new ArrayList<Integer>();
		Map<TopicPartition, List<Integer>> offsetMapForRetrieval = new HashMap<TopicPartition, List<Integer>>();

//seeking offset value from start time stamp	
		startpartitionOffsetMap.forEach((tp, offsetAndTimestamp) -> {
			consumer1.seek(tp, offsetAndTimestamp.offset());
			offsets.add((int) offsetAndTimestamp.offset());
			System.out.println("partition" + tp + "start offset" + offsetAndTimestamp.offset());
			offsetMapForRetrieval.put(tp, offsets);

		});

//seeking offset value from end time stamp
		endpartitionOffsetMap.forEach((tp, offsetAndTimestamp) -> {
			offsets.add((int) offsetAndTimestamp.offset());
			System.out.println("partition" + tp + "end offset" + offsetAndTimestamp.offset());

			offsetMapForRetrieval.put(tp, offsets);

		});

		System.out.println("size of offsetList is" + offsets.size());
		offsets.forEach(i -> System.out.println("element in offset " + i + "\n"));

		return offsetMapForRetrieval;

	}

	public static void fetchData(Map<TopicPartition, List<Integer>> offsetMapForRetrieval) throws Exception {
		int startOffset = 0;
		int endOffset = 0;

		System.out.println("fetchData() called");

		ConsumerRecords<String, String> records = consumer1.poll(100);

		if (records == null) {
			System.out.println("records are null");
			System.exit(0);
		}

		System.out.println("record count is " + records.count());
		int index = 0;
		boolean flag = true;

		for (TopicPartition partition : offsetMapForRetrieval.keySet()) {

			startOffset = offsetMapForRetrieval.get(partition).get(index);
			if (flag) {
				endOffset = offsetMapForRetrieval.get(partition).get(index + 1);
			} else {
				endOffset = offsetMapForRetrieval.get(partition).get(index + 2);

			}
			System.out.println("partition " + partition + " startOffset " + startOffset + "endOffset" + endOffset);
			int count = 0;
			for (ConsumerRecord<String, String> record : records) {
				// System.out.println(record.offset()+"offset \t "+record.partition()+"\t
				// partition");

				if (record.offset() <= endOffset && record.partition() == partition.partition()) {
					System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value());
					count++;

				}

				if (record.offset() == endOffset) {
					continue;
				}

			}
			flag = false;
			index++;

			System.out.println("count is " + count);
			count = 0;
		}

	}

}
