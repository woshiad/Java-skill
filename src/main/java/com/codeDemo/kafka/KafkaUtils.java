package com.codeDemo.kafka;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

/**
 * Kafka工具类
 *
 * @author LJF
 */
@Slf4j
//@Component
public class KafkaUtils {

    @Autowired
    private CreatClient creatClient;


    /**
     * 获取最新一条topic Data
     *
     * @param consumer 消费者
     * @param topic    topicName
     * @return
     */
    public static String getNewConsumerData(KafkaConsumer consumer, String topic, String regexExp) {
        String topicData = "";
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();

        //取100条日志
        Long dataSize = 100L;
        try {
//            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
//
//            List<PartitionInfo> partitionInfos = topics.get(topic);
//            for (PartitionInfo partitionInfo : partitionInfos) {
//                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
//            }
//            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
//
//            //分区对象
//            TopicPartition topicPartition = null;
//
//            for (TopicPartition tp : endOffsets.keySet()) {
//                topicPartition = tp;
//                break;
//            }
//
//            //设置消费的分区 这个相当于临时消费不记录在topic中(手动提交偏移量)
//            consumer.assign(Collections.singletonList(topicPartition));

//            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
//
//            List<PartitionInfo> partitionInfos = topics.get(topic);
//            for (PartitionInfo partitionInfo : partitionInfos) {
//                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
//            }


//            consumer.assign(Collections.singleton(topicPartitions));


//            consumer.subscribe(Arrays.asList(topic));

//            Duration duration = Duration.ofNanos(1000000);//设置 1000000纳秒时间
//            ConsumerRecords<String, String> records = consumer.poll(duration);
//            if (!records.isEmpty()) {
//                for (ConsumerRecord<String, String> record : records) {
//                    if (record != null) {
//                        topicData = record.value();
//                    }
//                }
//            }


//            int sum = 0;
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();

            if (null == topics || topics.size() == 0 || !topics.containsKey(topic)) {
                throw new RuntimeException("kafka中无对应topic");
            }

            List<PartitionInfo> partitionInfos = topics.get(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(topicPartitions);
//            long position = consumer.position(topicPartitions.get(0));


            //分区对象
            TopicPartition topicPartition = null;


            long endIndex = 0;

            for (TopicPartition tp : endOffsets.keySet()) {
                if (topicPartition == null) {
                    topicPartition = tp;
                    endIndex = endOffsets.get(tp);
                } else {
                    if (endOffsets.get(tp) > endIndex) {
                        topicPartition = tp;
                        endIndex = endOffsets.get(tp);
                    }
                }
            }

            //拿到已提交的 开始的偏移量
            Long beginIndex = beginOffsets.get(topicPartition);

            if (Convert.toStr(beginIndex).equals(Convert.toStr(endIndex))) {
                throw new RuntimeException("kafkaTopic 中无数据");
            }

            //设置获取数据的偏移量
            long expectedOffSet = endIndex - dataSize;//获取最新100条数据
            if (expectedOffSet < 0) {
                expectedOffSet = 0;
            }

            //手动订阅topic
            consumer.assign(Collections.singletonList(topicPartition));

            //设置从kafka开始读取的偏移位置
            consumer.seek(topicPartition, expectedOffSet);

            //循环拿取，直到拿够100条（最大循环3次）
            ArrayList<ConsumerRecords<String, String>> recordsArrayList = new ArrayList<>();
            int count = 0;//记录当前拿了多少条了

            for (int i = 0; i < 3; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                count += records.count();
                recordsArrayList.add(records);

                //如果拿够100条则提前退出
                if (count >= dataSize) {
                    break;
                }

            }
            log.info("从kafka拿了：{}条日志", count);

            //判断是否有正则表达式
            if (StringUtils.isNotBlank(regexExp)) {
                //存在则校验正则表达式
                ArrayList<String> arrayList1 = new ArrayList<>();
                for (ConsumerRecords<String, String> records : recordsArrayList) {
                    for (ConsumerRecord<String, String> record : records) {
                        if (record != null && record.value().matches(regexExp)) {
                            arrayList1.add(record.value());
                        }
                    }
                }

                topicData = getRandomLog(arrayList1, dataSize);

            } else {
                //不存在则直接随机取一条数据
                ArrayList<String> arrayList2 = new ArrayList<>();
                for (ConsumerRecords<String, String> records : recordsArrayList) {
                    for (ConsumerRecord<String, String> record : records) {
                        if (record != null) {
                            arrayList2.add(record.value());
                        }
                    }
                }

                topicData = getRandomLog(arrayList2, dataSize);

            }
        } catch (Exception ex) {
            log.error("获取一条日志异常", ex);
        } finally {
            consumer.close();
        }
        return topicData;
    }

    /**
     * 随机序号获取一条日志
     *
     * @param logList  过滤后的日志List
     * @param dataSize 最大循环次数 / 随机数最大值
     * @return
     */
    private static String getRandomLog(List<String> logList, Long dataSize) {
        String dataStr = "";
        if (logList.size() > 0) {
            log.info("过滤日志中共：{}条", logList.size());
            Random random = new Random();
            for (int i = 0; i < dataSize; i++) {
                int index = random.nextInt(Integer.parseInt(String.valueOf(dataSize)));
                if (index < logList.size()) {
                    dataStr = logList.get(index);
                    log.info("随机日志序号为：{},---随机取的日志为：{}", index, dataStr);
                }
                if (!dataStr.equals("")) {
                    log.info("随机序号循环了：{}次", i);
                    break;
                }
            }
        } else {
            log.info("过滤日志中无日志！");
        }
        return dataStr;
    }


    /**
     * 比较未解析字段topic偏移量是否为最大偏移量
     *
     * @param consumer       消费者
     * @param topic          topicName
     * @param beginOffsetStr 起始偏移量
     * @return
     */
    public static Boolean compareOffset(KafkaConsumer consumer, String topic, String beginOffsetStr) {
        try {
            List<TopicPartition> topicPartitions = new ArrayList<>();
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            List<PartitionInfo> partitionInfos = topics.get(topic);
            //没有该topic的分区对象
            if (partitionInfos == null) {
                return false;
            }
            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            //分区对象
            TopicPartition topicPartition = null;

            //该topic最大偏移量
            Long endIndex = 0L;

            for (TopicPartition tp : endOffsets.keySet()) {
                if (topicPartition == null) {
                    topicPartition = tp;
                    endIndex = endOffsets.get(tp);
                } else {
                    if (endOffsets.get(tp) > endIndex) {
                        topicPartition = tp;
                        endIndex = endOffsets.get(tp);
                    }
                }
            }

            Long beginIndex = 0L;
            if (StringUtils.isNotBlank(beginOffsetStr)) {
                beginIndex = Long.parseLong(beginOffsetStr);
            }

            //如果最大偏移量大于当前偏移量
            if (endIndex > beginIndex) {
                return true;
            }

        } catch (Exception ex) {
            log.error("比较consumer偏移量异常", ex);
        } finally {
            consumer.close();
        }
        return false;
    }

    /**
     * 查询未解析topic中Data
     *
     * @param consumer       消费者
     * @param topic          topicName
     * @param beginOffsetStr 起始偏移量
     * @return
     */
    public static List<String> getConsumerDataCount(KafkaConsumer consumer, String topic, String beginOffsetStr) {
        List<String> data = new ArrayList<>();
        List<TopicPartition> topicPartitions = new ArrayList<>();
        try {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            List<PartitionInfo> partitionInfos = topics.get(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            //分区对象
            TopicPartition topicPartition = topicPartitions.get(0);

            //如果当前存在已提交的 开始的偏移量
            Long beginIndex = 0L;
            if (StringUtils.isNotBlank(beginOffsetStr)) {
                beginIndex = Long.parseLong(beginOffsetStr);
            }

            //手动订阅topic
            consumer.assign(Collections.singletonList(topicPartition));

            //设置开始读取的偏移量
            consumer.seek(topicPartition, beginIndex);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                if (record != null) {
                    data.add(record.value());
                }
            }
//            —————————————————————————————————————————————————————————————————————————————————

            // 1、获取未解析字段(自动提交关、不提交偏移量)
                /*consumer.subscribe(Arrays.asList(topic));
                Duration duration2 = Duration.ofNanos(1000000);//设置 1000000纳秒时间
                ConsumerRecords<String, String> records2 = consumer.poll(duration2);
                for (ConsumerRecord<String, String> record : records2) {
                    if (record != null) {
                        data.add(record.value());
                    }
                }*/

            // 2、提交未解析字段（自动提交关，手动提交最大偏移量）
                /*Map<String, List<PartitionInfo>> topics = consumer.listTopics();

                List<PartitionInfo> partitionInfos = topics.get(topic);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
                }
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);


                //分区对象
                TopicPartition topicPartition = null;

                //获取最大偏移量
                Long endIndex = null;
                for (TopicPartition tp : endOffsets.keySet()) {
                    if (topicPartition == null) {
                        topicPartition = tp;
                        endIndex = endOffsets.get(tp);
                    } else {
                        if (endOffsets.get(tp) > endIndex) {
                            topicPartition = tp;
                            endIndex = endOffsets.get(tp);
                        }
                    }
                }


                //设置消费的分区 这个相当于临时消费不记录在topic中(手动提交偏移量)
                consumer.assign(Collections.singletonList(topicPartition));

                //提交最大偏移量，设置 1000000纳秒时间
                ConsumerRecords records = consumer.poll(Duration.ofNanos(1000000));
                log.error("——————————————————utils——————————————————————");

                //提交偏移量到指定分区 (参数格式：Map<分区,偏移量>)
//                consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(endIndex-1)));*/

        } catch (Exception ex) {
            log.error("查询未解析字段异常", ex);
        } finally {
            consumer.close();
        }
        return data;
    }


    /**
     * 查看数据预处理后的数据
     * <p>
     * (当前为正序实现)
     *
     * @return
     */
    public static List<Map<String, Object>> getAccessHandleData(KafkaConsumer consumer, String topic, Long beginTime, Long endTime, String offsetStr, Integer pageSize) {
        List<Map<String, Object>> data = new ArrayList<>();
        try {
            //转换为TopicPartition
            List<TopicPartition> topicPartitions = new ArrayList<>();

            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            List<PartitionInfo> partitionInfos = topics.get(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            //目前使用的分区对象 均只有1个分区
            TopicPartition usePartition = topicPartitions.get(0);

            //开始偏移量
            long offset;

            //截止偏移量
            Long stopOffset;

            //topic最大偏移量
            long maxOffset;

            /*
             * 开始和结束偏移量的获取标识
             *  如果 beginOffsetFlag 为 false，则说明通过时间获取偏移量为空，此时有2种情况
             *  1、开始时间在所有数据之前
             *  2、开始时间在所有数据之后
             *
             *  如果 stopOffsetFlag 为 false，则说明通过时间获取偏移量为空，此时有2种情况
             *  1、结束时间在所有数据之前
             *  2、结束时间在所有数据之后
             *
             *  所有此组合有4种情况存在（以下仅表示值，顺序均为 beginOffsetFlag-stopOffsetFlag）：
             * 1、true-true
             * 2、true-false
             * 3、true-true
             * 4、false-false
             *
             */
            boolean beginOffsetFlag = false;
            boolean stopOffsetFlag = false;

            //如果没有则通过时间获取偏移量 ，有偏移量则直接使用
            if (StringUtils.isBlank(offsetStr)) {
                //根据时间获取偏移量

                //开始偏移量
                Map<TopicPartition, Long> map1 = new HashMap<>();
                map1.put(usePartition, beginTime);
                Map<TopicPartition, OffsetAndTimestamp> beginOffsets = consumer.offsetsForTimes(map1);
                OffsetAndTimestamp offsetAndTimestamp = beginOffsets.get(usePartition);
                if (offsetAndTimestamp != null) {
                    offset = offsetAndTimestamp.offset();
                } else {
                    //如果根据开始时间获取偏移量为null，则默认值给0
                    offset = 0L;
                }
            } else {
                offset = Long.parseLong(offsetStr);
            }

            //如果选择时间超过当前 则将结束时间置为当前
            if (endTime > System.currentTimeMillis()) {
                endTime = System.currentTimeMillis();
            }

            //截止偏移量
            Map<TopicPartition, Long> map2 = new HashMap<>();
            map2.put(usePartition, endTime);
            Map<TopicPartition, OffsetAndTimestamp> stopOffsets = consumer.offsetsForTimes(map2);
            if (stopOffsets.get(usePartition) == null) {
                //如果选择结束时间获取偏移量为null 则获取topic最大偏移量
                Map<TopicPartition, Long> topicEndOffsets = consumer.endOffsets(topicPartitions);
                stopOffset = topicEndOffsets.get(usePartition);
                maxOffset = topicEndOffsets.get(usePartition);
            } else {
                OffsetAndTimestamp offsetAndTimestamp = stopOffsets.get(usePartition);
                stopOffset = offsetAndTimestamp.offset();

                //获取最大偏移量
                maxOffset = (long) consumer.endOffsets(topicPartitions).get(usePartition);
            }

            //判断截止偏移量是否为空 或 为最大偏移量

            //手动订阅指定分区对象 TopicPartition
            consumer.assign(Collections.singletonList(usePartition));

            //设置开始读取的偏移量
            consumer.seek(usePartition, offset);

            boolean stopFor = false;
            for (int i = 0; i < 3; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if (record != null) {
                        //如果偏移量大于截止偏移量 或 数据量达到分页值 则退出 或 最大偏移量
                        if (record.offset() > stopOffset || data.size() > pageSize) {
                            stopFor = true;
                            break;
                        }
                        HashMap<String, Object> map = new HashMap<>();
                        String format = DateUtil.format(DateUtil.date(record.timestamp()), "yyyy-MM-dd HH:mm:ss");
                        map.put("time", format);
                        map.put("content", record.value());
                        map.put("offset", record.offset());
                        data.add(map);
                    }
                }
                if (stopFor) {
                    break;
                }

            }

        } catch (Exception ex) {
            log.error("查看数据预处理后数据异常", ex);
        } finally {
            consumer.close();
        }
        return data;
    }


    /**
     * 向kafka发送数据
     */
    public Boolean sendToKafka(String json, String topicName) {
        Producer producer = null;
        AdminClient adminClient = null;
        KafkaConsumer consumer = null;
        try {
            adminClient = creatClient.getKafkaAdminClient();
            consumer = creatClient.getKafkaConsumerToLog();

            Map topics = consumer.listTopics();

            if (null == topics || topics.size() == 0 || !topics.containsKey(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic));
            }

            producer = creatClient.getKafkaProducer();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, json);
            producer.send(record);

            log.info("写入kafka的数据为：{}", record);
            return true;
        } catch (RuntimeException e) {
            log.error("向kafka中发送数据失败" + e);
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
            if (consumer != null) {
                consumer.close();
            }
            if (producer != null) {
                producer.close();
            }
        }
        return false;
    }

}
