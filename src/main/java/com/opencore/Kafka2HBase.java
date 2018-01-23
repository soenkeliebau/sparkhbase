package com.opencore;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * This is a simple example of BulkPut with Spark Streaming
 */
final public class Kafka2HBase {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("Kafka2HBase");
    //JavaSparkContext jsc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

    Configuration hbaseConf = HBaseConfiguration.create();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jssc.sparkContext(), hbaseConf);

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "10.0.0.7:9092,10.0.0.8:9092,10.0.0.9:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "kafkatestjobreader");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);

    Collection<String> topics = Arrays.asList("test");

    JavaDStream<String> test;

    JavaInputDStream<ConsumerRecord<String, String>> stream =
        KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

    hbaseContext.streamBulkPut(stream, TableName.valueOf("test"), new PutFunction());

    jssc.start();
  }

  public static class PutFunction implements Function<ConsumerRecord<String, String>, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(ConsumerRecord<String, String> record) throws Exception {
      Put put = new Put(Bytes.toBytes(record.partition() + record.offset()));

      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("key"), Bytes.toBytes(record.key()));
      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("offset"), Bytes.toBytes(record.offset()));
      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("partition"), Bytes.toBytes(record.partition()));
      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("timestamp"), Bytes.toBytes(record.timestamp()));
      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(record.value()));

      return put;
    }

  }
}