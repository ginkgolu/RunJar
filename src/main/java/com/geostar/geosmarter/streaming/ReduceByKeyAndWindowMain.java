package com.geostar.geosmarter.streaming;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.geostar.geosmarter.traffic.datagenerate.Common;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 需求：测试reduceByKeyAndWindow操作，每间隔10处理前15秒的数据。（不严谨，有待修改）
 * 		reduceByKeyAndWindowMain数据处理模型：https://blog.csdn.net/luyinxing1/article/details/101361315
 * 		kafka单机版搭建：https://blog.csdn.net/luyinxing1/article/details/101204506
 * 
 * @author luyinxing
 * @created 2019-09-26
 */
public class ReduceByKeyAndWindowMain {
	
	public static String topic = "test";//定义主题
	public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws InterruptedException {
		//Step 1:模拟生产实时数据
		produceData();
		
		//Step 2:创建spark会话
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("wordCount");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		jsc.checkpoint("C:\\Users\\Administrator\\Desktop\\test");
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put(Common.METADATA_BROKER_LIST, Common.METADATA_BROKER_LIST_VALUE);
		Set<String> topicSet = new HashSet<>();
		topicSet.add("test");
		
		//Step 3:创建离散流
		JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);
		
		//Step 4:取出value
		JavaDStream<String> map = map(stream);
		//Step 5:对value进行计数，并扁平化处理
		JavaPairDStream<String,Integer> mapToPair = mapToPair(map);
		//Step 6:统计
		JavaPairDStream<String,Integer> windowedWordCounts = reduceByKeyAndWindow(mapToPair);
		//Step 7：打印结果
		printResult(windowedWordCounts);
		
		//Step 8:启动SparkStreaming
		jsc.start();
		jsc.awaitTermination();
	}

	/**
	 * map操作，取出value   key为null
	 * */
	public static JavaDStream<String> map(JavaPairInputDStream<String, String> stream) {
		JavaDStream<String> map = stream.map(new Function<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Tuple2<String, String> v1) {
				return v1._2;
			}
		});
		return map;
	}

	public static JavaPairDStream<String, Integer> mapToPair(JavaDStream<String> map) {
		JavaPairDStream<String, Integer> mapToPair = map.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String word) {
				return new Tuple2<>(word, 1);
			}
		});
		return mapToPair;
	}

	public static JavaPairDStream<String, Integer> reduceByKeyAndWindow(JavaPairDStream<String,Integer> mapToPair) {
		JavaPairDStream<String, Integer> windowedWordCounts = mapToPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) {
				return v1 + v2;
			}
		}, Durations.seconds(15), Durations.seconds(10));
		return windowedWordCounts;
	}
	
	public static void printResult(JavaPairDStream<String,Integer> windowedWordCounts) {
		windowedWordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaPairRDD<String, Integer> t) {
				t.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public void call(Iterator<Tuple2<String, Integer>> t) throws IOException {
						List<String> list = new ArrayList<>();
						while (t.hasNext()) {
							Tuple2<String, Integer> next = t.next();
							String _1 = next._1;
							list.add(_1);
						}
						list.sort(String::compareTo);
						File file = new File("C:\\Users\\Administrator\\Desktop\\test\\test.txt");
						if (!file.exists()) {
							file.createNewFile();
						}
						file.createNewFile();
						FileOutputStream fos = new FileOutputStream(file, true);
						OutputStreamWriter osw = new OutputStreamWriter(fos, Common.CHARSETNAME_UTF_8);
						PrintWriter pw = new PrintWriter(osw);
						for (String str : list) {
							pw.write(str + "\r\n");
						}
						pw.write("====================================\r\n");
						pw.close();
						osw.close();
						fos.close();
					}
				});
			}
		});
	}

	public static void produceData() {
		Properties p = new Properties();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.244.135:9092");
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);
		
		Thread thread = new Thread(() -> {
			try {
				while (true) {
					String time = format.format(new Date());
					ProducerRecord<String, String> record = new ProducerRecord<>(topic, time);
					kafkaProducer.send(record);
					Thread.sleep(1000);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				kafkaProducer.close();
			}
		});
		thread.start();
	}

}

