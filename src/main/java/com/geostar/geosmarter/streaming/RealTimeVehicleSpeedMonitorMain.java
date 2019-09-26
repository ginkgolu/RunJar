package com.geostar.geosmarter.streaming;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import kafka.serializer.StringDecoder;

import org.apache.log4j.Logger;
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

import com.geostar.geosmarter.streaming.util.FileUtils;
import com.geostar.geosmarter.streaming.util.RealTimeDataGenerateUtils;
import com.geostar.geosmarter.traffic.datagenerate.Common;

import scala.Tuple2;


/**
 * 需求：使用SparkStreaming，并且结合Kafka，获取实时道路交通拥堵情况信息。<br><br>
 * 
 * 目的： 对监控点平均车速进行监控，可以实时获取交通拥堵情况信息。相关部门可以对交通拥堵情况采取措施。<br>
 * e.g.1.通过广播方式，让司机改道。<br>
 * 2.通过实时交通拥堵情况数据，反映在一些APP上面，形成实时交通拥堵情况地图，方便用户查询。<br><br>
 * 
 * 使用说明：<br>
 * 
 * Step 1:部署kafka,参照https://blog.csdn.net/luyinxing1/article/details/101204506
 * 
 * Step 2: 启动Zookeeper<br>
 * 
 * Step 2: 启动Kafka<br>
 * 
 * Step 3: 创建topic： spark-real-time-vehicle-log<br>
 * 
 * Step 4: 查看所有topic<br>
 * 
 * Step 5： 运行RealTimeVehicleSpeedMonitorMain<br>
 * 
 * Step 6： 在开浏览器中把./output/realTimeVehicleSpeedMonitor.html文件打开，查看动态效果。<br>
 * 
 * 
 * @author luyinxing
 * @since 2019-09-24
 */
public class RealTimeVehicleSpeedMonitorMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(RealTimeVehicleSpeedMonitorMain.class);

	public static void main(String[] args) throws InterruptedException {
		logger.info("Begin Task");
		RealTimeVehicleSpeedMonitorMain realTimeVehicleSpeedMonitor = new RealTimeVehicleSpeedMonitorMain();
		realTimeVehicleSpeedMonitor.processRealTimeVehicleSpeedMonitor();
	}

	private void processRealTimeVehicleSpeedMonitor() throws InterruptedException {
		//Step 1: 模拟生产数据
		generateDataToKafka();
		
		//Step 2: 初始化Spark
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_REAL_TIME_TRAFFIC_JAM_STATUS);
		conf.set(Common.SPARK_LOCAL_DIR, Common.SPARK_CHECK_POINT_DIR);
		// 初始化SaprkStreaming
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(Common.SPARK_STREAMING_PROCESS_DATA_FREQUENCY));
		// 日志级别
		jsc.sparkContext().setLogLevel(Common.SPARK_STREAMING_LOG_LEVEL);
		// RDD快照
		jsc.checkpoint(Common.SPARK_CHECK_POINT_DIR);
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put(Common.METADATA_BROKER_LIST, Common.METADATA_BROKER_LIST_VALUE);
		Set<String> topicSet = new HashSet<>();
		topicSet.add(Common.KAFKA_TOPIC_SPARK_REAL_TIME_VEHICLE_LOG);
		
		//Step 3: 读取kafka数据
		JavaPairInputDStream<String, String> vehicleLogDS = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);
		// 获取行： 2019-01-22 18:15:18 SSX14139U 141 10006 20011 4002
		JavaDStream<String> vehicleLogDStream = vehicleLogDstream(vehicleLogDS);
		// 移除脏数据
		JavaDStream<String> vehicleLogFilterDStream = filter(vehicleLogDStream);
		// 返回<monitor_id, vehicle_speed>格式
		JavaPairDStream<String, Integer> monitorAndVehicleSpeedDStream = monitorAndVehicleSpeedDstream(vehicleLogFilterDStream);
		// 对key进行处理
		JavaPairDStream<String, Tuple2<Integer, Integer>> monitorAndVehicleSpeedWithVehicleNumDStream = processKey(monitorAndVehicleSpeedDStream);
		// <monitor_id, Tuple2<total_vehicle_speed, total_vehicle_num>>
		JavaPairDStream<String, Tuple2<Integer, Integer>> reduceByKeyAndWindow = getResult(monitorAndVehicleSpeedWithVehicleNumDStream);

		// print result
		printResult(reduceByKeyAndWindow);

		// 启动SparkStreaming
		jsc.start();
		jsc.awaitTermination();
	}

	/**
	 * 读取行value
	 * */
	private JavaDStream<String> vehicleLogDstream(JavaPairInputDStream<String, String> vehicleLogDS) {
		JavaDStream<String> vehicleLogDStream = vehicleLogDS.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2;
			}
		});
		return vehicleLogDStream;
	}

	/**
	 * 移除脏数据
	 * */
	private JavaDStream<String> filter(JavaDStream<String> vehicleLogDStream) {
		JavaDStream<String> vehicleLogFilterDStream = vehicleLogDStream.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String line) {
				// 移除掉包含Common.ILLEGAL_LOG的行
				return !line.equals(Common.ILLEGAL_LOG);
			}
		});
		return vehicleLogFilterDStream;
	}

	/**
	 * @param （检测点）
	 * @return 显示屏    速度
	 * */
	private JavaPairDStream<String, Integer> monitorAndVehicleSpeedDstream(JavaDStream<String> vehicleLogFilterDStream) {
		JavaPairDStream<String, Integer> monitorAndVehicleSpeedDStream = vehicleLogFilterDStream.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String line) {
				String[] split = line.split(Common.SEPARATOR);
				return new Tuple2<>(split[4], Integer.valueOf(split[2]));
			}
		});
		return monitorAndVehicleSpeedDStream;
	}

	/**
	 * map操作
	 * @param  （监测点ID    车速）
	 * @return 显示屏  <车速  1>
	 * */
	private JavaPairDStream<String, Tuple2<Integer, Integer>> processKey(JavaPairDStream<String, Integer> monitorAndVehicleSpeedDStream) {
		JavaPairDStream<String, Tuple2<Integer, Integer>> monitorAndVehicleSpeedWithVehicleNumDStream = monitorAndVehicleSpeedDStream.mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, Integer> call(Integer v) {
				return new Tuple2<>(v, 1);
			}
		});
		return monitorAndVehicleSpeedWithVehicleNumDStream;
	}

	/**
	 * reduces操作
	 * @param （监测点ID  <速度，1>）
	 * @return 
	 * */
	private JavaPairDStream<String, Tuple2<Integer, Integer>> getResult(JavaPairDStream<String, Tuple2<Integer, Integer>> monitorAndVehicleSpeedWithVehicleNumDStream) {
		JavaPairDStream<String, Tuple2<Integer, Integer>> reduceByKeyAndWindow = monitorAndVehicleSpeedWithVehicleNumDStream.reduceByKeyAndWindow(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) {
				// <total_vehicle_speed, total_vehicle_num>
				// 加入新值
				return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2());
			}
		}, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) {
				// <total_vehicle_speed, total_vehicle_num>
				// 移除之前的值
				return new Tuple2<>(v1._1 - v2._1, v2._2 - v2._2);
			}
			// 每隔Common.SPARK_STREAMING_PROCESS_DATA_FREQUENCY秒，处理过去Common.SPARK_STREAMING_PROCESS_DATA_HISTORY分钟的数据
		}, Durations.minutes(Common.SPARK_STREAMING_PROCESS_DATA_HISTORY), Durations.seconds(Common.SPARK_STREAMING_PROCESS_DATA_FREQUENCY));
		return reduceByKeyAndWindow;
	}

	/**
	 * 结果输出，写文件
	 */
	private void printResult(JavaPairDStream<String, Tuple2<Integer, Integer>> reduceByKeyAndWindow) {
		reduceByKeyAndWindow.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Integer, Integer>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaPairRDD<String, Tuple2<Integer, Integer>> rdd) {
				final SimpleDateFormat sdf = new SimpleDateFormat(Common.DATE_FORMAT_YYYY_MM_DD_HHMMSS);
				final Map<Integer, Integer> result = new TreeMap<>();
				logger.warn("**********************");
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<Integer, Integer>>>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public void call(Iterator<Tuple2<String, Tuple2<Integer, Integer>>> ite) throws Exception {
						while (ite.hasNext()) {
							Tuple2<String, Tuple2<Integer, Integer>> tuple = ite.next();
							String monitorId = tuple._1;
							int totalVehicleSpeed = tuple._2._1;
							int totalVehicleNumber = tuple._2._2;
							// 如果vehicle speed < 30，就判断为拥堵状态
							if (totalVehicleNumber != 0) {
								int averageVehicleSpeed = totalVehicleSpeed / totalVehicleNumber;
								//collect result
								result.put(Integer.valueOf(monitorId), averageVehicleSpeed);
							}
						}
						FileUtils.generateFile(Common.REAL_TIME_RESULT_PAGE, result, Common.SPARK_STREAMING_PROCESS_DATA_FREQUENCY, sdf.format(Calendar.getInstance().getTime()));
					}
				});
			}
		});
	}

	/**
	 * 向Kafka中产生实时Vehicle Log数据
	 */
	private void generateDataToKafka() {
		logger.info("Begin Generate Data to Kafka");
		RealTimeDataGenerateUtils.generateDataToKafka();
	}

}
