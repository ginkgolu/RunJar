package com.geostar.geosmarter.streaming.datagenerate;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.geostar.geosmarter.traffic.datagenerate.Common;
import com.geostar.geosmarter.traffic.datagenerate.GenerateVehicleLog;
import com.geostar.geosmarter.traffic.datagenerate.VehiclePlateGenerateSG;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * 产生实时数据线程类
 * 
 * 该线程会每隔{@link Common.DATA_GENERATION_FREQUENCY}秒，
 * 向Kafka中生产数据最大值为{@link Common.DATA_GENERATION_MAX_CAPACITY}的数据。
 * 
 * @author luyinxing
 * @created 2019-07-24
 */
public class RealTimeDataGenerate implements Runnable {

	Integer[] roadIdArray;
	Random random;
	List<Integer> errorRoadIdList;
	Producer<String, String> producer;

	public RealTimeDataGenerate(Integer[] roadIdArray, Random random, List<Integer> errorRoadIdList) {
		super();
		this.roadIdArray = roadIdArray;
		this.random = random;
		this.errorRoadIdList = errorRoadIdList;
		producer = new Producer<>(createProducerConfig());
	}

	@Override
	public void run() {
		int dataIndex = 0;
		while (true) {
			generateRealTimeDataToKafka(dataIndex);
			try {
				// 每次产生数据最大值
				if (dataIndex >= this.random.nextInt(Common.DATA_GENERATION_MAX_CAPACITY)) {
					// 数据产生频率(s)
					TimeUnit.SECONDS.sleep(Common.DATA_GENERATION_FREQUENCY);
					dataIndex = 0;
				} else {
					dataIndex++;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 向kafka中发送数据
	 */
	private void generateRealTimeDataToKafka(int key) {
		producer.send(new KeyedMessage<>(Common.KAFKA_TOPIC_SPARK_REAL_TIME_VEHICLE_LOG, key+Common.EMPTY, getOneVehicleLog()));
	}

	/**
	 * 产生一条vehicle log<2019-01-22 18:15:18 SSX14139U 141 10006 20011 4002>
	 */
	public String getOneVehicleLog() {
		String vehiclePlate = VehiclePlateGenerateSG.generatePlate(true);//车牌
		String oneVehicleLog = GenerateVehicleLog.getOneVehicleLog(true, Common.EMPTY, random, errorRoadIdList, vehiclePlate, roadIdArray);//记录
		if (oneVehicleLog != null && oneVehicleLog.length() > 0) {
			oneVehicleLog = oneVehicleLog.substring(0, oneVehicleLog.length() - 1);
		} else {
			return Common.ILLEGAL_LOG;
		}
		return oneVehicleLog;
	}

	/**
	 * 配置Kafka
	 */
	public static ProducerConfig createProducerConfig() {
		Properties props = new Properties();
		props.put(Common.SERIALIZER_CLASS, Common.SERIALIZER_CLASS_AVLUE);
		props.put(Common.METADATA_BROKER_LIST, Common.METADATA_BROKER_LIST_VALUE);
		return new ProducerConfig(props);
	}

}
