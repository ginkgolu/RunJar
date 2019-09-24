package com.geostar.geosmarter.traffic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import com.geostar.geosmarter.traffic.datagenerate.Common;
import com.geostar.geosmarter.traffic.datagenerate.util.DataLoadUtils;


/**
 * 当一辆车在道路上面行驶的时候，道路上面的监控点里面的摄像头就会对车进行数据采集。
 * 我们对采集的数据进行分析，处理。
 * 需求： 统计分析监控点和摄像头的状态（正常工作/异常）
 * 
 * 目的：如果摄像头异常，那么，就需要对这些摄像头进行维修或者更换。
 * 
 * @author luyinxing
 * @since 2019-09-24
 */
public class MonitorStatusMain implements Serializable{

	private static final long serialVersionUID = 1L;
	
	static final Logger logger = Logger.getLogger(MonitorStatusMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		MonitorStatusMain monitorStatusMain = new MonitorStatusMain();
		monitorStatusMain.processMonitorStatus();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	public void processMonitorStatus() {
		//Step 1：创建会话
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_MONITOR_STATUS);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		//Step 2:加载数据
		loadData(jsc, sqlContext);

		//Step 3:读取数据
		// 读取显示屏和摄像头数据
		JavaPairRDD<String, List<String>> monitorAndCameraIdListRDD = roadMonitorAndCameraDataProcess(sqlContext);
		// 读取车辆数据
		JavaPairRDD<String, Set<String>> vehicleLogMonitorAndCameraIdSetRDD = vehicleLogDataProcess(sqlContext);

		//Step 4：RDD转换
		JavaPairRDD<List<String>, List<String>> leftOuterJoinResultRDD = getLeftOuterJoinResultRDD(monitorAndCameraIdListRDD, vehicleLogMonitorAndCameraIdSetRDD);

		// print monitor status
		printMonitorStatus(leftOuterJoinResultRDD);
		
		jsc.stop();
	}

	/**
	 * 读取道路摄像头关系
	 * */
	private JavaPairRDD<String, List<String>> roadMonitorAndCameraDataProcess(SQLContext sqlContext) {
		//生成对应的javaRDD
		JavaRDD<Row> roadMonitorAndCameraRDD = sqlContext.sql("select roadId, monitorId,cameraId from " + Common.T_ROAD_MONITOR_CAMERA_RELATIONSHIP).javaRDD();
		//把查询出来的一条一条数据组装成<monitor_id, Row>形式
		JavaPairRDD<String, Row> monitorAndRowRDD = roadMonitorAndCameraRDD.mapToPair(new PairFunction<Row, String, Row>() {
			private static final long serialVersionUID = 1L;
			// <monitor_id, Row>
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getAs(Common.MONITOR_ID) + Common.EMPTY, row);
			}
		});

		// <monitor_id, Iterable<Row>>
		//通过groupByKey，这样就可以获取到同一个monitor_id下面对应的所有camera_id了
		JavaPairRDD<String, Iterable<Row>> monitorAndRowGroupByKeyRDD = monitorAndRowRDD.groupByKey();

		// <monirot_id, List<camera_id>>
		//再对groupByKey的结果进行组装，得到一个monitor_id对应一个List<camera_id>
		JavaPairRDD<String, List<String>> monitorAndCameraIdListRDD = monitorAndRowGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, List<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, List<String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
				Iterator<Row> rows = tuple2._2.iterator();
				List<String> cameraIdList = new ArrayList<>();
				while (rows.hasNext()) {
					Row row = rows.next();
					cameraIdList.add(row.getAs(Common.CAMERA_ID) + Common.EMPTY);
				}
				//因为这里是主表，所以这里是可以使用List<String>
				return new Tuple2<String, List<String>>(tuple2._1, cameraIdList);
			}
		});
		return monitorAndCameraIdListRDD;
	}

	/**
	 * 读取车辆数据
	 * */
	private JavaPairRDD<String, Set<String>> vehicleLogDataProcess(SQLContext sqlContext) {
		//only query monitor_id and camera_id
		JavaRDD<Row> vehicleLogRDD = sqlContext.sql("select monitorId,cameraId from " + Common.T_VEHICLE_LOG).javaRDD();
		JavaPairRDD<String, Row> vehicleLogMonitorAndRowRDD = vehicleLogRDD.mapToPair(new PairFunction<Row, String, Row>() {
			private static final long serialVersionUID = 1L;
			// <monitor_id, Row>
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getAs(Common.MONITOR_ID) + Common.EMPTY, row);
			}
		});

		JavaPairRDD<String, Iterable<Row>> vehicleLogMonitorAndRowGroupByKeyRDD = vehicleLogMonitorAndRowRDD.groupByKey();

		JavaPairRDD<String, Set<String>> vehicleLogMonitorAndCameraIdSetRDD = vehicleLogMonitorAndRowGroupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, Set<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Set<String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
				Iterator<Row> rows = tuple2._2.iterator();
				//不同的vehicle 可能产生相同的camera_id记录，所以这里需要把相同的记录去掉(去重)
				Set<String> cameraIdSet = new HashSet<String>();
				while (rows.hasNext()) {
					Row row = rows.next();
					cameraIdSet.add(row.getAs(Common.CAMERA_ID) + Common.EMPTY);
				}
				return new Tuple2<String, Set<String>>(tuple2._1, cameraIdSet);
			}
		});
		return vehicleLogMonitorAndCameraIdSetRDD;
	}

	//把主表记录和Vehicle Log记录join起来
	//即在主表中拿到了<monitor_id, List<camera_id>>记录，并且在vehicle Log中也拿到了<monitor_id, Set<camera_id>>记录
	//这时需要把两个记录进行Left Outer Join
	//就会得到<monitor_id, Tuple2<List<camera_id>, Optional<Set<camera_id>>>>记录
	//即同一个monitor_id,拿主表的List<camera_id>记录和vehicle log中的Set<camera_id>记录进比较
	//vehicle log中的Set<camera_id>可能全都能匹配主表里面的List<camera_id> --> 说明该monitor_id工作正常
	//vehicle log中的Set<camera_id>可能不全都能匹配主表里面的List<camera_id> --> 说明该monitor_id异常
	private JavaPairRDD<List<String>, List<String>> getLeftOuterJoinResultRDD(JavaPairRDD<String, List<String>> monitorAndCameraIdListRDD, JavaPairRDD<String, Set<String>> vehicleLogMonitorAndCameraIdSetRDD) {
		//left outer join
		JavaPairRDD<String, Tuple2<List<String>, Optional<Set<String>>>> leftOuterJoinRDD = monitorAndCameraIdListRDD.leftOuterJoin(vehicleLogMonitorAndCameraIdSetRDD);
		//cache left outer join result
		JavaPairRDD<String, Tuple2<List<String>, Optional<Set<String>>>> leftOuterJoinRDDCache = leftOuterJoinRDD.cache();
		
		JavaPairRDD<List<String>, List<String>> leftOuterJoinResultRDD = leftOuterJoinRDDCache.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<List<String>, Optional<Set<String>>>>>, List<String>, List<String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<Tuple2<List<String>, List<String>>> call(Iterator<Tuple2<String, Tuple2<List<String>, Optional<Set<String>>>>> ite) throws Exception {
				//存放所有异常的monitor_id
				List<String> errorMonitorIdList = new ArrayList<String>();
				//存放所有异常的camera_id
				List<String> errorCameraIdList = new ArrayList<String>();

				while (ite.hasNext()) {
					Tuple2<String, Tuple2<List<String>, Optional<Set<String>>>> tuple2 = ite.next();
					String monitorId = tuple2._1;
					List<String> stableCameraIdList = tuple2._2._1;
					Optional<Set<String>> optional = tuple2._2._2;
					Set<String> vehicleLogCameraIdSet = null;
					int stableCameraIdListSize = 0;
					int vehicleLogCameraIdSetSize = 0;

					// check if it has record
					if (optional.isPresent()) {
						vehicleLogCameraIdSet = optional.get();
						if (vehicleLogCameraIdSet != null) {
							vehicleLogCameraIdSetSize = vehicleLogCameraIdSet.size();
						}
					}

					if (stableCameraIdList != null) {
						stableCameraIdListSize = stableCameraIdList.size();
					}

					// this monitor with error
					if (stableCameraIdListSize != vehicleLogCameraIdSetSize) {
						if (vehicleLogCameraIdSetSize == 0) {
							// all cameras with error
							for (String cameraId : stableCameraIdList) {
								errorCameraIdList.add(cameraId);
							}
						} else {
							// at least one camera with error
							for (String cameraId : stableCameraIdList) {
								if (!vehicleLogCameraIdSet.contains(cameraId)) {
									errorCameraIdList.add(cameraId);
								}
							}
						}
						errorMonitorIdList.add(monitorId);
					}
				}

				return Arrays.asList(new Tuple2<List<String>, List<String>>(errorMonitorIdList, errorCameraIdList)).iterator();
			}
		});
		return leftOuterJoinResultRDD;
	}

	/**
	 异常监控点id和异常监控点对应的摄像头id
	 output:
	 	Error monitor id : 20010
		Error monitor id : 20094
		Error monitor id : 20078
		Error camera id : 40019
		Error camera id : 40020
		Error camera id : 40187
		Error camera id : 40188
		Error camera id : 40155
		Error camera id : 40156
	 */
	private void printMonitorStatus(JavaPairRDD<List<String>, List<String>> leftOuterJoinResultRDD) {
		leftOuterJoinResultRDD.foreach(new VoidFunction<Tuple2<List<String>, List<String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<List<String>, List<String>> tuple2) throws Exception {
				List<String> errorMonitorIdList = tuple2._1;
				List<String> errorCameraIdList = tuple2._2;

				if (errorMonitorIdList != null && errorMonitorIdList.size() > 0) {
					for (String monitorId : errorMonitorIdList) {
						logger.info("Error monitor id : " + monitorId);
					}

					if (errorCameraIdList != null && errorCameraIdList.size() > 0) {
						for (String cameraId : errorCameraIdList) {
							logger.info("Error camera id : " + cameraId);
						}
					}
				} else {
					logger.info("All monitors work well.");
				}
			}
		});
	}

	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		DataLoadUtils.dataLoad(jsc, sqlContext, true);
	}

}
