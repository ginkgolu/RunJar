package com.geostar.geosmarter.traffic;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.geostar.geosmarter.traffic.datagenerate.Common;
import com.geostar.geosmarter.traffic.datagenerate.util.DataLoadUtils;


/**
 * 需求： 在所有监控点里面，超速（max speed: 250）车辆最多的10个监控点是什么？
 * 
 * 目的： 知道了结果以后，相关人员可以对这些监控点所在的路段进行分析，并采取相关措施来限制车辆超速。比如：加设减速带等
 * 
 * @author luyinxing
 * @since 2019-09-22
 */
public class Top10OverSpeedVehicleNumberMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(Top10OverSpeedVehicleNumberMain.class);

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		Top10OverSpeedVehicleNumberMain top10OverSpeedVehicleNumberMain = new Top10OverSpeedVehicleNumberMain();
		top10OverSpeedVehicleNumberMain.processTop10OverSpeedVehicleNumber();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processTop10OverSpeedVehicleNumber() {
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName(Common.APP_NAME_TOP10_OVER_SPEED_VEHICLE_NUMBER);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		// load data
		loadData(jsc, sqlContext);
		// query top 10 Over speed vehicle number
		// 这里利用sql帮助我们查询出所需要的结果
		// 这时，我们就需要去调用一些算子来帮助我们实现 - groupByKey(), sortByKey(), mapToPair()等待
		JavaRDD<Row> monitorAndVehicleNumberRDD = sqlContext.sql("select monitorId,count(vehicleSpeed) vehicleNum from " + Common.T_VEHICLE_LOG + " where vehicleSpeed > " + Common.OVER_SPEED + " group by monitorId order by vehicleNum desc limit 10").javaRDD();

		// print result
		printResult(monitorAndVehicleNumberRDD);
		
		jsc.stop();
	}

	/**
	output:
		Top 10 Over Speed Monitor : 20093 , Vehicle Number : 124
		Top 10 Over Speed Monitor : 20077 , Vehicle Number : 119
		Top 10 Over Speed Monitor : 20009 , Vehicle Number : 105
		Top 10 Over Speed Monitor : 20059 , Vehicle Number : 73
		Top 10 Over Speed Monitor : 20079 , Vehicle Number : 71
		Top 10 Over Speed Monitor : 20005 , Vehicle Number : 71
		Top 10 Over Speed Monitor : 20118 , Vehicle Number : 71
		Top 10 Over Speed Monitor : 20033 , Vehicle Number : 71
		Top 10 Over Speed Monitor : 20073 , Vehicle Number : 70
		Top 10 Over Speed Monitor : 20065 , Vehicle Number : 70
	 */
	private void printResult(JavaRDD<Row> monitorAndVehicleNumberRDD) {
		monitorAndVehicleNumberRDD.foreach(new VoidFunction<Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row row) throws Exception {
				logger.info("Top 10 Over Speed Monitor : " + row.getAs(Common.MONITOR_ID) + " , Vehicle Number : " + row.getAs(Common.VEHICLE_NUM));
			}
		});
	}

	private void loadData(JavaSparkContext jsc, SQLContext sqlContext) {
		// load Vehicle Log data
		DataLoadUtils.dataLoad(jsc, sqlContext, false);
	}
}
