package com.geostar.geosmarter.traffic.datagenerate.util;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.geostar.geosmarter.traffic.datagenerate.DataGenerate;
import com.geostar.geosmarter.traffic.datagenerate.DataLoad;


/**
 * @author luyinxing
 * @created 2019-09-24
 */
public class DataLoadUtils implements Serializable{

	private static final long serialVersionUID = 1L;

	/**
	 * 从文件中加载数据，如果系统找不到相应的数据文件，则会新生成数据，并把生成的数据保存在相应的文件中，以便后面加载数据的时候可以从里面加载出来。<br>
	 * 该方法数据不会丢失，也不会覆盖现有文件的数据。<br>
	 * 推荐使用此方法。<br>
	 * 
	 * @param jsc JavaSparkContext
	 * @param sqlContext SQLContext
	 * @param loadRoadMonitorAndCameraData 是否加载Road Monitor and Camera data
	 */
	public static void dataLoad(JavaSparkContext jsc, SQLContext sqlContext, boolean loadRoadMonitorAndCameraData) {
		DataLoad dataLoad = new DataGenerate(false);
		dataLoad.dataLoadFromFile(jsc, sqlContext, loadRoadMonitorAndCameraData);
	}
	
	/**
	 * 每次都会产生新的数据，，则会新生成数据，并把生成的数据保存在相应的文件中。<br>
	 * 这种方式会覆盖掉之前原有的数据。所以，之前文件里面的数据会丢失。<br>
	 * 该方法不推荐使用。<br>
	 * 
	 * @param jsc JavaSparkContext
	 * @param sqlContext SQLContext
	 * @param loadRoadMonitorAndCameraData 是否加载Road Monitor and Camera data
	 */
	public static void dataLoadOverWriteFile(JavaSparkContext jsc, SQLContext sqlContext, boolean loadRoadMonitorAndCameraData) {
		DataLoad dataLoad = new DataGenerate(true);
		dataLoad.loadDataFromAutoGenerate(jsc, sqlContext, loadRoadMonitorAndCameraData);
	}
	
	/**
	 * 从内存中加载数据，即每次临时产生数据在内存中，使用完后，数据会丢失。<br>
	 * 测试过程中可以使用此方法。<br>
	 * 
	 * @param jsc JavaSparkContext
	 * @param sqlContext SQLContext
	 * @param loadRoadMonitorAndCameraData 是否加载Road Monitor and Camera data
	 */
	public static void dataLoadFromMemory(JavaSparkContext jsc, SQLContext sqlContext, boolean loadRoadMonitorAndCameraData) {
		DataLoad dataLoad = new DataGenerate(false);
		dataLoad.loadDataFromAutoGenerate(jsc, sqlContext, loadRoadMonitorAndCameraData);
	}
}
