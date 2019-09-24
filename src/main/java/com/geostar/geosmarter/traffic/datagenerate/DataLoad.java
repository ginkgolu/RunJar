package com.geostar.geosmarter.traffic.datagenerate;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * @author luyinxing
 * @since 2019-09-24
 */
public interface DataLoad {

	public void dataLoadFromFile(JavaSparkContext jsc, SQLContext sqlContext, boolean loadRoadMonitorAndCameraData);

	public void loadDataFromAutoGenerate(JavaSparkContext jsc, SQLContext sqlContext, boolean loadRoadMonitorAndCameraData);
}
