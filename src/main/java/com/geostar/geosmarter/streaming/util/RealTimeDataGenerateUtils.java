package com.geostar.geosmarter.streaming.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.geostar.geosmarter.streaming.datagenerate.RealTimeDataGenerate;
import com.geostar.geosmarter.traffic.datagenerate.DataGenerate;


/**
 * 实时数据生成工具类<br>
 * 
 * 提供向kafka中生产数据<br>
 * 
 * @author luyinxing
 * @since 2019-09-24
 */
public class RealTimeDataGenerateUtils implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void generateDataToKafka() {
		DataGenerate dataLoad = new DataGenerate(false);
		Integer[] roadIdArray = dataLoad.generateRoadIds(null, null, false);

		Random random = new Random();

		// 异常道路
		List<Integer> errorRoadIdList = new ArrayList<Integer>();
		dataLoad.generateErrorRoadIdList(random, errorRoadIdList);

		//使用线程池
		ExecutorService exec = Executors.newCachedThreadPool();
		exec.execute(new RealTimeDataGenerate(roadIdArray, random, errorRoadIdList));
		exec.shutdown();
	}

}
