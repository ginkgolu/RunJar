package com.geostar.geosmarter.recommendation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.geostar.geosmarter.recommendation.entity.MyRelationShip;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.geostar.geosmarter.traffic.datagenerate.Common;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;


/**
 * 
 * 需求：实现 好友推荐功能,推荐Top2的好友
 * 
 * 目的：增加application的用户粘度，让更多的用户使用该产品
 * 
 * 
 * 人物关系图 ：  
   A*****************************G
   * *                       * * *
   *   *                *    *   *
   *     C**********D      *     *
   *         *      *    *       *
   *             *  *  *         *
   B****************E************F
 * 
 * A ==> B C G
 * B ==> A E
 * C ==> A D E
 * D ==> C E G
 * E ==> B C D F G
 * F ==> E G
 * G ==> A D E F
 * 
 * 数据分析：
 * A B C G
 * A有朋友B,C,G
 * 那么A-B,A-C,A-G都是属于直接朋友关系
 * 如果单看这一行数据，那么B-C,B-G,C-G都是属于间接朋友关系
 * 
 * B A E
 * B有朋友A,E
 * 那么B-A,B-E都是属于直接朋友关系
 * 
 * 这时候，我们就可以看出A-E属于间接朋友关系，因为他们之间有B
 * 我们要实现的功能就是：把A推荐给E，或把E推荐给A
 * 
 * 
 * @author luyinxing
 * @created 2019-09-22
 */
public class FriendRecommendationMain implements Serializable {

	private static final long serialVersionUID = 1L;

	static final Logger logger = Logger.getLogger(FriendRecommendationMain.class);

	public static final int TOP_N = 2;

	public static void main(String[] args) {
		long begin = System.currentTimeMillis();
		FriendRecommendationMain friendRecommendationMain = new FriendRecommendationMain();
		friendRecommendationMain.processFriendREcommendation();
		long end = System.currentTimeMillis();
		logger.info("Finished Task. Total: " + (end - begin) + " ms");
	}

	private void processFriendREcommendation() {
		// Step 1:初始化
		SparkConf conf = new SparkConf();
		conf.setMaster(Common.MASTER_NAME).setAppName("Friend Recommendation");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		//Step 2:读取数据，生成第一个RDD
		JavaRDD<String> lines = jsc.textFile("RunJar/src/main/resources/recommendation_data");
		
		//Step 3:记录所有人关系，并扁平化处理
		JavaRDD<String> flatMapRDD = flatMapRDD(lines);
		//Step 4:将人物与关系切分开
		JavaPairRDD<String, Integer> mapToPairRDD = mapToPairRDD(flatMapRDD);
		//Step 5:相同key，将其value合并
		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = mapToPairRDD.groupByKey();
		//Step 6:将value值相加，并互换key与value的值，在进行降序
		JavaPairRDD<Integer, String> sortByKeyRDD = sortedRDD(groupByKeyRDD);
		//Step 7:过滤掉没有关系的人
		JavaPairRDD<Integer, String> filterRDD = filterRDD(sortByKeyRDD);

		//Step 8:推荐好友
		recommendProcess(filterRDD);

		//Step 9:关闭spark会话
		jsc.stop();

	}

	/**
	 * Step 3 根据每行数据，记录所有人的关系
	 * A-B-0 表示直接关系
	 * B-C-1 表示间接关系
	 */
	private JavaRDD<String> flatMapRDD(JavaRDD<String> lines) {
		JavaRDD<String> flatMapRDD = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<String> call(String line) {
				String[] split = line.split(Common.BLANK);
				List<String> relationshipList = new ArrayList<>();
				if (split != null && split.length > 0) {
					for (int i = 1; i < split.length; i++) {
						String directRelation = compare(split[0], split[i]) + "0";
						relationshipList.add(directRelation);
						for (int j = i + 1; j < split.length; j++) {
							String indirectRelation = compare(split[i], split[j]) + "1";
							relationshipList.add(indirectRelation);
						}
					}
				}
				return relationshipList.iterator();
			}
			
			private String compare(String a, String b) {
				if (a.compareTo(b) < 0) {
					return a + Common.MINUS + b + Common.MINUS;
				} else {
					return b + Common.MINUS + a + Common.MINUS;
				}
			}
		});
		return flatMapRDD;
	}

	/**
	 * Step 4
	 * input: F-G-0, D-F-1
	 * output: <F-G, 0>, <D-F, 1>
	 */
	private JavaPairRDD<String, Integer> mapToPairRDD(JavaRDD<String> flatMapRDD) {
		JavaPairRDD<String, Integer> mapToPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String str) {
				String[] split = str.split(Common.MINUS);
				return new Tuple2<>(split[0] + Common.MINUS + split[1], Integer.valueOf(split[2]));
			}
		});
		return mapToPairRDD;
	}

	/**
	 * Step 6
	 * 对间接关系的记录进行统计: <1,A-F>, <2,D-F>
	 * 忽略统计直接关系的记录    : <0,C-E>, <0,C-D>
	 */
	private JavaPairRDD<Integer, String> sortedRDD(JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD) {
		JavaPairRDD<Integer, String> sortByKeyRDD = groupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, Integer, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				Iterator<Integer> iterator = t._2.iterator();
				boolean isDirectRelation = false;
				int sum = 0;
				while (iterator.hasNext()) {
					if (iterator.next() == 0) {
						isDirectRelation = true;
						break;
					}
					sum++;
				}
				if (!isDirectRelation) {
					return new Tuple2<>(sum, t._1);
				}
				return new Tuple2<>(0, t._1);
			}
		}).sortByKey(false);
		return sortByKeyRDD;
	}

	/**
	 * Step 7
	 * 把直接关系的记录排除掉: <0,C-E>, <0,C-D>
	 * 那么剩下的就是间接关系的记录： <1,A-F>, <2,D-F>
	 */
	private JavaPairRDD<Integer, String> filterRDD(JavaPairRDD<Integer, String> sortByKeyRDD) {
		JavaPairRDD<Integer, String> filterRDD = sortByKeyRDD.filter(new Function<Tuple2<Integer, String>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<Integer, String> t) {
				return t._1 != 0;
			}
		});
		return filterRDD;
	}

	/**
	 * Step 8 好友推荐
	 */
	private void recommendProcess(JavaPairRDD<Integer, String> filterRDD) {
		JavaRDD<MyRelationShip> flatMapRelationShipRDD = filterRDD.flatMap(new FlatMapFunction<Tuple2<Integer, String>, MyRelationShip>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterator<MyRelationShip> call(Tuple2<Integer, String> t) throws Exception {
				String key = t._2;
				String[] split = key.split(Common.MINUS);
				List<MyRelationShip> list = new ArrayList<>();
				MyRelationShip relationShipAB = new MyRelationShip(split[0], split[1], t._1);
				MyRelationShip relationShipBA = new MyRelationShip(split[1], split[0], t._1);
				list.add(relationShipAB);
				list.add(relationShipBA);
				return list.iterator();
			}
		});

		JavaPairRDD<String, MyRelationShip> mapToPairRelationShipRDD = flatMapRelationShipRDD.mapToPair(new PairFunction<MyRelationShip, String, MyRelationShip>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, MyRelationShip> call(MyRelationShip vo) throws Exception {
				return new Tuple2<>(vo.getSelfName(), vo);
			}
		});

		// 分组和排序
		JavaPairRDD<String, Iterable<MyRelationShip>> groupByKey = mapToPairRelationShipRDD.groupByKey().sortByKey();

		// 结果展示
		showResult(groupByKey);
	}

	private void showResult(JavaPairRDD<String, Iterable<MyRelationShip>> groupByKey) {
		groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<MyRelationShip>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<String, Iterable<MyRelationShip>> t) throws Exception {
				Iterator<MyRelationShip> iterator = t._2.iterator();
				List<MyRelationShip> list = new ArrayList<>();
				while (iterator.hasNext()) {
					list.add(iterator.next());
				}
				Collections.sort(list);
				if (list != null && list.size() > 0) {
					int num = Math.min(FriendRecommendationMain.TOP_N, list.size());
					for (int i = 0; i < num; i++) {
						MyRelationShip myRelationShip = list.get(i);
						if (myRelationShip != null) {
							logger.info(t._1 + " , Recommended : " + myRelationShip.getOtherName() + ",weith : " + myRelationShip.getWeight());
						}
					}
				}
			}
		});
	}

}
