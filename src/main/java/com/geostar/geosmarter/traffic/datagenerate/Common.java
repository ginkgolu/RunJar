package com.geostar.geosmarter.traffic.datagenerate;

import java.io.Serializable;

/**
 * @author luyinxing
 * @created 2019-09-22
 */
public class Common implements Serializable {
	private static final long serialVersionUID = 1L;

	public static String VEHICLE_LOG = "src/main/resources/vehicle_log";
	public static String ERROR_ROAD_IDS = "src/main/resources/error_road_ids";
	public static String ROAD_MONITOR_CAMERA_RELATIONSHIP = "src/main/resources/road_monitor_camera_relationship";
	
	public static String REAL_TIME_RESULT_PAGE = "src/main/resources/output/realTimeVehicleSpeedMonitor.html";

	public static final String[] ALPHABETICAL_ARRAY = new String[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
	public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
	public static final String DATE_FORMAT_YYYY_MM_DD_HHMMSS = DATE_FORMAT_YYYY_MM_DD + " HH:mm:ss";
	public static final String DATE_FORMAT_YYYYMMDD = "dd/MM/yyyy";
	public static final String DATE_FORMAT_YYYYMMDDHHMMSS = DATE_FORMAT_YYYYMMDD + " HH:mm:ss";
	public static final String DECIMAL_FORMAT = "#.#####";

	public static final String CHARSETNAME_UTF_8 = "UTF-8";
	public static final String SEPARATOR = "\t";
	public static final String LINE_BREAK = "\n";
	public static final String BLANK = " ";
	public static final String EMPTY = "";
	public static final String COLON = ":";
	public static final String COMMA = ",";
	public static final String ZERO = "0";
	public static final String AT = "@";
	public static final String MINUS = "-";
	public static final String GREATER_THAN_SIGN = ">";
	public static final String ILLEGAL_LOG = "ILLEGAL_LOG";

	// 车辆数
	public static final int VEHICLE_NUMBER = 5000;
	// 道路数
	public static final int ROAD_NUM = 60;
	// 异常路数量
	public static final int ROAD_ERROR_NUM = 4;
	// 最大车速
	public static final int MAX_SPEED = 300;
	// 超速
	public static final int OVER_SPEED = 250;
	
	public static final String ROAD_ID = "roadId";
	public static final String MONITOR_ID = "monitorId";
	public static final String CAMERA_ID = "cameraId";
	public static final String DATE_TIME = "dateTime";
	public static final String VEHICLE_PLATE = "vehiclePlate";
	public static final String VEHICLE_SPEED = "vehicleSpeed";

	public static final String VEHICLE_NUM = "vehicleNum";

	public static final String T_VEHICLE_LOG = "t_vehicle_log";
	public static final String T_ROAD_MONITOR_CAMERA_RELATIONSHIP = "t_road_monitor_camera_relationship";

	//Spark Core and SparkSQL
	public static final String MASTER_NAME = "local";
	public static final String APP_NAME_MONITOR_STATUS = "Monitor Status";
	public static final String APP_NAME_TOP10_MONITOR = "Top 10 Monitor";
	public static final String APP_NAME_TOP10_OVER_SPEED_MONITOR = "Top 10 Over Speed Monitor";
	public static final String APP_NAME_TOP10_OVER_SPEED_VEHICLE_NUMBER = "Top 10 Over Speed Vehicle Number";
	public static final String APP_NAME_TOP10_VEHICLE_NUMBER_HOUR = "Top 10 Vehicle Number Hour";
	public static final String APP_NAME_TOP10_TRAFFIC_JAM = "Top 10 Traffic Jam";
	public static final String APP_NAME_RANDOM_10_VEHICLE_TRACING = "Random 10 Vehicle Tracing";

	//SparkStreaming
	public static final String APP_NAME_REAL_TIME_TRAFFIC_JAM_STATUS = "Real Time Traffic Jam Status";
	//SparkStreaming的窗口函数，每隔多少时间处理一批数据
	public static final int SPARK_STREAMING_PROCESS_DATA_FREQUENCY = 5;
	//SparkStreaming的窗口函数，处理多久的数据
	public static final int SPARK_STREAMING_PROCESS_DATA_HISTORY = 1;
	
	public static final String SPARK_STREAMING_LOG_LEVEL = "WARN";
	public static final String SPARK_LOCAL_DIR = "spark.local.dir";
	public static final String SPARK_CHECK_POINT_DIR = "src/main/resources/checkpoint";
	
	// Kafka conf
	public static final String SERIALIZER_CLASS = "serializer.class";
	public static final String SERIALIZER_CLASS_AVLUE = "kafka.serializer.StringEncoder";
	public static final String METADATA_BROKER_LIST = "metadata.broker.list";
	public static final String METADATA_BROKER_LIST_VALUE = "192.168.244.135:9092";
	// 数据产生频率(s)
	public static final int DATA_GENERATION_FREQUENCY = 3;
	// 每次产生数据最大值
	public static final int DATA_GENERATION_MAX_CAPACITY = 5000;
	// kafka 里面的Topic
	public static final String KAFKA_TOPIC_SPARK_REAL_TIME_VEHICLE_LOG = "spark-real-time-vehicle-log";

}