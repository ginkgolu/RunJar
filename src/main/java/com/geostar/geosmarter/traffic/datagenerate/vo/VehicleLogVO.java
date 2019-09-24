package com.geostar.geosmarter.traffic.datagenerate.vo;

import java.io.Serializable;

/**
 * @author luyinxing
 * @created 2019-09-22
 */
public class VehicleLogVO implements Serializable {

	private static final long serialVersionUID = -7858585505355892518L;

	private String dateTime;	//时间
	private String vehiclePlate;//车辆编号
	private int vehicleSpeed;	//车辆速度
	private String roadId;		//道路编号
	private String monitorId;	//监视器编号
	private String cameraId;	//摄像头编号

	public VehicleLogVO(String dateTime, String vehiclePlate, int vehicleSpeed, String roadId, String monitorId, String cameraId) {
		super();
		this.dateTime = dateTime;
		this.vehiclePlate = vehiclePlate;
		this.vehicleSpeed = vehicleSpeed;
		this.roadId = roadId;
		this.monitorId = monitorId;
		this.cameraId = cameraId;
	}

	public VehicleLogVO(String roadId, String monitorId, String cameraId) {
		super();
		this.roadId = roadId;
		this.monitorId = monitorId;
		this.cameraId = cameraId;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getVehiclePlate() {
		return vehiclePlate;
	}

	public void setVehiclePlate(String vehiclePlate) {
		this.vehiclePlate = vehiclePlate;
	}

	public int getVehicleSpeed() {
		return vehicleSpeed;
	}

	public void setVehicleSpeed(int vehicleSpeed) {
		this.vehicleSpeed = vehicleSpeed;
	}

	public String getRoadId() {
		return roadId;
	}

	public void setRoadId(String roadId) {
		this.roadId = roadId;
	}

	public String getMonitorId() {
		return monitorId;
	}

	public void setMonitorId(String monitorId) {
		this.monitorId = monitorId;
	}

	public String getCameraId() {
		return cameraId;
	}

	public void setCameraId(String cameraId) {
		this.cameraId = cameraId;
	}

}
