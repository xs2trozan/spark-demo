package com.deepak.spark;

import java.io.Serializable;
import java.util.Date;

/**
 * Model Class for SparkCassandraDemo
 * @author pathakd
 */
@SuppressWarnings("serial")
public class SensorData implements Serializable{
	
	private int sensor_id;
	private Date collected_at;
	private float volts;
	
	
	public SensorData(int sensor_id, Date collected_at, float volts) {
		super();
		this.sensor_id = sensor_id;
		this.collected_at = collected_at;
		this.volts = volts;
	}

	@Override
	public String toString() {
		return "SensorData [sensor_id=" + sensor_id + ", collected_at=" + collected_at + ", volts=" + volts + "]";
	}

	public float getVolts() {
		return volts;
	}

}
