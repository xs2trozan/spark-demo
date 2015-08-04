package com.deepak.spark;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

/**
 * CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = { 'class' :
 * 'SimpleStrategy', 'replication_factor' : 2 };
 * 
 * CREATE TABLE IF NOT EXISTS test_keyspace.data ( sensor_id int, collected_at
 * timestamp, volts float, PRIMARY KEY (sensor_id, collected_at) ) WITH COMPACT
 * STORAGE;
 * 
 * 
 * This class shows how spark can be connected to Cassandra database and
 * retrieve datasets and perform transformation and actions.
 * 
 * Sample data is stored in cassandra database
 * 3,2015-08-04 11:43:40+0530,0.5654227, program finds out maximum volts.
 * 
 * @author pathakd
 * 
 */
public class SparkCassandraDemo implements Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public void runSparkCassandraDemo() {

		// Setup the Spark configuration
		SparkConf conf = new SparkConf().setAppName("TestApplication").setMaster("local")
				.set("spark.cassandra.connection.host", "localhost");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Setting up CassandraJava RDD by passing <key space> and <table>
		CassandraJavaRDD<CassandraRow> cassandraRowsRDD = CassandraJavaUtil.javaFunctions(sc).cassandraTable(
				"test_keyspace", "data");

		// Transform : CassandraRowsRDD to JavaRDD
		JavaRDD<SensorData> sensorDataRDD = cassandraRowsRDD.map(new Function<CassandraRow, SensorData>() {
			public SensorData call(CassandraRow row) throws Exception {
				// System.out.println(row.getInt("sensor_id") + "||" +
				// row.getDate("collected_at") + "||" + row.getFloat("volts"));
				return new SensorData(row.getInt("sensor_id"), row.getDate("collected_at"), row.getFloat("volts"));
			}
		});

		// Calculate maximum volt
		SensorData maxVoltsSensorData = sensorDataRDD.reduce(new Function2<SensorData, SensorData, SensorData>() {
			public SensorData call(SensorData v1, SensorData v2) throws Exception {
				return (v1.getVolts() > v2.getVolts() ? v1 : v2);
			}
		});

		System.out.println(maxVoltsSensorData.toString());
	}

	// Local Test
	public static void main(String[] args) throws IOException {
		new SparkCassandraDemo().runSparkCassandraDemo();
	}
}
