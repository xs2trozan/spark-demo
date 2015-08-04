package com.deepak.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

/**
 * This class shows basic elements of Spark programming like reading external
 * dataset, performing transformation and actions on the dataset in a simpler
 * manner.
 * 
 * @author pathakd
 * 
 * */
public class SparkDemo implements Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public void runSparkDemo(String path) {
		
		// Set Spark configuration
		SparkConf conf = new SparkConf().setAppName("TestApplication").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load external data file as input x,y
		JavaRDD<String> lines = sc.textFile(path + "/data/sampledata.json");

		// Transform : Parse json data and transform into another JavaRDD
		JavaRDD<List<Data>> dataset = lines.map(new Function<String, List<Data>>() {
			public List<Data> call(String json) throws Exception {
				List<Data> data = new Gson().fromJson(json, new TypeToken<List<Data>>() {
				}.getType());
				return data;
			}
		});

		// Transform : Calculate simple addition on data (z=x+y) and transform into
		// calculated data RDD
		JavaRDD<List<Data>> calculatedData = dataset.map(new Function<List<Data>, List<Data>>() {
			public List<Data> call(List<Data> dataList) throws Exception {
				List<Data> dataset1 = new ArrayList<Data>();
				for (Data data : dataList) {
					dataset1.add(new Data(data.getX(), data.getY(), (data.getX() + data.getY())));
				}
				return dataset1;
			}
		});

		// Transform: Convert calculated data into json data.
		JavaRDD<List<JsonObject>> jsonDataSet = calculatedData.map(new Function<List<Data>, List<JsonObject>>() {
			public List<JsonObject> call(List<Data> dataList) throws Exception {
				List<JsonObject> jsonList = new ArrayList<JsonObject>();
				for (Data data : dataList) {
					JsonObject obj = new JsonObject();
					obj.addProperty("x", data.getX());
					obj.addProperty("y", data.getY());
					obj.addProperty("z", data.getZ());
					jsonList.add(obj);
				}
				return jsonList;
			}
		});

		// Action: Write Calculated data to system console
		calculatedData.foreach(new VoidFunction<List<Data>>() {
			public void call(List<Data> dataSet) throws Exception {
				for (Data data : dataSet)
					System.out.println(data.toString());
			}
		});

		// Action: Write jsonDataSet to a file
		jsonDataSet.saveAsTextFile(path + "/output");
	}

	// Local Test
	public static void main(String[] args) throws IOException {
		new SparkDemo().runSparkDemo("src/main/resources");
	}
}
