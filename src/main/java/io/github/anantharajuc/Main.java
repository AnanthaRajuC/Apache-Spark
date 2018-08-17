package io.github.anantharajuc;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Map;

public class Main 
{
	public static void main(String[] args) 
	{
		System.out.println("Apache Spark - Java API"); 
		
		SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD").setMaster("local[2]").set("spark.executor.memory", "2g");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);
		
		Map<String, String> filePropertiesMap = new HashedMap();
		
		filePropertiesMap.put("path", "K:\\sample_csv_file.csv");
		filePropertiesMap.put("header", "true"); 
		
		//Load data from CSV file to data frame
		DataFrame df = sqlContext.load("com.databricks.spark.csv", filePropertiesMap);
						
		//Display all records in the dataframe having the CSV data
		df.show();
	}
}
