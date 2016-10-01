package com.pbehera.insightfinder.Demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Json2CSVParser {
	
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
	
	static Date startTime = new Date();
	static Date endTime = new Date();
	
	
	
	String sourceJsonFile;
	String destinationCSVFile;
	Map<String, InstanceStats> instantStatsMap = new HashMap<String, InstanceStats>();
	Set<String> fieldNames = new TreeSet<String>();
	
	public Json2CSVParser(String sourceJsonFile, String destinationCSVFile) throws FileNotFoundException {
		this.sourceJsonFile = sourceJsonFile;
		this.destinationCSVFile = destinationCSVFile;
		
		getAttributeNames();
	}
	
	public void getAttributeNames() throws FileNotFoundException
	{
		File file = new File(sourceJsonFile);
		Scanner sc = new Scanner(new File(sourceJsonFile));
		
		int count = 0;
		
		while(sc.hasNextLine())
		{
			String line = sc.nextLine();
			String jsonString = StringUtils.stripEnd(line,null);
			if (StringUtils.isBlank(line))
				continue;
			//System.out.println("String is: " + jsonString);
			JsonElement jElement = null;
			Gson gson = new Gson();
			jElement = new JsonParser().parse(jsonString);
			
			JsonObject  jObject = jElement.getAsJsonObject().getAsJsonObject("payload").
					getAsJsonArray("datasets").get(0).getAsJsonObject().getAsJsonObject("data").getAsJsonObject("metadata");
			
			JsonArray jArray = jObject.getAsJsonArray("keys");
			fieldNames.addAll(gson.fromJson(jArray, ArrayList.class));
		}
		
		fieldNames.remove("assetId");
		fieldNames.remove("timestamp");
		
	}
	
	public void generateCSVFile() throws IOException
	{
		Scanner sc;
		FileWriter fileWriter = new FileWriter(destinationCSVFile);
		
		try {
			File file = new File(sourceJsonFile);
			sc = new Scanner(file);
			
			fileWriter.append(getCSVHeader());
			
			while(sc.hasNextLine())
			{
				String line = sc.nextLine();
				String jsonString = StringUtils.stripEnd(line,null);
				if (StringUtils.isBlank(line))
					continue;
				//System.out.println("String is: " + jsonString);
				
				MetricDataRow metricDataRow = new MetricDataRow(jsonString);
				
				//System.out.println("KeySet MetricIndex as: " + metricDataRow.metricIndexMap.toString());
				
				for(List<Object> dataEntry:metricDataRow.metricValues)
				{
					fileWriter.append(getCSVRow(dataEntry, metricDataRow.metricIndexMap));
				}
				
				//System.out.println(metricDataRow.toString());
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			fileWriter.flush();
			fileWriter.close();
		}
	}
	
	public String getCSVHeader()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("timestamp");
		sb.append(COMMA_DELIMITER);
		sb.append("assetId");
		
		for(String fieldName: fieldNames)
		{
			sb.append(COMMA_DELIMITER);
			sb.append(fieldName);
		}
	
		sb.append(NEW_LINE_SEPARATOR);
		return sb.toString();	
	}
	
	
	public String getCSVRow(List<Object> dataEntry, Map<String, Integer> metricIndexMap)
	{
		StringBuilder sb = new StringBuilder();
		InstanceStats instanceStats;
		sb.append(dataEntry.get(metricIndexMap.get("timestamp")));
		sb.append(COMMA_DELIMITER);
		String assetId = (String) dataEntry.get(metricIndexMap.get("assetId"));
		assetId = assetId.split(":")[2];
		
		if(!instantStatsMap.keySet().contains(assetId))
		{
			instanceStats = new InstanceStats();
			instanceStats.setInstanceId(assetId);
			instantStatsMap.put(assetId, instanceStats);
		}
		else
		{
			instanceStats = instantStatsMap.get(assetId);
		}
		
		instanceStats.setNumOfDataPoints(instanceStats.getNumOfDataPoints()+1);
		
		sb.append(assetId);
		
		//System.out.println("The Map keyset is: " + metricIndexMap.keySet().toString());
		
		
		for(String fieldName: fieldNames)
		{
			if(metricIndexMap.keySet().contains(fieldName))
			{
					sb.append(COMMA_DELIMITER + dataEntry.get(metricIndexMap.get(fieldName)));
			}
			else
			{
				if(instanceStats.missingMetricData.keySet().contains(fieldName))
					instanceStats.missingMetricData.put(fieldName, instanceStats.missingMetricData.get(fieldName)+1);
				else
					instanceStats.missingMetricData.put(fieldName, 1);
				sb.append(COMMA_DELIMITER+"Nan");
			}
		}
		
		
		
		
		/*
		if(metricIndexMap.keySet().contains("memory:usedpercent.avg"))
		{
				sb.append(COMMA_DELIMITER + dataEntry.get(metricIndexMap.get("memory:usedpercent.avg")));
		}
		else
		{
			if(instanceStats.missingMetricData.keySet().contains("memory:usedpercent.avg"))
				instanceStats.missingMetricData.put("memory:usedpercent.avg", instanceStats.missingMetricData.get("memory:usedpercent.avg")+1);
			else
				instanceStats.missingMetricData.put("memory:usedpercent.avg", 1);
			sb.append(COMMA_DELIMITER+"Nan");
		}
			
		if(metricIndexMap.keySet().contains("cpu:usedpercent.max"))
		{
			sb.append(COMMA_DELIMITER + dataEntry.get(metricIndexMap.get("cpu:usedpercent.max")));
		}
		else
		{
			if(instanceStats.missingMetricData.keySet().contains("cpu:usedpercent.max"))
				instanceStats.missingMetricData.put("cpu:usedpercent.max", instanceStats.missingMetricData.get("cpu:usedpercent.max")+1);
			else
				instanceStats.missingMetricData.put("cpu:usedpercent.max", 1);
			sb.append(COMMA_DELIMITER+"Nan");
		}
			
		if(metricIndexMap.keySet().contains("fs:size.avg"))
		{
			sb.append(COMMA_DELIMITER + dataEntry.get(metricIndexMap.get("fs:size.avg")));
		}
		else
		{
			if(instanceStats.missingMetricData.keySet().contains("fs:size.avg"))
				instanceStats.missingMetricData.put("fs:size.avg", instanceStats.missingMetricData.get("fs:size.avg")+1);
			else
				instanceStats.missingMetricData.put("fs:size.avg", 1);
			sb.append(COMMA_DELIMITER+"Nan");
		}
			
		if(metricIndexMap.keySet().contains("fs:used.avg"))
		{
			sb.append(COMMA_DELIMITER + dataEntry.get(metricIndexMap.get("fs:used.avg")));
		}
		else
		{
			if(instanceStats.missingMetricData.keySet().contains("fs:used.avg"))
				instanceStats.missingMetricData.put("fs:used.avg", instanceStats.missingMetricData.get("fs:used.avg")+1);
			else
				instanceStats.missingMetricData.put("fs:used.avg", 1);
			sb.append(COMMA_DELIMITER+"Nan");
		}
		*/
			
		sb.append(NEW_LINE_SEPARATOR);
		return sb.toString();
	}
	
	String getIndex(String field)
	{
		if(field.toLowerCase().equals("cpu"))
			return "4001";
		if(field.toLowerCase().equals("diskread") || field.toLowerCase().equals("diskwrite"))
			return "4002";
		if(field.toLowerCase().equals("networkin") || field.toLowerCase().equals("networkout"))
			return "4003";
		if(field.toLowerCase().equals("memused"))
			return "4004";
		
		return "";
		
	}
	
	

}
