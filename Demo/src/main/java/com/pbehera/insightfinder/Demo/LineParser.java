package com.pbehera.insightfinder.Demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class LineParser {
	
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
	
	String sourceJsonFile;
	String destinationCSVFile;
	Map<String, InstanceStats> instantStatsMap = new HashMap<String, InstanceStats>();
	Set<String> fieldNames = new TreeSet<String>();
	
	String jsonString;
	
	public void getAttributeNames(String jsonString)
	{

		//System.out.println("String is: " + jsonString);
		JsonElement jElement = null;
		Gson gson = new Gson();
		jElement = new JsonParser().parse(jsonString);
		
		JsonObject  jObject = jElement.getAsJsonObject().getAsJsonObject("payload").
				getAsJsonArray("datasets").get(0).getAsJsonObject().getAsJsonObject("data").getAsJsonObject("metadata");
		
		JsonArray jArray = jObject.getAsJsonArray("keys");
		fieldNames.addAll(gson.fromJson(jArray, ArrayList.class));

		
		fieldNames.remove("assetId");
		fieldNames.remove("timestamp");
		
	}
	
	public LineParser(String jsonString)
	{
		getAttributeNames(jsonString);
	}
	
	
	String generateFinalFormat(String jsonString) throws ParseException
	{
		Date startTime = new Date();
		Date endTime = new Date();
		
		int count = 0;
		
		List<String> csvRows = new ArrayList<>();
		MetricDataRow metricDataRow = new MetricDataRow(jsonString);
		
		for(List<Object> dataEntry:metricDataRow.metricValues)
		{
			csvRows.add(getCSVRow(dataEntry, metricDataRow.metricIndexMap));
			//System.out.println("CSV Row: " + getCSVRow(dataEntry, metricDataRow.metricIndexMap));
		}
		
		
		List<String> fieldNames = new ArrayList<String>(Arrays.asList(getCSVHeader().split(",")));
		
		System.out.println("CSV Header: " + getCSVHeader());
		System.out.println("Size of FieldNames: "+fieldNames.size());
		System.out.println("FieldNames: "+fieldNames.toArray());
		
		
		
		Map<InstanceDataPoint, List<Object>> finalData = new HashMap<>();
		
		for(String line: csvRows)
		{
			List<Object> fieldValues = new ArrayList<Object>(Arrays.asList(line.split(",")));
			Date timestamp = df.parse((String) fieldValues.get(0));
			
			InstanceDataPoint instanceDataPoint = new InstanceDataPoint(fieldValues.get(1).toString(), timestamp);
			
			if(count == 0)
			{
				startTime = timestamp;
				endTime = timestamp;
				count++;
			}
			
			if(startTime.after(timestamp))
				startTime = timestamp;
			

			if(endTime.before(timestamp))
				endTime = timestamp;
			
			
			if(finalData.keySet().contains(instanceDataPoint))
			{
				for(int i=2; i<fieldValues.size(); i++)
				{
					if(!fieldValues.get(i).equals("Nan"))
					{
						finalData.get(instanceDataPoint).set(i, fieldValues.get(i).toString());
					}
				}
			}
			else
			{
				List<Object> newValues = new ArrayList<Object>();
				for(int i=0; i<fieldValues.size(); i++)
				{
					newValues.add(fieldValues.get(i));
				}
				
				finalData.put(instanceDataPoint, newValues);	
			}
			
		}
		
		
		List<String> topInstances = findTopInstances(finalData, 75);
		System.out.println("Top Instance contains: " + topInstances.get(0));
		
		StringBuilder sb = new StringBuilder();
		sb.append("timestamp");
		
		int counter = 0;
		
		for(int k=0; k<topInstances.size(); k++)
		{
			String instance = topInstances.get(k);
			for(int i=2; i < fieldNames.size(); i++)
			{
				sb.append(COMMA_DELIMITER);
				sb.append(fieldNames.get(i).replace(':', '-') +"["+instance+"]:"+getGroupId(fieldNames.get(i).replace(':', '-')));
				counter++;
			}
		}
		
		sb.append(NEW_LINE_SEPARATOR);
		
		System.out.println("Total count of columns: " + counter++);
		
		for(Date timestamp: findTimestamps(finalData))
		{
			
			
			sb.append(String.valueOf(timestamp.getTime()));
			
			//System.out.println("Size of TopInstance: " + topInstances.size());
			
			for(int k=0; k<topInstances.size(); k++)
			{
				String instance = topInstances.get(k);
				InstanceDataPoint instanceDataPoint = new InstanceDataPoint(instance, timestamp);
				if(finalData.keySet().contains(instanceDataPoint))
				{
					List<Object> data = finalData.get(instanceDataPoint);
					
					for(int i=2; i < fieldNames.size(); i++)
					{
						sb.append(COMMA_DELIMITER);
						sb.append(data.get(i).toString());
					}
				}
				else
				{
					for(int i=2; i < fieldNames.size(); i++)
					{
						sb.append(COMMA_DELIMITER);
					}
				}
			}
			//System.out.println("Adding Line: " + sb.toString());
			sb.append(NEW_LINE_SEPARATOR);
		}		

		//System.out.println("The Output is: " + sb.toString());
		return sb.toString();
	}
	
	
	public static Map<String, Integer> findInstanceOccurenceMapWithAllDataAvaiable(Map<InstanceDataPoint, List<Object>> finalData)
	{
		Map<String, Integer> instanceOccurenceMap = new HashMap<>();
		
		for(InstanceDataPoint instanceDataPoint: finalData.keySet())
		{
			if(allDataAvailable(finalData.get(instanceDataPoint)))
			{
				if(instanceOccurenceMap.keySet().contains(instanceDataPoint.getAssetId()))
					instanceOccurenceMap.put(instanceDataPoint.getAssetId(), instanceOccurenceMap.get(instanceDataPoint.getAssetId())+1);
				else
					instanceOccurenceMap.put(instanceDataPoint.getAssetId(),1);
			}
		}
		
		return instanceOccurenceMap;
	}
	
	public static boolean allDataAvailable(List<Object>list)
	{
		for(Object element: list)
		{
			if(element.equals("Nan"))
			{
				return false;
			}
		}
		
		return true;
	}
	
	
	public static List<String> findTopInstances(Map<InstanceDataPoint, List<Object>> finalData, Integer n)
	{
		Map<String, Integer> instanceOccurenceMap = findInstanceOccurenceMapWithAllDataAvaiable(finalData);
		
		TreeMap<Integer, List<String>> numOfDataPoints = new TreeMap<Integer, List<String>>();
		
		for(String assetId: instanceOccurenceMap.keySet())
		{
			Integer numDataPoints = instanceOccurenceMap.get(assetId);
			
			if(numOfDataPoints.containsKey(numDataPoints))
			{
				List<String> instances = numOfDataPoints.get(numDataPoints);
				instances.add(assetId);
			}
			else
			{
				List<String> instances = new ArrayList<String>();
				instances.add(assetId);
				numOfDataPoints.put(numDataPoints, instances);
			}
		}
		
		List<String> instances = new ArrayList<>();
		int count = 0;
		
		for(Integer key: numOfDataPoints.descendingKeySet())
		{
			System.out.println("Num of Data Points: " + key + " Nodes Count: " + numOfDataPoints.get(key).size());
			if(count > n)
				break;
			count += numOfDataPoints.get(key).size();
			instances.addAll(numOfDataPoints.get(key));
			
		}
		
		return instances;
		
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
	
	public static Integer getGroupId(String fieldname)
	{
		switch(fieldname)
		{
		case "cpu-usedPercent.avg":
			return 1;
		case "cpu-usedPercent.max":
			return 1;
		case "cpu-usedPercent.min":
			return 1;
		case "memory-free.avg":
			return 2;
		case "memory-free.max":
			return 2;
		case "memory-free.min":
			return 2;
		case "memory-usedPercent.avg":
			return 3;
		case "memory-usedPercent.max":
			return 3;
		case "memory-usedPercent.min":
			return 3;
		case "fs-size.avg":
			return 4;
		case "fs-size.max":
			return 4;
		case "fs-size.min":
			return 4;
		case "fs-used.avg":
			return 5;
		case "fs-used.max":
			return 5;
		case "fs-used.min":
			return 5;
		case "fs-usedPercent.avg":
			return 6;
		case "fs-usedPercent.max":
			return 6;
		case "fs-usedPercent.min":
			return 6;
		
		}
		return null;
	}
	
	public static List<Date> findTimestamps(Map<InstanceDataPoint, List<Object>> finalData)
	{
		Set<Date> uniqueTimeStamps = new TreeSet<>();
		
		for(InstanceDataPoint instanceDataPoint : finalData.keySet())
		{
			uniqueTimeStamps.add(instanceDataPoint.getTimestamp());
		}
		
		System.out.println("Timestamps : " + uniqueTimeStamps.size());
		return new ArrayList<Date>(uniqueTimeStamps);
	}
	
	public static void main(String[] args) throws FileNotFoundException, ParseException
	{
		
		String sourceJsonFile = "/home/pbehera/data2.txt";
		String destinationJsonFile = "/home/pbehera/testOutput.csv";
		File file = new File(sourceJsonFile);
		Scanner sc = new Scanner(file);
		
		
		
		while(sc.hasNextLine())
		{
			String line = sc.nextLine();
			String jsonString = StringUtils.stripEnd(line,null);
			if (StringUtils.isBlank(line))
				continue;
			System.out.println("String is: " + jsonString);
			
			LineParser lineParser = new LineParser(jsonString);
			
			lineParser.generateFinalFormat(jsonString);
			System.out.println("The generatied csv is: " + lineParser.generateFinalFormat(jsonString));
			break;
			
		}
	}

}
