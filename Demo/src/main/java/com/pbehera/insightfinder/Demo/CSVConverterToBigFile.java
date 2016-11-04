package com.pbehera.insightfinder.Demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class CSVConverterToBigFile {

	public static String sourceCSVFile = "/home/pbehera/output1.csv";
	public static String sourceCSVFile2 = "/home/pbehera/output2.csv";
	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
	public static String destinationCSVFile = "/home/pbehera/output_big.csv";
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
	
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
	
	
	public static Integer findNumOfTimestamps(Map<InstanceDataPoint, List<Object>> finalData)
	{
		Set<Date> uniqueTimeStamps = new HashSet<>();
		
		for(InstanceDataPoint instanceDataPoint : finalData.keySet())
		{
			uniqueTimeStamps.add(instanceDataPoint.getTimestamp());
		}
		
		return uniqueTimeStamps.size();
	}
	
	
	public static List<Date> findTimestamps(Map<InstanceDataPoint, List<Object>> finalData)
	{
		Set<Date> uniqueTimeStamps = new TreeSet<>();
		
		for(InstanceDataPoint instanceDataPoint : finalData.keySet())
		{
			uniqueTimeStamps.add(instanceDataPoint.getTimestamp());
		}
		
		return new ArrayList<Date>(uniqueTimeStamps);
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
	
	
	public static Map<String, Integer> findInstanceOccurenceMap(Map<InstanceDataPoint, List<Object>> finalData)
	{
		Map<String, Integer> instanceOccurenceMap = new HashMap<>();
		
		for(InstanceDataPoint instanceDataPoint: finalData.keySet())
		{
			if(instanceOccurenceMap.keySet().contains(instanceDataPoint.getAssetId()))
				instanceOccurenceMap.put(instanceDataPoint.getAssetId(), instanceOccurenceMap.get(instanceDataPoint.getAssetId())+1);
			else
				instanceOccurenceMap.put(instanceDataPoint.getAssetId(),1);

		}
		
		return instanceOccurenceMap;
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

	
	public static void main(String[] args) throws ParseException, IOException
	{
		Date startTime = df.parse("2016-07-15T21:15:00");
		Date endTime = df.parse("2016-07-15T21:15:00");
		generateOutputCSVFile(sourceCSVFile, sourceCSVFile2, startTime, endTime, destinationCSVFile);
	}
	
	static void generateOutputCSVFile(String sourceCSVFile, String sourceCSVFile2, Date startTime, Date endTime, String destinationCSVFile) throws IOException, ParseException
	 {
		// TODO Auto-generated method stub
		
		File file = new File(sourceCSVFile);
		Scanner sc = new Scanner(new File(sourceCSVFile));
		File file2 = new File(sourceCSVFile2);
		Scanner sc2 = new Scanner(new File(sourceCSVFile2));
		
		
		String line = sc.nextLine();
		
		List<String> fieldNames = new ArrayList<String>(Arrays.asList(line.split(",")));
		
		Map<InstanceDataPoint, List<Object>> finalData = new HashMap<>();
		
		int count = 0;
		
		while(sc.hasNextLine())
		{
			line = sc.nextLine();
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
		
		line = sc2.nextLine();
		
		while(sc2.hasNextLine())
		{
			line = sc2.nextLine();
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
		
		for(int i=0; i<10; i++)
		{
			InstanceDataPoint instanceDataPoint = new ArrayList<>(finalData.keySet()).get(i);
			System.out.print("Instance: " + instanceDataPoint.getAssetId());
			System.out.print(" TimeStamp: " + df.format(instanceDataPoint.getTimestamp()));
			
			for(Object data: finalData.get(instanceDataPoint))
			{
				System.out.print(" " + data.toString() + " ");
			}
			
			System.out.println(" ");

			
		}
		
		System.out.println("Start Time is: " + df.format(startTime));
		System.out.println("End Time is: " + df.format(endTime));
		System.out.println("Unique TimeStamps: " + findNumOfTimestamps(finalData));
		
		Map<String, Integer> instanceOccurenceMap = findInstanceOccurenceMapWithAllDataAvaiable(finalData);
		System.out.println("Instance Occurence Map Size All Data Available" + instanceOccurenceMap.keySet().size());
		

		Map<String, Integer> instanceOccurenceMapWithoutAllData = findInstanceOccurenceMap(finalData);
		System.out.println("Instance Occurence Map Size All Data Available" + instanceOccurenceMapWithoutAllData.keySet().size());
		
		
		System.out.println("Top 10 Instances are: " + findTopInstances(finalData, 10));
		
		System.out.println("Num of Instance: " + findTopInstances(finalData, 100000000).size());
		System.out.println("Num of metrics: " + fieldNames.size());
		List<String> topInstances = findTopInstances(finalData, 75);
		for(String assetId: topInstances)
		{
			System.out.println("InstanceId: " + assetId + " InstanceCount: " + instanceOccurenceMap.get(assetId));
		}
		

			FileWriter fileWriter = new FileWriter(destinationCSVFile);
			
			fileWriter.append("timestamp");
			
			int counter = 0;
			
			for(int k=0; k<topInstances.size(); k++)
			{
				String instance = topInstances.get(k);
				for(int i=2; i < fieldNames.size(); i++)
				{
					fileWriter.append(COMMA_DELIMITER);
					fileWriter.append(fieldNames.get(i).replace(':', '-') +"["+instance+"]:"+getGroupId(fieldNames.get(i).replace(':', '-')));
					counter++;
				}
			}
			
			System.out.println("Total count of columns: " + counter++);
			fileWriter.append(NEW_LINE_SEPARATOR);
			
			for(Date timestamp: findTimestamps(finalData))
			{
				fileWriter.append(String.valueOf(timestamp.getTime()));
				
				for(int k=0; k<topInstances.size(); k++)
				{
					String instance = topInstances.get(k);
					InstanceDataPoint instanceDataPoint = new InstanceDataPoint(instance, timestamp);
					if(finalData.keySet().contains(instanceDataPoint))
					{
						List<Object> data = finalData.get(instanceDataPoint);
						
						for(int i=2; i < fieldNames.size(); i++)
						{
							fileWriter.append(COMMA_DELIMITER);
							fileWriter.append(data.get(i).toString());
						}
					}
					else
					{
						for(int i=2; i < fieldNames.size(); i++)
						{
							fileWriter.append(COMMA_DELIMITER);
						}
					}
				}
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
			
			fileWriter.flush();
			fileWriter.close();
		
		}
		
		

}
