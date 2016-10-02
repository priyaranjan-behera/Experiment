package com.pbehera.insightfinder.Demo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TestClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		

		try {
			
			Json2CSVParser json2csvParser = new Json2CSVParser("/home/pbehera/data2.txt", "/home/pbehera/output2.csv");
			json2csvParser.generateCSVFile();
			
			for(String assetId: json2csvParser.instantStatsMap.keySet())
			{
				//System.out.println("Asset: " + assetId + " Num. of Instance" + json2csvParser.instantStatsMap.get(assetId).getNumOfDataPoints());
				//System.out.println("Map is: " + json2csvParser.instantStatsMap.get(assetId).getMissingMetricData());
			}
			
			TreeMap<Integer, List<String>> numOfDataPoints = new TreeMap<Integer, List<String>>();
			
			for(String assetId: json2csvParser.instantStatsMap.keySet())
			{
				Integer numInstance = json2csvParser.instantStatsMap.get(assetId).getNumOfDataPoints();
				if(numOfDataPoints.containsKey(numInstance))
				{
					List<String> instances = numOfDataPoints.get(numInstance);
					instances.add(assetId);
				}
				else
				{
					List<String> instances = new ArrayList<String>();
					instances.add(assetId);
					numOfDataPoints.put(numInstance, instances);
				}
			}
			
			int count = 0;
			
			System.out.println("Top Instances are: ");
			
			
			for(Integer key: numOfDataPoints.descendingKeySet())
			{
				if(count > 100)
					break;
				count += numOfDataPoints.get(key).size();
				for(String node: numOfDataPoints.get(key))
				{
					System.out.println("Asset Id: " + node);
					System.out.println("Num. of Instances: " + json2csvParser.instantStatsMap.get(node).getNumOfDataPoints());
					System.out.println("Map with missing values is: " + json2csvParser.instantStatsMap.get(node).getMissingMetricData());
				}
				
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

}
