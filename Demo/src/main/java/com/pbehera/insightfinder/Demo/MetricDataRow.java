package com.pbehera.insightfinder.Demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MetricDataRow {

	List<String> metricNames;
	List<List<Object>> metricValues;
	
	Map<String, Integer> metricIndexMap = new HashMap<String, Integer>();
	
	public void initiaizeMetricIndexMap()
	{
		for(int i=0; i<metricNames.size(); i++)
		{
			metricIndexMap.put(metricNames.get(i), i);
		}
	}
	
	
	public MetricDataRow(String jsonString)
	{
		Gson gson = new Gson();
		
		JsonElement jElement = new JsonParser().parse(jsonString);
		JsonObject  jObject = jElement.getAsJsonObject().getAsJsonObject("payload").
				getAsJsonArray("datasets").get(0).getAsJsonObject().getAsJsonObject("data").getAsJsonObject("metadata");
		
		JsonArray jArray = jObject.getAsJsonArray("keys");
		metricNames = new ArrayList<>();
		for(JsonElement jElement2: jArray)
		{
			metricNames.add(jElement2.getAsString());
		}
		metricNames = gson.fromJson(jArray, ArrayList.class);
		//System.out.println("Metric Names: " + metricNames.toString());
		
		
		jObject = jElement.getAsJsonObject().getAsJsonObject("payload").
				getAsJsonArray("datasets").get(0).getAsJsonObject().getAsJsonObject("data");
		jArray = jObject.getAsJsonArray("values");
		
		metricValues = new ArrayList<List<Object>>();
		
		for(JsonElement jElement2: jArray)
		{
			JsonArray jArray2 = jElement2.getAsJsonArray();
			if(jObject == null)
				break;
			
			List<Object> list = new ArrayList<Object>();
			for(JsonElement jElement3: jArray2)
			{
				list.add(jElement3.getAsString());
			}
			metricValues.add(list);
			//System.out.println("ArrrayList Generated: " + gson.fromJson(jArray2, ArrayList.class).toString());
		}
		
		initiaizeMetricIndexMap();
		//System.out.println("Index Map: " + metricIndexMap.toString());
	}
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append("Keys: " + metricNames.toString());
		sb.append("Key Values: ");
		
		for(List list: metricValues)
		{
			sb.append(list.toString());
		}
		
		return sb.toString();
		
	}

}
