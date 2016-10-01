package com.pbehera.insightfinder.Demo;

import java.util.HashMap;
import java.util.Map;

public class InstanceStats {
	
	String instanceId;
	Integer numOfDataPoints;
	Map<String, Integer> missingMetricData;
	
	public InstanceStats() {
		numOfDataPoints = 0;
		missingMetricData = new HashMap<String, Integer>();
	}
	
	public String getInstanceId() {
		return instanceId;
	}
	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}
	public Integer getNumOfDataPoints() {
		return numOfDataPoints;
	}
	public void setNumOfDataPoints(Integer numOfDataPoints) {
		this.numOfDataPoints = numOfDataPoints;
	}
	public Map<String, Integer> getMissingMetricData() {
		return missingMetricData;
	}
	public void setMissingMetricData(Map<String, Integer> missingMetricData) {
		this.missingMetricData = missingMetricData;
	}
	
	

}
