package com.pbehera.insightfinder.Demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class OutputGenerator {

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";

	public static Node getMetricValue(Document document, String metric, String value)
	{
		Element node = document.createElement(metric);
		node.appendChild(document.createTextNode(value));
		return node;
	}

	public static Node createOutputElement(Document document, Map<String,Map<Date, MetricEntry>> compiledData, Date timestamp)
	{
		Element observation = document.createElement("Observation");
		observation.setAttribute("Timestamp", df.format(timestamp));
		for(String metric: compiledData.keySet())
		{	
			if(compiledData.get(metric).containsKey(timestamp))
				observation.appendChild(getMetricValue(document, metric, compiledData.get(metric).get(timestamp).getAverage()));
		}

		return observation;
	}

	public void generateCSVFile(Map<String,Map<Date, MetricEntry>> compiledData, String filename) throws Exception
	{
		FileWriter fileWriter = new FileWriter(filename);

		try
		{
			StringBuilder fileHeader = new StringBuilder();


			fileHeader.append("Timestamp in GMT");

			for(String metric: compiledData.keySet())
			{
				fileHeader.append(COMMA_DELIMITER);
				fileHeader.append(metric);
			}

			fileHeader.append(NEW_LINE_SEPARATOR);

			fileWriter.append(fileHeader.toString());

			Calendar cal = Calendar.getInstance();
			//Date testStart = (Date) new ArrayList(compiledData.get("CPUUtilization").keySet()).get(0);
			//Date testEnd = (Date) new ArrayList(compiledData.get("CPUUtilization").keySet()).get(new ArrayList(compiledData.get("CPUUtilization").keySet()).size()-1);


			Date testStart = df.parse("2016-07-15T21:15:00Z");
			Date testEnd = df.parse("2016-07-19T08:25:00Z");

			cal.setTime(testStart);

			while(cal.getTime().before(testEnd))
			{
				StringBuilder dataRow = new StringBuilder(df.format(cal.getTime()));

				for(String metric : compiledData.keySet())
				{	
					dataRow.append(COMMA_DELIMITER);
					if(compiledData.get(metric).containsKey(cal.getTime()))
						dataRow.append(compiledData.get(metric).get(cal.getTime()).getAverage());
				}

				dataRow.append(NEW_LINE_SEPARATOR);
				fileWriter.append(dataRow.toString());
				cal.add(Calendar.MINUTE, 5);
			}


		}catch(Exception e)
		{
			throw e;
		}
		finally {
			fileWriter.flush();
			fileWriter.close();
			System.out.println("CSV Generated Successfully");
		}
	}

	public void generateXMLFile(Map<String,Map<Date, MetricEntry>> compiledData, String filename) throws TransformerException, ParserConfigurationException, FileNotFoundException, ParseException
	{
		FileOutputStream outputStream = new FileOutputStream(new File(filename));
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;

		dBuilder = dbFactory.newDocumentBuilder();
		Document document = dBuilder.newDocument();

		Element mainRootElement = document.createElementNS("http://thho.net/files/insightfinder/techtest/metrics.json", "Metrics");
		document.appendChild(mainRootElement);

		Calendar cal = Calendar.getInstance();
		//Date testStart = (Date) new ArrayList(compiledData.get("CPUUtilization").keySet()).get(0);
		//Date testEnd = (Date) new ArrayList(compiledData.get("CPUUtilization").keySet()).get(new ArrayList(compiledData.get("CPUUtilization").keySet()).size()-1);


		Date testStart = df.parse("2016-07-15T21:15:00Z");
		Date testEnd = df.parse("2016-07-19T08:25:00Z");

		cal.setTime(testStart);

		while(cal.getTime().before(testEnd))
		{
			mainRootElement.appendChild(createOutputElement(document, compiledData, cal.getTime()));
			cal.add(Calendar.MINUTE, 5);
		}

		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes"); 
		DOMSource source = new DOMSource(document);
		transformer.transform(source, new StreamResult(outputStream));

		System.out.println("XML DOM Created Successfully..");


	}

	public void generateJsonFile(Map<String,Map<Date, MetricEntry>> compiledData, String filename) throws Exception
	{
		FileWriter fileWriter = new FileWriter(filename);
		
		try
		{

		
		JsonObject observations = new JsonObject();
		observations.addProperty("Title", "Metrics");

		JsonArray jsonArray = new JsonArray();

		Calendar cal = Calendar.getInstance();

		Date testStart = df.parse("2016-07-15T21:15:00Z");
		Date testEnd = df.parse("2016-07-19T08:25:00Z");

		cal.setTime(testStart);

		while(cal.getTime().before(testEnd))
		{
			JsonObject record = new JsonObject();
			record.addProperty("Timestamp", df.format(cal.getTime()));
			JsonObject observation = new JsonObject();
			for(String metric : compiledData.keySet())
			{	
				if(compiledData.get(metric).containsKey(cal.getTime()))
					observation.addProperty(metric, compiledData.get(metric).get(cal.getTime()).getAverage());
			}
			record.add("Observation", observation);
			jsonArray.add(record);
			cal.add(Calendar.MINUTE, 5);
		}
		
		observations.add("Observations", jsonArray);
		
		Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
        fileWriter.write(gson.toJson(observations));


		

		}catch(Exception e)
		{
			throw e;
		}
		finally {
			
			fileWriter.flush();
			fileWriter.close();
			System.out.println("Json Created Successfully..");
		}

	}

}
