This project is aimed to make GPUdb both a data source and data sink for NiFi.


==================================================
NiFi Connector Developer Manual
==================================================

The following guide provides step by step instructions to get started using GPUdb as a data source to read from and write to.

Building the GPUdb NiFi Connector
===========================
1. Change directories to gpudb-connector-nifi and build with Maven ::

		mvn clean package

	
Installing the GPUdb NiFi Connector into NiFi
===========================
Copy the file gpudb-connector-nifi/nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-1.0-SNAPSHOT.nar into the NiFi lib directory
	
Getting Streaming Data from GPUdb
===========================
1.	Drag a new Processor onto the flow
		- Select the type GetGPUdb		
		
2.	In the Settings tab:
		- Under "Auto terminate Relationships", check the "success" option.

3.	In the Properties tab:
		- Server URL: The URL of the GPUdb instance you are using.  This will be in the format http://<GPUdb host>:<GPUdb port>.  Ex. http://172.30.20.231:9191
		- Table Name: The name of the table to read from
		- Table Monitor URL: The URL GPUdb will be using to forward any new data inserted into the above table.  This will be in the format tcp://<GPUdb host>:<Monitor port>. Ex. tcp://172.30.20.231:9002

The output of GetGPUdb processor is a CSV, where the first line represents the schema and subsequent lines contain the data		
		
		
		
Saving Data to GPUdb
===========================
1.	Drag a new Processor onto the flow
		- Select the type PutGPUdb		

2.	In the Settings tab:
		- Under "Auto terminate Relationships", check the "failure" and "success" options.

3.	Properties tab:
		- Server URL: The URL of the GPUdb instance you are using.  This will be in the format http://<GPUdb host>:<GPUdb port>  ex. http://172.30.20.231:9191
		- Collection Name: Set this value if you want the table created in a collection.
		- Table Name: The name of the table to read from
		- Schema: A CSV string, where each entry is of the form <fieldname>|<data type>[| subtype]* ex. x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search,AUTHOR|String|text_search|data,URL|String|store_only
				  For more details on schemas, read the GPUdb documentation.

4.	Create a connector between the data source processor and the PutGPUdb processor
	In the Details tab:
		Check the "with coordinates" option.
		
The input for the PutGPUdb processor is a CSV of the same format as the output of GetGPUdb


