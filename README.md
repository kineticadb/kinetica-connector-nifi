Kinetica NiFi Connector
=======================

This project is aimed to make Kinetica both a data source and data sink for NiFi.

The documentation can be found at http://www.kinetica.com/docs/5.4/index.html. The connector specific documentation can be found at:

*   www.kinetica.com/docs/5.4/connectors/nifi_guide.html

For changes to the connector API, please refer to CHANGELOG.md.  For changes
to Kinetica functions, please refer to CHANGELOG-FUNCTIONS.md.


==================================================
NiFi Connector Developer Manual
==================================================

The following guide provides step by step instructions to get started using Kinetica as a data source to read from and write to.

Building the Kinetica NiFi Connector
====================================
1. Change directories to gpudb-connector-nifi and build with Maven ::

		mvn clean package

	
Installing the Kinetica NiFi Connector into NiFi
================================================
Copy the file gpudb-connector-nifi/nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-1.0-SNAPSHOT.nar into the NiFi lib directory
	

Twitter Example Template	
==========================
This project includes a NiFi template in the file GenericTwitterToGPUdb.xml.  This shows usage of both PutGPUdbFromAttributes and GetGPUdbToJSON.

	
Getting Streaming Data from Kintecia to JSON files
===================================================
1.	Drag a new Processor onto the flow
		- Select the type GetGPUdbToJSON		

2.	In the Properties tab:
		- Server URL: The URL of the Kinetica instance you are using.  This will be in the format http://<Kinetica host>:<Kinetica port>.  Ex. http://172.30.20.231:9191
		- Table Name: The name of the table to read from
		- Table Monitor URL: The URL Kinetica will be using to forward any new data inserted into the above table.  This will be in the format tcp://<Kinetica host>:<Monitor port>. Ex. tcp://172.30.20.231:9002
	
The output of GetGPUdbToJSON is a JSON file containing the record inserted into the Kinetica table.


Saving Data to Kinetica using NiFi Attributes
==============================================
1.	Drag a new Processor onto the flow
		- Select the type PutGPUdbFromAttributes		

2.	In the Settings tab:
		- Under "Auto terminate Relationships", check the "failure" and "success" options.

3.	Properties tab:
		- Server URL: The URL of the Kinetica instance you are using.  This will be in the format http://<Kinetica host>:<Kinetica port>  ex. http://172.30.20.231:9191
		- Collection Name: Set this value if you want the table created in a collection.
		- Table Name: The name of the table to read from
		- Schema: A CSV string, where each entry is of the form <fieldname>|<data type>[| subtype]* ex. x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search,AUTHOR|String|text_search|data,URL|String|store_only
				  For more details on schemas, read the Kinetica documentation.
		- Label: The name of the Kinetica type created from the schema specified above

4.	Specifying data to be saved into Kinetica:
		- Place processors upstream from this which assigns values to user-defined attributes named gpudb.<field name>, where <field name> is the name of a field in your Kinetica table.
		- Each record written to your Kinetica table will contain field values of:
			- the value in the attributes with names gpudb.<field name> or
			- the value "" or 0 depending on the field type if no attribute is found with that field name.
		



	
Getting Streaming Data from Kinetica to CSV files
===================================================
1.	Drag a new Processor onto the flow
		- Select the type GetGPUdb		
		
2.	In the Settings tab:
		- Under "Auto terminate Relationships", check the "success" option.

3.	In the Properties tab:
		- Server URL: The URL of the Kinetica instance you are using.  This will be in the format http://<Kinetica host>:<Kinetica port>.  Ex. http://172.30.20.231:9191
		- Table Name: The name of the table to read from
		- Table Monitor URL: The URL Kinetica will be using to forward any new data inserted into the above table.  This will be in the format tcp://<Kinetica host>:<Monitor port>. Ex. tcp://172.30.20.231:9002

The output of GetGPUdb processor is a CSV, where the first line represents the schema and subsequent lines contain the data		
		
		
		
Saving Data to Kinetica using CSV files
=====================================
1.	Drag a new Processor onto the flow
		- Select the type PutGPUdb		

2.	In the Settings tab:
		- Under "Auto terminate Relationships", check the "failure" and "success" options.

3.	Properties tab:
		- Server URL: The URL of the Kinetica instance you are using.  This will be in the format http://<Kinetica host>:<Kinetica port>  ex. http://172.30.20.231:9191
		- Collection Name: Set this value if you want the table created in a collection.
		- Table Name: The name of the table to read from
		- Schema: A CSV string, where each entry is of the form <fieldname>|<data type>[| subtype]* ex. x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search,AUTHOR|String|text_search|data,URL|String|store_only
				  For more details on schemas, read the Kinetica documentation.

4.	Create a connector between the data source processor and the PutGPUdb processor
	In the Details tab:
		Check the "with coordinates" option.
		
The input for the PutGPUdb processor is a CSV of the same format as the output of GetGPUdb


