Kinetica NiFi Connector
=======================

This project is aimed to make Kinetica both a data source and data sink for NiFi.

The documentation can be found at http://www.kinetica.com/docs/6.2/index.html.
The connector specific documentation can be found at:

* ``http://www.kinetica.com/docs/6.2/connectors/nifi_guide.html``

For changes to the connector API, please refer to CHANGELOG.md.  For changes
to Kinetica functions, please refer to CHANGELOG-FUNCTIONS.md.

-----

NiFi Connector Developer Manual
===============================

The following guide provides step by step instructions to get started using
*Kinetica* as a data source to read from and write to.  Source code for the
connector can be found at:

* ``https://github.com/kineticadb/kinetica-connector-nifi``


Building the Kinetica NiFi Connector
------------------------------------

Change directories to ``kinetica-connector-nifi`` and build with *Maven*::

    mvn clean package
    
Note: If you are using a different version of NiFi, change the pom.xml file and run 'mvn clean package'
	<parent>
        <groupId>org.apache.nifi</groupId>
        <artifactId>nifi-nar-bundles</artifactId>
        <version>1.3.0</version>
    </parent>

 
Installing the Kinetica NiFi Connector into NiFi
------------------------------------------------

Copy the file ``kinetica-connector-nifi/nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-1.3.0.nar`` into the *NiFi* lib directory

 
Getting Streaming Data from Kinetica to JSON or CSV Files
--------------------------------------------------

1.  Drag a new *Processor* onto the flow

    * Select the type *GetKineticaToJSON* or *GetKineticabToCSV*

2.  *Properties* tabs

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This
        will be in the format ``http://<host>:<port>``
        (ex. ``http://172.30.20.231:9191``)
    *   *Table Name*: The name of the table to read from
    *   *Table Monitor URL*: The URL *Kinetica* will be using to forward any new
        data inserted into the above table.  This will be in the format
        ``tcp://<host>:<table_monitor_port>``  (ex. ``tcp://172.30.20.231:9002``)

The output of *GetKineticaToJSON* is a JSON file containing the record inserted
into the *Kinetica* table.

The output of *GetKineticaToCSV* is a CSV file containing the record inserted
into the *Kinetica* table.

Saving Data to Kinetica Using NiFi Attributes
---------------------------------------------

1.  Drag a new *Processor* onto the flow:

    * Select the type *PutKinetica*

2.  *Settings* tab:

    * Under *Auto terminate Relationships*, check the *failure* and *success* options.

3.  *Properties* tab:

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This
        will be in the format ``http://<host>:<port>``
        (ex. ``http://172.30.20.231:9191``)
    *   *Collection Name*: Set this value if you want the table created in a collection.
    *   *Table Name*: The name of the table to read from
    *   *Schema*: A CSV string, where each entry is of the form ``<fieldname>|<data type>[| subtype]*``
        For example:

            X|Float|data,Y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search
            
    *   Batch Size - Kinetica uses bulk load semantics. The Batch Size tells the processor 
    	group and compress batches for efficient loading
    *	Username - if Kinetica authentication is enabled, a username is required
    *	Password - if Kinetica authentication is enabled, a password is required
    *	Update on Existing PK - If a Primary Key (PK) is defined for a table then Kinetica has
    	two options on how to handle new rows coming in with a duplicate PK. If "true", Kinetica
    	will perform an upsert, if "false" Kinetica will ignore the new row completely
    *	Replicate Table - Tables can be distributed or replicated in Kinetica. If set to "true",
    	the table will be replicated across all servers in the cluster. If "false", the table will
    	be distributed across all servers
	*	Date Format - if your data contains a datetime field, you will need to convert it to a long.
		Provide the date format so it can be parsed. Example: 'dd-MM-yyyy hh:mm:ss'
	*	TimeZone - Provide the timezone if the date is not from your local timezone

4.  Specifying data to be saved into *Kinetica*:

    *   Place processors upstream from this which assigns values to user-defined
        attributes named ``<field name>``, where ``<field name>`` is the
        name of a field in your table.
    *   Each record written to your table will contain field values of:

        * the value in the attributes with names ``<field name>`` or
        * the value of null if no attribute is found with that field name.
  
Saving Data to Kinetica Using Delimited Files
=============================================

1.  Drag a new Processor onto the flow

    * Select the type *PutKineticaFromFile*

2.  *Settings* tab:

    * Under *Auto terminate Relationships*, check the *failure* and *success*
      options.

3.  *Properties* tab:

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This
        will be in the format ``http://<host>:<port>``  (ex.
        ``http://172.30.20.231:9191``)
    *   *Collection Name*: Set this value if you want the table created in a
        collection.
    *   *Table Name*: The name of the table to read from
    *   *Schema*: A CSV string, where each entry is of the form
        ``<fieldname>|<data type>[| subtype]*``
        For example:

            X|Float|data,Y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search

        For more details on schemas, read the *Kinetica* documentation.
        
    *   *Delimiter* - Delimited files can be comma, tab, pipe etc. 
    * 	Batch Size - Kinetica uses bulk load semantics. The Batch Size tells the processor 
    	group and compress batches for efficient loading
    *	Error Handling - Many large files contain bad data. This processor can skip bad rows
    	if Error Handling is set to "true". If "false", the file will stop loading if errors occur.
    *	Username - if Kinetica authentication is enabled, a username is required
    *	Password - if Kinetica authentication is enabled, a password is required
    *	Update on Existing PK - If a Primary Key (PK) is defined for a table then Kinetica has
    	two options on how to handle new rows coming in with a duplicate PK. If "true", Kinetica
    	will perform an upsert, if "false" Kinetica will ignore the new row completely
    *	Replicate Table - Tables can be distributed or replicated in Kinetica. If set to "true",
    	the table will be replicated across all servers in the cluster. If "false", the table will
    	be distributed across all servers
	*	Date Format - if your data contains a datetime field, you will need to convert it to a long.
		Provide the date format so it can be parsed. Example: 'dd-MM-yyyy hh:mm:ss'
	*	TimeZone - Provide the timezone if the date is not from your local timezone
    	

4.  Create a connector between the data source processor and the PutKineticaFromFile processor

    *   *Details* tab: check the *with coordinates* option.
  
The input for the *PutKineticaFromFile* processor is a delimited file


