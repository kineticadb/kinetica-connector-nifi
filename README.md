Kinetica NiFi Connector
=======================

This project is aimed to make Kinetica both a data source and data sink for NiFi.

The documentation can be found at http://www.kinetica.com/docs/6.0/index.html.
The connector specific documentation can be found at:

* ``http://www.kinetica.com/docs/6.0/connectors/nifi_guide.html``

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

 
Installing the Kinetica NiFi Connector into NiFi
------------------------------------------------

Copy the file ``kinetica-connector-nifi/nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-6.0.0.nar`` into the *NiFi* lib directory


Twitter Example Template
------------------------

This project includes a *NiFi* template in the file
``GenericTwitterToGPUdb.xml``.  This shows usage of both
*PutGPUdbFromAttributes* and *GetGPUdbToJSON*.

 
Getting Streaming Data from Kinetica to JSON Files
--------------------------------------------------

1.  Drag a new *Processor* onto the flow

    * Select the type *GetGPUdbToJSON*

2.  *Properties* tab

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This
        will be in the format ``http://<host>:<port>``
        (ex. ``http://172.30.20.231:9191``)
    *   *Table Name*: The name of the table to read from
    *   *Table Monitor URL*: The URL *Kinetica* will be using to forward any new
        data inserted into the above table.  This will be in the format
        ``tcp://<host>:<table_monitor_port>``  (ex. ``tcp://172.30.20.231:9002``)

The output of *GetGPUdbToJSON* is a JSON file containing the record inserted
into the *Kinetica* table.


Saving Data to Kinetica Using NiFi Attributes
---------------------------------------------

1.  Drag a new *Processor* onto the flow:

    * Select the type *PutGPUdbFromAttributes*

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

    *   *Label*: The name of the type created from the schema specified above

4.  Specifying data to be saved into *Kinetica*:

    *   Place processors upstream from this which assigns values to user-defined
        attributes named ``gpudb.<field name>``, where ``<field name>`` is the
        name of a field in your table.
    *   Each record written to your table will contain field values of:

        * the value in the attributes with names ``gpudb.<field name>`` or
        * the value "" or 0 depending on the field type, if no attribute is found
          with that field name.
  

 
Getting Streaming Data from Kinetica to CSV Files
=================================================

1.  Drag a new *Processor* onto the flow

    * Select the type *GetGPUdb*
  
2.  *Settings* tab:

    * Under *Auto terminate Relationships*, check the *success* option.

3.  *Properties* tab:

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This will
        be in the format ``http://<host>:<port>``  (ex.
        ``http://172.30.20.231:9191``)
    *   *Table Name*: The name of the table to read from
    *   *Table Monitor URL*: The URL *Kinetica* will be using to forward any new
        data inserted into the above table.  This will be in the format
        ``tcp://<host>:<table_monitor_port>``  (ex. ``tcp://172.30.20.231:9002``)

The output of *GetGPUdb* processor is a CSV, where the first line represents the
schema and subsequent lines contain the data.
  
  
  
Saving Data to Kinetica Using CSV Files
=======================================

1.  Drag a new Processor onto the flow

    * Select the type *PutGPUdb*

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

4.  Create a connector between the data source processor and the PutGPUdb processor

    *   *Details* tab: check the *with coordinates* option.
  
The input for the *PutGPUdb* processor is a CSV of the same format as the output
of *GetGPUdb*


