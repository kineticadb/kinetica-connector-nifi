Kinetica NiFi Connector
=======================

This project is aimed to make Kinetica both a data source and data sink for NiFi.

The documentation can be found at http://www.kinetica.com/docs/7.0/index.html.
The connector specific documentation can be found at:

* ``http://www.kinetica.com/docs/7.0/connectors/nifi_guide.html``

For changes to the connector API, please refer to CHANGELOG.md.  For changes
to Kinetica functions, please refer to CHANGELOG-FUNCTIONS.md.

-----

NiFi Connector Developer Manual
===============================

The following guide provides step by step instructions to get started using
*Kinetica* as a data source to read from and write to.  Source code for the
connector can be found at:

* <https://github.com/kineticadb/kinetica-connector-nifi>


Building the Kinetica NiFi Connector
------------------------------------

The connector jar can be built with *Maven*.

1. Download the connector source::

        $ git clone https://github.com/kineticadb/kinetica-connector-nifi.git
        $ cd kinetica-connector-nifi

2. If using a version of *NiFi* other than *1.3.0*, update the ``pom.xml`` file
   with the correct version of *NiFi* in this block::

        <parent>
            <groupId>org.apache.nifi</groupId>
            <artifactId>nifi-nar-bundles</artifactId>
            <version>1.3.0</version>
        </parent>

3. Build the connector jar::

        $ mvn clean package


Installing the Kinetica NiFi Connector into NiFi
------------------------------------------------

Deploy the connector jar built in the previous step to the *NiFi* libraries
directory::

        $ cp nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-1.3.0.nar <NiFiHome>/lib


Getting Streaming Data from Kinetica to JSON or CSV Files
---------------------------------------------------------

1.  Drag a new *Processor* onto the flow

    *   Select the *GetKineticaToJSON* or *GetKineticaToCSV* type

2.  *Properties* tab

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This
        will be in the format ``http://<host>:<port>``
        (ex. ``http://172.10.20.30:9191``)
    *   *Table Name*: The name of the table to read from
    *   *Table Monitor URL*: The URL *Kinetica* will be using to forward any new
        data inserted into the above table.  This will be in the format
        ``tcp://<host>:<table_monitor_port>``  (ex. ``tcp://172.10.20.30:9002``)
    *   *Delimiter*: For CSVs, the delimiter used in the file (e.g., comma, tab,
        pipe, etc.); defaults to tab
    *   *Username*: *Kinetica* login username; required if authentication is
        enabled
    *   *Password*: *Kinetica* login password; required if authentication is
        enabled

The output of *GetKineticaToJSON* is a JSON file containing the record inserted
into the *Kinetica* table.

The output of *GetKineticaToCSV* is a CSV file containing the record inserted
into the *Kinetica* table.


Saving Data to Kinetica Using NiFi Attributes
---------------------------------------------

1.  Drag a new *Processor* onto the flow:

    *   Select the *PutKinetica* type

2.  *Settings* tab:

    *   Under *Auto terminate Relationships*, check the *failure* and *success*
        options.

3.  *Properties* tab:

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This
        will be in the format ``http://<host>:<port>``
        (ex. ``http://172.10.20.30:9191``)
    *   *Collection Name*: Set this value if you want the table created in a
        collection.
    *   *Table Name*: The name of the table to write to
    *   *Schema*: A CSV string, where each entry is of the form
        ``<fieldname>|<data type>[|<subtype>]*``.
        For example::

            X|Float|data,Y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search

        For more details on schemas, read the *Kinetica* documentation.

    *   *Batch Size*: The size of the batch to compress for efficient loading
    *   *Username*: *Kinetica* login username; required if authentication is
        enabled
    *   *Password*: *Kinetica* login password; required if authentication is
        enabled
    *   *Update on Existing PK*: If a *primary key (PK)* is defined for a table,
        then there are two options for handling each new record pending insert
        that has a PK value matching an existing record in the target table.  If
        set to ``true``, the record in the target table will be updated with the
        new record's values; if ``false``, the new record will be discarded;
        defaults to ``false``
    *   *Replicate Table*: If ``true``, the target table will be *replicated*;
        if ``false``, the table will be *distributed*; defaults to ``false``
    *   *Date Format*: The date format to use to parse values in any *datetime*
        fields (e.g., ``dd-MM-yyyy hh:mm:ss``)
    *   *TimeZone*: Provide the timezone if the date is not from your local
        timezone

4.  Specifying data to be saved into *Kinetica*:

    *   Place processors upstream from this which assigns values to user-defined
        attributes named ``<field name>``, where ``<field name>`` is the
        name of a field in your table
    *   Each record written to your table will contain field values of:

        * the value in the attributes with names ``<field name>`` or
        * the value of *null* if no attribute is found with that field name

Saving Data to Kinetica Using Delimited Files
---------------------------------------------

1.  Drag a new Processor onto the flow

    *   Select the *PutKineticaFromFile* type

2.  *Settings* tab:

    *   Under *Auto terminate Relationships*, check the *failure* and *success*
        options.

3.  *Properties* tab:

    *   *Server URL*: The URL of the *Kinetica* instance you are using.  This
        will be in the format ``http://<host>:<port>``
        (ex. ``http://172.10.20.30:9191``)
    *   *Collection Name*: Set this value if you want the table created in a
        collection.
    *   *Table Name*: The name of the table to write to
    *   *Schema*: A CSV string, where each entry is of the form
        ``<fieldname>|<data type>[|<subtype>]*``.
        For example::

            X|Float|data,Y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search

        For more details on schemas, read the *Kinetica* documentation.

    *   *Delimiter*: The delimiter used in the file (e.g., comma, tab, pipe,
        etc.); defaults to ``,``
    *   *Escape Character*: The character used to escape other characters in the
        data (e.g., ``\``); defaults to ``"``
    *   *Quote Character*: The character used to quote column data in the file
        (e.g., ``"`` or ``'``); defaults to ``"``
    *   *File Has Header*: Whether the first line of the file is a header row or
        not; defaults to ``true``
    *   *Batch Size*: The size of the batch to compress for efficient loading
    *   *Error Handling*: If ``true``, the processor will skip rows that can't
        be loaded successfully (due to parse error, etc.); if ``false``, the
        processor will stop loading as soon as an error occurs; defaults to
        ``true``
    *   *Username*: *Kinetica* login username; required if authentication is
        enabled
    *   *Password*: *Kinetica* login password; required if authentication is
        enabled
    *   *Update on Existing PK*: If a *primary key (PK)* is defined for a table,
        then there are two options for handling each new record pending insert
        that has a PK value matching an existing record in the target table.  If
        set to ``true``, the record in the target table will be updated with the
        new record's values; if ``false``, the new record will be discarded;
        defaults to ``false``
    *   *Replicate Table*: If ``true``, the target table will be *replicated*;
        if ``false``, the table will be *distributed*; defaults to ``false``
    *   *Date Format*: The date format to use to parse values in any *datetime*
        fields (e.g., ``dd-MM-yyyy hh:mm:ss``)
    *   *TimeZone*: Provide the timezone if the date is not from your local
        timezone


4.  Create a connector between the data source processor and the
    *PutKineticaFromFile* processor

    *   *Details* tab: check the *with coordinates* option.

The input for the *PutKineticaFromFile* processor is a delimited file.
