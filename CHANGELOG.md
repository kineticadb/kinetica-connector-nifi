# GPUdb NiFi Connector Changelog

## Version 7.1

### Version 7.1.0.0 - 2020-07-27

#### Changed
-   Updated version to 7.1


## Version 7.0

### Version 7.0.3.0 - 2019-05-03

#### Changed
-   Updated the Kinetica Java API version to 7.0.3.0 to take advantage of
    recent changes (support for HA failover for multi-head I/O).

### Version 7.0.2.0 - 2019-04-12

#### Changed
-   Modified the PutKineticaFromFile processor to divert bad records/lines
    from CSV files to a failure relationship.

## Version 6.2

### Version 6.2.0 - 2018-05-16
-   Added the ability to customize the following for the PutKineticaFromFile
    processor (which loads a CSV file into Kinetica):
    -   Delimiter
    -   Quote character
    -   Escape character


## Version 6.1.0 - 2017-10-05

-   Maintenance


## Version 5.2.0 - 2016-06-25

-   Maintenance.


## Version 5.1.0 - 2016-05-06

-   Updated pom.xml and imports for new GPUdb API structure.


## Version 4.2.0 - 2016-04-11

-   Initial version
