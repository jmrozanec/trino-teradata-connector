# Presto-Teradata Connector

This is a [PrestoDB](https://prestodb.io/) connector to query data from [Teradata](http://www.teradata.com/)

Queries "walk", but you can help us make them run! ;) You are welcome to stop by and contribute.

[![Gitter Chat](http://img.shields.io/badge/chat-online-brightgreen.svg)](https://gitter.im/jmrozanec/presto-teradata-connector)
[![Project stats by OpenHub](https://www.openhub.net/p/presto-teradata-connector/widgets/project_thin_badge.gif)](https://www.openhub.net/p/presto-teradata-connector/)

## Development

### Set up
We do not provide Teradata JDBC jars, since they require to agree on some terms due to US legislation.
After cloning this repo, you should 

* download Teradata JDBC drivers from [here](https://downloads.teradata.com/download/connectivity/jdbc-driver)
* rename the jars
* place them in the following directories

        $PROJECT_HOME/lib/com/teradata/config/15.10.0/config-15.10.0.jar
        $PROJECT_HOME/lib/com/teradata/jdbc/15.10.0/jdbc-15.10.0.jar


### Building the project
To build Presto Teradata Connector, execute:

    mvn clean install
 

## Presto-Teradata connector installation
### Connection Configuration

Create new properties file inside etc/catalog dir:

    connector.name=teradata
    # connection-url is the Teradata JDBC URL. You may use different configurations per environment.
    # For more information, please visit 
    # [JDBC driver docs](https://developer.teradata.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html)
    connection-url=jdbc:teradata://aaa.bbb.ccc.ddd/TMODE=ANSI,CHARSET=UTF8
	connection-user=someusername
	connection-password=somepassword

To install the connector, copy presto-teradata-{version}.jar and jars at $PROJECT_HOME/presto-dependencies/ directory to some location, ex.: /tmp/teradata-jars/

    cd $PRESTODB_HOME
    mkdir -p plugin/teradata
    cp /tmp/teradata-jars/* plugin/teradata
   
   
## Related resources
Below we list resources related to Prestor connectors. If you wrote a Presto connector to any database, we want to hear from you!

* [Prestogres: connecting postgres to Presto](http://www.slideshare.net/frsyuki/presto-meetup)

## Contribute & Support!

Contributions are welcome! You can contribute by
 * starring this repo!
 * requesting or adding new features.
 * enhancing existing code or documentation.
 * testing.
 * bringing suggestions and reporting bugs.
 * spreading the word / telling us how you use it!
