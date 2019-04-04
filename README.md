# Kafka Topic Mirror

A console app that diff and mirror topics metadata from a kafka to another

## WARNINGS
**Source kafka must access zookeeper**

**This app does not remove topics**

**This app does not replicate partition reassignments**


## Usage

```bash
$ sbt assembly 
$ java -jar target/scala-2.12/kafka-topic-mirror.jar 
Diff or mirror topics metadata between kafkas.
Option                                   Description                           
------                                   -----------                           
--bootstrap-servers-dst <String: hosts>  REQUIRED: The connection string for   
                                           the kafka connection in the form    
                                           host:port.
--command-config-property-dst <String:   A mechanism to pass user-defined       
  dst_prop>                                properties in the form key=value to  
                                           the destination kafka Admin Client   
                                           connection. Multiple entries allowed.                                                              
--diff                                   List topics differences.              
--help                                   Print usage information.              
--mirror                                 Change or create topics on destination
--zookeeper-src <String: hosts>          REQUIRED: The connection string for   
                                           the zookeeper connection in the form
                                           host:port. Multiple hosts can be    
                                           given to allow fail-over.           
```