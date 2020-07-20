# Kafka Topic Mirror

A console app that diff and mirror topics metadata from a kafka to another

## WARNINGS
**Source kafka must access zookeeper**

**This app does not remove topics**

**This app does not replicate partition reassignments**


## Usage

```bash
$ sbt run
....
Mirror topics between Kafkas.
Option                                   Description                            
------                                   -----------                            
--bootstrap-servers-dst <String: hosts>  REQUIRED: The connection string for    
                                           the destination kafka connection in  
                                           the form host:port.                  
--command-config-property-dst <String:   A mechanism to pass user-defined       
  dst_prop>                                properties in the form key=value to  
                                           the destination kafka Admin Client   
                                           connection. Multiple entries allowed.
--dry-run                                Just list topics differences without   
                                           mirroring anything.                  
--help                                   Print usage information.               
--zookeeper-src <String: hosts>          REQUIRED: The connection string for    
                                           the source zookeeper connection in   
                                           the form host:port. Multiple hosts   
                                           can be given to allow fail-over. ...
```