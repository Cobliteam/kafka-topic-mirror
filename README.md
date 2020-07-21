# Kafka Topic Mirror

A console app that diff and mirror topics metadata from a kafka to another

## WARNINGS
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
--bootstrap-servers-src <String: hosts>  REQUIRED: The connection string for  
                                           the source kafka connection in the 
                                           form host:port.                    
--dry-run                                Just list topics differences without 
                                           mirroring anything.                
--help                                   Print usage information.
```