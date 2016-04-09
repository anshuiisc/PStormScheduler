# PStormScheduler

- Project includes special spout that reads events from CSV file as rows  for emitting using timestamp column in input CSV file 

- Takes arguments as the X-factor (0.5x,3x) to increase or dicrease the input rate in spout using same CSV file as input 
  (0.5 as arg to double the rate)

- How to USE -

    In local mode  args passed in IDE -
    
    example
    ```
   
    <Topo-mode L-local,C-cluster>   <topo name>   <input CSV file path>    <datatype-ExperimentID>  <rate>   <output log file path>
    
    L   IdentityTopology   /Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/test/java/operation/output/eventDist.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp
    ```
  
  
   In global mode args passed - 
   
   example-
   ```
   
   <storm command>  jar  <jar file path>   <class path>   <Topo-mode>   <toponame>  <csv data path >  <PLUG-expid>  <rate>  <log file path>
    
   storm jar $stormJarpath  in.dream_lab.bm.uidai.auth.topology.$i  C  $i  $plugdatapath   PLUG-$expNum   1.0     $outputpath
   
   ```
   
   
- Limitations 
   
  - Must pass   expid  folllowed by PLUG keyword  it is  being used to identify data type while emitting - <PLUG-expid>   (will correct this soon)
  - Timestamp  must be in milliseconds
  - sample csv file  entries - for more columns please add  them after these 2 columns
    
    
    | msgId        | timestamp           |
| ------------- |:-------------:| 
| 1      | 1443033000 | 
| 2      | 1443033001     | 
| 3 | 14430330002    | 

    
  - After cloning the code please   comment the below line in pom.xml to run in IDE and  reverse to run in Cluster mode 
  
  ```
  <scope>provided</scope>  <!-- IMPORTANT -->
  ```
  - Note 
   Input CSV file must be present in all the locations of  cluster nodes , because we dont know in case of default scheduler  where spout is going to run
  
