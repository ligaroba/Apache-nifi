# Apache Nifi
Extending Nifi's Functionalities by adding new components 

Custom Processors 
1.  Convert JSON To Cassandra Counter column UPSERT Statement 
  
    This processor add functionality of working with cassandra Counter tables.The problem solved in this context is to 
    reduce Perfomance overheads that Comes with dealing with Large amounts of data.
    
    ## Problem Faced     
    Was trying to Load large amounts of data into a counter table, i was forced to use split text 
    processor to for me to Evaluate JSON path the json object as atrribute then use Replace text to generate a custom 
    UPSERT statement e.g 
    
    fromSource-->QueryRecord-->SplitText {Splits to single flowfile}-->EvaluateJSONPath-->ReplaceText-->PutCassandraSQL 
        
    OUTPUT
    UPDATE keyspace.tablename SET counter_col1=counter_col1 + number ... WHERE primary_keyCol1= 
    value ...
    
    Reason for splitting to single flow files was because ReplaceText pick attributes values using Expression language,
    Attributes stores only one record at a time so if you have n records in a file only one of the record will be available 
    from the Attribute of that flowfile
    
    ## Solution
  
    I created a Custom Convert JSON to Cassandra Counter UPSERT SQL That does batch update statement that takes in a 
    batch flowfile content and generates UPSERT statements for all the flowfile records 
    
    fromSource-->QueryRecord-->ConvertJSONToCassandraCounterSQL-->PutCassandraSQL 
    
    OUTPUT
    UPDATE keyspace.tablename SET counter_col1=counter_col1 + number ... WHERE primary_keyCol1= 
        value ...
    
