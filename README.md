# nifi
Extending Nifi's Functionalities by adding new components 

Custom Processors 
1.  Convert JSON To Cassandra Counter column UPSERT Statement 
    This processor add functionality of working with cassandra Counter tables.The problem solved in this context is to 
    reduce Perfomance overheads that Comes with dealing with Large amounts of data.
    
    ## Problem Faced     
    Was trying to Load large amounts of data into a counter table, iwas forced to use split text 
    processor to for me to Evaluate JSON path the json object as atrribute then use Replace text to generate a custom 
    UPSERT statement e.g UPDATE keyspace.tablename SET counter_col1=counter_col1 + number ... WHERE primary_keyCol1= 
    value ...
    
    ##Solution 
    I created a Custom Convert JSON to Cassandra Counter UPSERT SQL That does batch update statement that takes in a 
    batch flowfile content and generates UPSERT statements for all the flowfile records 
    
