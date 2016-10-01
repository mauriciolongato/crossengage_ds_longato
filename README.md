# crossengage_ds_longato
Repo of the CrossEngage Challenge

Architecture:
  * twitter_flow - Will be responsible to set and manage the connection with Twitter Streaming API:
     - Connect witH Twitter API
     - Feed Sqlite with data (Not all fields from twitter! only the necessary)
  
  * twitter_analyser - Will connect tt_db and analyse:
     - Spikes in data series from a given streaming
     - Evaluate aggregations on hashtags over timeframes  

  * dashboard.html
     - Will consume processed data in order to display the analysis
     
 summary of the pipe architecture:
 twitter listener >> DB >> twitter analyser >> front

In this file I will keep track of failures and architectural changes
