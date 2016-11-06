# crossengage_ds_longato
Repo of the CrossEngage Challenge

Structure:
  * main - application entry point
    - creates initial database
    - handles parameters
    - starts, in parallel, twitter_flow, twitter_analyser, and flask_app

  * twitter_flow - responsible to set and manage the connection with Twitter Streaming API:
    - Connect with Twitter API
    - Feed sqlite with data (not all fields from twitter! only the necessary)
  
  * twitter_analyser - connect tt_db and analyse:
    - Spikes in data series from a given streaming
    - Evaluate aggregations on hashtags over timeframes  

  * flask_app
    - consumes processed data in order to display the analysis
     
summary of the project flow:
    main >> DB >> twitter_flow >>  twitter_analyser >> flask_app