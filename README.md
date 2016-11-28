# Crossengage_ds_longato
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

# Aplication Deploy:

1. Define the root directory for the app
    a. cd ~/your/app/
    b. git clone https://github.com/mauriciolongato/crossengage_ds_longato.git
    c. cd ./crossengage_ds_longato

2. Go to configkeys.py and insert your user keys from Twitter Streaming API
    - Get information about: https://dev.twitter.com/streaming/public
    - In order to get the keys: https://apps.twitter.com/

3. At the prompt: in ~/crossengage_ds_longato/:
    a. docker-compose build (It will build a the images for the App)
    b. docker-compose up -d (It will start the process)
    c. docker ps (Check if The conteiner is running)

4. In order to stop the execution
    a. docker-compose stop (It will gracefully stop container)

# Parameter throgh command-line and checking the results:

1. After the comand docker-compose up -d, the aplication is already running using a test paramenter in order to test. 
So how do I pass the parameter through command line? 
Go into the Dockerfile and you will see in the last line theres a comand CMD ~ you will find a test comand there 

2. Using your web browser go to localhost:8080

3. After a few minutes there will apear the last last peak