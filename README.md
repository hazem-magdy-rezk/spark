project Introduction : 

    this project is for a music streaming startup, 
    it aims to build an ETL pipeline that extracts their data from S3, 
    processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 
    This will allow their analytics team to continue finding insights in what songs their users are listening to.

data schema:

Fact Table

    songplays - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

files in repository:
        
      song_data    : the song data & log data in Json format
      log_data     : the log data in Json format
      dl.cfg       : the config credentials
      etl.py       : the python script
      
how to run python script : 
    1- open terminal 
    2- type 'python etl.py' then press enter.
