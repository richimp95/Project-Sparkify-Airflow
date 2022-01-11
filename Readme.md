
# SPARKIFY PROJECT 5


## Purpose

The purpose of this project is to consolidate the user and songs played information obtained from the Sparkify app. It consists of 1 fact table of song plays, 4 dimension tables consisting of users in the app, songs in the music DB, artist in the DB and the timestamps of the records, and 2 staging tables, staging_events which has all the Sparkify event information and the staging_songs with has all its songs information. This code was all made to be used with the Airflow and the AWS Readshift ecosystem.


## Tables Description

### Fact Table

1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
    * *songplay_id*
    * *start_time*
    * *user_id*
    * *level*
    * *song_id*
    * *artist_id*
    * *session_id*
    * *location*
    * *user_agent*

    
### Dimension Tables

1. **users** - users in the app
    * *user_id*
    * *first_name*
    * *last_name*
    * *gender*
    * *level*
1. **songs** - songs in music database
    * *song_id*
    * *title*
    * *artist_id*
    * *year*
    * *duration*
1. **artists** - artists in music database
    * *artist_id*
    * *name*
    * *location*
    * *latitude*
    * *longitude*
1. **time** - timestamps of records in songplays broken down into specific units
    * *start_time*
    * *hour*
    * *day*
    * *week*
    * *month*
    * *year*
    * *weekday*

### Staging Tables

1. **staging_events** - staging events in the app
    * *artist*
    * *auth*
    * *first_name*
    * *gender*
    * *itemInSession*
    * *last_name*
    * *length*
    * *level*
    * *location*
    * *method*
    * *page*
    * *registration*
    * *sessionId*
    * *song*
    * *status*
    * *ts*
    * *userAgent*
    * *userId*

1. **staging_songs** - staging songs in the app
    * *num_songs*
    * *artist_id*
    * *artist_latitude*
    * *artist_longitude*
    * *artist_location*
    * *artist_name*
    * *song_id*
    * *title*
    * *duration*
    * *year*


## Files in Repository

### main_dag.py

Contains the ETL divided in operators with an specific order.

### stage_redshift.py

Operator that loads JSON formatted files from S3 to Redshift.

### load_fact.py

Operator that loads the data from the staging tables to the fact table. 

### load_dimension.py

Operator that loads the data from a fact table to a dimention.


### data_quality.py

Operator that checks the data correctness.
