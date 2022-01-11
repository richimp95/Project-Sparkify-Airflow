class SqlQueries:
    songplay_table_insert = ("""
        insert into {} (start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        (   SELECT
                DATEADD(MILLISECOND, evs.ts % 1000, DATEADD(SECOND, evs.ts / 1000, '19700101')) as start_time, 
                evs.userid, 
                evs.level, 
                songs.song_id, 
                songs.artist_id, 
                evs.sessionid, 
                evs.location, 
                evs.useragent
            FROM staging_events evs
            LEFT JOIN staging_songs songs
            ON evs.song = songs.title
                AND evs.artist = songs.artist_name
            WHERE evs.userid is not NULL
        );
    """)

    user_table_insert = ("""
        insert into {} (

           select distinct
                userId,
                firstname,
                lastname,
                gender,
                level
            from staging_events
            where page = 'NextSong' and userId is not null

        )
    """)

    song_table_insert = ("""
        insert into {} (

           select distinct
                song_id,
                title,
                artist_id,
                year,
                duration
            from staging_songs
            where song_id is not null

        )
    """)

    artist_table_insert = ("""
        insert into {} (

           select distinct
                artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
            from staging_songs
            where artist_id is not null

        )
    """)

    time_table_insert = ("""
        insert into {} (

           select distinct
                start_time, 
                extract(hour from start_time), 
                extract(day from start_time), 
                extract(week from start_time), 
                extract(month from start_time), 
                extract(year from start_time), 
                extract(dayofweek from start_time)
            from songplays
            where start_time is not null

        )
    """)