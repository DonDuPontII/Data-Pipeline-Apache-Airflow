class SqlQueries:
    songplay_table_insert = ("""
        SELECT DISTINCT
           md5(events.sessionid || events.start_time) songplay_id,
           events.start_time, 
           events.userid, 
           events.level, 
           songs.song_id, 
           songs.artist_id, 
           events.sessionid, 
           events.location, 
           events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
              FROM staging_events
              WHERE page='NextSong') as events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        select distinct
            staging_events.userid,
            firstname,
            lastname,
            gender,
            level
        from staging_events 
        join (select 
                userid, 
                max(ts) as ts
              from staging_events
              where page = 'NextSong'
              group by userid) as recent_ts
            on staging_events.userid = recent_ts.userid
                and staging_events.ts = recent_ts.ts
    """)

    song_table_insert = ("""
        SELECT distinct
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        select distinct
           staging_songs.artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
        from staging_songs
        join (select  
               artist_id,
               max(song_id) as song_id
              from staging_songs
              group by artist_id) as id
             on staging_songs.artist_id = id.artist_id
                 and staging_songs.song_id = id.song_id
    """)

    time_table_insert = ("""
        SELECT DISTINCT
            start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time), 
            extract(month from start_time),
            extract(year from start_time),
            extract(dayofweek from start_time)
        FROM songplays
    """)