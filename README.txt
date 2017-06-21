Purposes:
    Create application which will predict most popular places for ride or destination in NYC
    (for example data for NYC taxis http://www.andresmh.com/nyctaxitrips/)
    Input params: time, from or to place
    Output: load zone factor (he can divide map by zones, actually it is already divided) use for this spark
    Spark should be used!

Input dta format (csv file):
    medallion,hack_license,vendor_id,rate_code,store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude
    89D227B655E5C82AECF13C3F540D4CF4,BA96DE419E711691B9445D6A6307C170,CMT,1,N,2013-01-0115:11:48,2013-01-0115:18:10,4,382,1.00,-73.978165,40.757977,-73.989838,40.751171
    0BD7C8F5BA12B88E0B67BED28BEA73D8,9FD8F69F0804BDB5549F40E9DA1BE472,CMT,1,N,2013-01-0600:18:35,2013-01-0600:22:54,1,259,1.50,-74.006683,40.731781,-73.994499,40.75066
    0BD7C8F5BA12B88E0B67BED28BEA73D8,9FD8F69F0804BDB5549F40E9DA1BE472,CMT,1,N,2013-01-0518:49:41,2013-01-0518:54:23,1,282,1.10,-74.004707,40.73777,-74.009834,40.726002
    DFD2202EE08F7A8DC9A57B02ACB81FE2,51EE87E3205C985EF8431D850C786310,CMT,1,N,2013-01-0723:54:15,2013-01-0723:58:20,2,244,.70,-73.974602,40.759945,-73.984734,40.759388
    DFD2202EE08F7A8DC9A57B02ACB81FE2,51EE87E3205C985EF8431D850C786310,CMT,1,N,2013-01-0723:25:03,2013-01-0723:34:24,1,560,2.10,-73.97625,40.748528,-74.002586,40.747868
    20D9ECB2CA0767CF7A01564DF2844A3E,598CCE5B9C1918568DEE71F43CF26CD2,CMT,1,N,2013-01-0715:27:48,2013-01-0715:38:37,1,648,1.70,-73.966743,40.764252,-73.983322,40.743763
    496644932DF3932605C22C7926FF0FE0,513189AD756FF14FE670D10B92FAF04C,CMT,1,N,2013-01-0811:01:15,2013-01-0811:08:14,1,418,.80,-73.995804,40.743977,-74.007416,40.744343

After running app (SpringBootSparkApplication):
    App available at: http://localhost:3613/
    SparkUI available at http://localhost:4040

More input data are available at: https://archive.org/download/nycTaxiTripData2013/trip_data.7z

Links:
    https://www.youtube.com/watch?v=XLSQJQjmFFw
    https://habrahabr.ru/company/jugru/blog/325070/
    http://spark.apache.org/docs/2.1.0/
    https://github.com/SpringOne2GX-2014/SparkForSpring
    https://www.darrinward.com/lat-long/