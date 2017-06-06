package oaa.taxi.domain;

/**
 * @author aovcharenko date 30-05-2017.
 */
public enum Fields {
    IN_TS("pickup_datetime"),
    OUT_TS("dropoff_datetime"),
    PASSENGER_COUNT("passenger_count"),
    IN_X("pickup_longitude"),
    IN_Y("pickup_latitude"),
    OUT_X("dropoff_longitude"),
    OUT_Y("dropoff_latitude"),

    MEDALLION("medallion"),
    HACK_LICENSE("hack_license"),
    VENDOR_ID("vendor_id"),
    RATE_CODE("rate_code"),
    STORE_AND_FWD_FLAG("store_and_fwd_flag"),
    TRIP_TIME_IN_SECS("trip_time_in_secs"),
    TRIP_DISTANCE("trip_distance"),

    SECONDS("seconds"),
    IN_SECONDS("in_seconds"),
    OUT_SECONDS("out_seconds"),
    X("x"),
    Y("y"),
    IN_X_INDEX("in_x_index"),
    IN_Y_INDEX("in_y_index"),
    OUT_X_INDEX("out_x_index"),
    OUT_Y_INDEX("out_y_index"),;

    private String name;

    Fields(String name) {
        this.name = name;
    }

    public java.lang.String getName() {
        return name;
    }

    public static class Constants {
        public static final String[] uselessFields = new String[] {
            MEDALLION.getName(),
            HACK_LICENSE.getName(),
            VENDOR_ID.getName(),
            RATE_CODE.getName(),
            STORE_AND_FWD_FLAG.getName(),
            TRIP_TIME_IN_SECS.getName(),
            TRIP_DISTANCE.getName(),
        };
        public static final String[] uselessFields2 = new String[] {
            IN_X.getName(),
            IN_Y.getName(),
            OUT_X.getName(),
            OUT_Y.getName(),
            IN_TS.getName(),
            OUT_TS.getName(),
        };
        public static final String[] uselessFields3_out = new String[] {
            MEDALLION.getName(),
            HACK_LICENSE.getName(),
            VENDOR_ID.getName(),
            RATE_CODE.getName(),
            STORE_AND_FWD_FLAG.getName(),
            TRIP_TIME_IN_SECS.getName(),
            TRIP_DISTANCE.getName(),

            OUT_X.getName(),
            OUT_Y.getName(),
            OUT_TS.getName(),
        };

        public static final String[] uselessFields3_in = new String[] {
            MEDALLION.getName(),
            HACK_LICENSE.getName(),
            VENDOR_ID.getName(),
            RATE_CODE.getName(),
            STORE_AND_FWD_FLAG.getName(),
            TRIP_TIME_IN_SECS.getName(),
            TRIP_DISTANCE.getName(),

            IN_X.getName(),
            IN_Y.getName(),
            IN_TS.getName(),
        };
    }
}
