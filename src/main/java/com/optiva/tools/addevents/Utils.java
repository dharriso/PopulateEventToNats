package com.optiva.tools.addevents;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
    /**
     * Compute a partition ID, either for event context or for the event table partitions
     * @throws NoSuchAlgorithmException */
    public static int computeCustomerPartitionId( String rootCustomerId, int numberOfPartitions) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.reset();
        md.update(rootCustomerId.concat(" ").getBytes());
        byte[] result = md.digest();
        int res = 0;

        for ( int i = 7; i >= 0; i-- ) {
            int d = (int) result[ i ];
            if ( d < 0 ) {
                d = -d;
            }
            res += res * numberOfPartitions + d;
        }
        if ( res < 0 ) {
            res = -res;
        }
        return(res % numberOfPartitions);
    }
}
