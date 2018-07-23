

package com.excellenceengineeringsolutions;

import org.junit.Test;

import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class GcloudDate {

    @Test
    public void t1() {

        Calendar calendar = Calendar.getInstance();
        //public final static int DECEMBER = 11;
        calendar.set(Calendar.MONTH, Calendar.JANUARY);

        //java.lang.IllegalArgumentException: Invalid month: 0
        //Preconditions.checkArgument(month > 0 && month <= 12, "Invalid month: " + month);
        com.google.cloud.Date gDate = com.google.cloud.Date
                .fromYearMonthDay(2018, calendar.get(Calendar.MONTH), 1);

        // Month value is 0-based. e.g., 0 for January.
        calendar.set(2018, 05, 01);
    }

    @Test
    public void t2(){
        /*
         * @param   month   the month between 0-11.
         */
        java.util.Date date = new java.util.Date();
        assertEquals(4, date.getMonth()); // 4 - May (the 5th month)
    }
}
