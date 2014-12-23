

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Date utility
 *
 * $Id$
 */
public class Utilities {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    private static final long MILLI_SEC_PER_DAY = 1000 * 60 * 60 * 24;
    private static final Logger logger = Logger.getLogger(Utilities.class.getName());

    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    public static String toTimeString(long time) {
        return ((time < 1300) ? time / 100 : time / 100 - 12)
                + ":" + time % 100
                + ((time < 1200) ? " AM" : " PM");
    }

      public static Properties loadParameters(String parameterFile) throws FileNotFoundException, IOException {
        Properties p = new Properties();
        FileInputStream propFile;        
            propFile = new FileInputStream(parameterFile);
            p.load(propFile);
        return p;
    }
   
    public static long getDeltaDays(String date) {
        long deltaDays = 0;

        try {
            Date d = sdf.parse(date);
            deltaDays = (d.getTime() - getCurrentTime()) / MILLI_SEC_PER_DAY;
        } catch (Throwable t) {
            System.out.println(" [Error] Problem parsing date: " + date + ", Exception: " + t.getMessage());
            logger.log(Level.SEVERE, null, t);
        }
        return deltaDays;
    }
    // Get  date in given format and default timezone

    public static String getFormatedDate(String format, long timeMS) {
        TimeZone tz = TimeZone.getDefault();
        String date = getFormatedDate(format, timeMS, tz);
        return date;
    }
    
    // Get  date in given format and timezone
    public static String getFormatedDate(String format, long timeMS, TimeZone tmz) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(tmz);
        String date = sdf.format(new Date(timeMS));
        return date;
    }

    //parse the date string in the given format and timezone to return a date object
    public static Date parseDate(String format, String date) {
        Date dt = null;
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat(format);
            dt = sdf1.parse(date);
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
        return dt;
    }

    public static Date parseDate(String format, String date, String timeZone) {
        Date dt = null;
        try {
            TimeZone tz;
            SimpleDateFormat sdf1 = new SimpleDateFormat(format);
            if("".compareTo(timeZone)==0){
                tz=TimeZone.getDefault();
            }else{
                tz=TimeZone.getTimeZone(timeZone);
            }
            sdf1.setTimeZone(tz);
            dt = sdf1.parse(date);
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
        return dt;
    }
    
    public static Date addDays(Date date, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days); //minus number would decrement the days
        return cal.getTime();
    }

    public static Date addSeconds(Date date, int seconds) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.SECOND, seconds); //minus number would decrement the days
        return cal.getTime();
    }

    public static boolean isValidDate(String dateString, SimpleDateFormat inputFormat) {
    if (dateString == null || dateString.length() != inputFormat.toPattern().length()) {
        return false;
    }

    SimpleDateFormat yyyyMMdd=new SimpleDateFormat("yyyyMMdd");
        try {
            Date inputdate=inputFormat.parse(dateString);
            dateString=yyyyMMdd.format(inputdate);
        } catch (ParseException ex) {
            Logger.getLogger(Utilities.class.getName()).log(Level.SEVERE, null, ex);
        }

        int date;
    try {
        date = Integer.parseInt(dateString);
    } catch (NumberFormatException e) {
        return false;
    }

    
    int year = date / 10000;
    int month = (date % 10000) / 100;
    int day = date % 100;

    // leap years calculation not valid before 1581
    boolean yearOk = (year >= 1581) && (year <= 2500);
    boolean monthOk = (month >= 1) && (month <= 12);
    boolean dayOk = (day >= 1) && (day <= daysInMonth(year, month));

    return (yearOk && monthOk && dayOk);
}

private static int daysInMonth(int year, int month) {
    int daysInMonth;
    switch (month) {
        case 1: // fall through
        case 3: // fall through
        case 5: // fall through
        case 7: // fall through
        case 8: // fall through
        case 10: // fall through
        case 12:
            daysInMonth = 31;
            break;
        case 2:
            if (((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0)) {
                daysInMonth = 29;
            } else {
                daysInMonth = 28;
            }
            break;
        default:
            // returns 30 even for nonexistant months 
            daysInMonth = 30;
    }
    return daysInMonth;
}


    //Testing routine
    public static void main(String args[]){
        String out=Utilities.getFormatedDate("yyyy-MM-dd HH:mm:ss",new Date().getTime(),TimeZone.getTimeZone("GMT-4:00"));
        System.out.println(out);
        
    }
}
