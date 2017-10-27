
import com.cedarsoftware.util.io.JsonObject;
import com.cedarsoftware.util.io.JsonReader;
import java.io.BufferedReader;
import com.cedarsoftware.util.io.JsonWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;

/**
 * Date utility
 *
 * $Id$
 */
public class Utilities {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    private static final long MILLI_SEC_PER_DAY = 1000 * 60 * 60 * 24;
    private static final Logger logger = Logger.getLogger(Utilities.class.getName());

   public static TreeMap<Long, String> getPrices(String exchangeSymbol, String expiry, String right, String optionStrike, Date startDate, Date endDate, String metric) {
        TreeMap<Long, String> out = new TreeMap<>();
        try {
            HttpClient client = new HttpClient("http://" + "91.121.168.138" + ":8085");
            String strike = Utilities.formatDouble(Utilities.getDouble(optionStrike, 0), new DecimalFormat("#.##"));
            QueryBuilder builder = QueryBuilder.getInstance();
            String symbol = null;
            //symbol = exchangeSymbol.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
            symbol=exchangeSymbol;
            builder.setStart(startDate)
                    .setEnd(endDate)
                    .addMetric(metric)
                    .addTag("symbol", symbol);
            if (expiry != null && !expiry.equals("")) {
                builder.getMetrics().get(0).addTag("expiry", expiry);
            }
            if (right != null && !right.equals("")) {
                builder.getMetrics().get(0).addTag("option", right);
                builder.getMetrics().get(0).addTag("strike", strike);
            }
            builder.getMetrics().get(0).setOrder(QueryMetric.Order.DESCENDING);
            long time = new Date().getTime();
            QueryResponse response = client.query(builder);

            List<DataPoint> dataPoints = response.getQueries().get(0).getResults().get(0).getDataPoints();
            for (DataPoint dataPoint : dataPoints) {
                long lastTime = dataPoint.getTimestamp();
                out.put(lastTime, dataPoint.getValue().toString());
            }
        } catch (Exception e) {
            logger.log(Level.INFO, null, e);
        }
        return out;
    }

   public static Object[] getOptionStrikesFromKDB(String symbol,String expiry,long startTime, long endTime, String metric){
        Object[] out=new Object[1];
        HashMap<String, Object> param = new HashMap();
        param.put("TYPE", Boolean.FALSE);
        HistoricalRequestJson request = new HistoricalRequestJson(metric,
                new String[]{"symbol", "expiry"},
                new String[]{symbol, expiry},
                null,
                null,
                null,
                String.valueOf(startTime),
                String.valueOf(endTime));
        //http://stackoverflow.com/questions/7181534/http-post-using-json-in-java
        String json_string = JsonWriter.objectToJson(request, param);
        StringEntity requestEntity = new StringEntity(
                json_string,
                ContentType.APPLICATION_JSON);

        HttpPost postMethod = new HttpPost("http://91.121.165.108:8085/api/v1/datapoints/query/tags");
        postMethod.setEntity(requestEntity);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            HttpResponse rawResponse = httpClient.execute(postMethod);
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((rawResponse.getEntity().getContent())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                param.clear();
                param.put("USE_MAPS", "false");
                JsonObject obj = (JsonObject) JsonReader.jsonToJava(output, param);
                JsonObject t = (JsonObject) ((Object[]) obj.get("queries"))[0];
                JsonObject results = (JsonObject) ((Object[]) t.get("results"))[0];
                JsonObject tags = (JsonObject) results.get("tags");
                Object[] strikes=(Object[])tags.get("strike");
                out = strikes; //0 is long time, 1 is value
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }

       
        return out;
    }

   public static Object[] getExpiriesFromKDB(String symbol,long startTime, long endTime, String metric){
        Object[] out=new Object[1];
        HashMap<String, Object> param = new HashMap();
        param.put("TYPE", Boolean.FALSE);
        String strike=null;
        String expiry=null;
        HistoricalRequestJson request = new HistoricalRequestJson(metric,
                new String[]{"symbol"},
                new String[]{symbol},
                null,
                null,
                null,
                String.valueOf(startTime),
                String.valueOf(endTime));
        //http://stackoverflow.com/questions/7181534/http-post-using-json-in-java
        String json_string = JsonWriter.objectToJson(request, param);
        StringEntity requestEntity = new StringEntity(
                json_string,
                ContentType.APPLICATION_JSON);

        HttpPost postMethod = new HttpPost("http://91.121.168.138:8085/api/v1/datapoints/query/tags");
        postMethod.setEntity(requestEntity);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            HttpResponse rawResponse = httpClient.execute(postMethod);
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((rawResponse.getEntity().getContent())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                param.clear();
                param.put("USE_MAPS", "false");
                JsonObject obj = (JsonObject) JsonReader.jsonToJava(output, param);
                JsonObject t = (JsonObject) ((Object[]) obj.get("queries"))[0];
                JsonObject results = (JsonObject) ((Object[]) t.get("results"))[0];
                JsonObject tags = (JsonObject) results.get("tags");
                Object[] expiries=(Object[])tags.get("expiry");
                //int length = expiries.length;
                //Object[] outarray = (Object[]) expiries[length - 1];
                out = expiries; //0 is long time, 1 is value

            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }

       
        return out;
    }
    
   public static Object[] getSymbolsFromKDB(long startTime, long endTime, String metric){
        Object[] out=new Object[1];
        HashMap<String, Object> param = new HashMap();
        param.put("TYPE", Boolean.FALSE);
        String strike=null;
        String expiry=null;
        HistoricalRequestJson request = new HistoricalRequestJson(metric,
                null,
                null,
                null,
                null,
                null,
                String.valueOf(startTime),
                String.valueOf(endTime));
        //http://stackoverflow.com/questions/7181534/http-post-using-json-in-java
        String json_string = JsonWriter.objectToJson(request, param);
        StringEntity requestEntity = new StringEntity(
                json_string,
                ContentType.APPLICATION_JSON);

        HttpPost postMethod = new HttpPost("http://91.121.168.138:8085/api/v1/datapoints/query/tags");
        postMethod.setEntity(requestEntity);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            HttpResponse rawResponse = httpClient.execute(postMethod);
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((rawResponse.getEntity().getContent())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                param.clear();
                param.put("USE_MAPS", "false");
                JsonObject obj = (JsonObject) JsonReader.jsonToJava(output, param);
                JsonObject t = (JsonObject) ((Object[]) obj.get("queries"))[0];
                JsonObject results = (JsonObject) ((Object[]) t.get("results"))[0];
                JsonObject tags = (JsonObject) results.get("tags");
                Object[] symbols=(Object[])tags.get("symbol");
                //int length = expiries.length;
                //Object[] outarray = (Object[]) expiries[length - 1];
                out = symbols; //0 is long time, 1 is value
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }

       
        return out;
    }
   
   public static void deleteData(String exchangeSymbol, String expiry, String right, String optionStrike, Date startDate, Date endDate, String metric)  {
        TreeMap<Long, String> out = new TreeMap<>();
        try {
            HttpClient client = new HttpClient("http://" + "91.121.168.138" + ":8085");
            String strike = Utilities.formatDouble(Utilities.getDouble(optionStrike, 0), new DecimalFormat("#.##"));
            QueryBuilder builder = QueryBuilder.getInstance();
            String symbol = null;
            //symbol = exchangeSymbol.replaceAll("[^A-Za-z0-9-]", "").toLowerCase();
            symbol=exchangeSymbol.toLowerCase();
            builder.setStart(startDate)
                    .setEnd(endDate)
                    .addMetric(metric)
                    .addTag("symbol", symbol);
            if (expiry != null && !expiry.equals("")) {
                builder.getMetrics().get(0).addTag("expiry", expiry);
            }
            if (right != null && !right.equals("")) {
                builder.getMetrics().get(0).addTag("option", right);
                builder.getMetrics().get(0).addTag("strike", strike);
            }
            Response response = client.delete(builder);

        }catch (Exception e){
            logger.log(Level.SEVERE,null,e);
        }
        
   }
   

    public static String formatDouble(double d, DecimalFormat df) {
        return df.format(d);
    }

    public static double getDouble(Object input, double defvalue) {
        try {
            if (isDouble(input.toString())) {
                return Double.parseDouble(input.toString().trim());
            } else {
                return defvalue;
            }
        } catch (Exception e) {
            return defvalue;
        }
    }

    public static boolean isDouble(String value) {
        //String decimalPattern = "([0-9]*)\\.([0-9]*)";  
        //return Pattern.matches(decimalPattern, value)||Pattern.matches("\\d*", value);
        if (value != null) {
            value = value.trim();
            return value.matches("-?\\d+(\\.\\d+)?");
        } else {
            return false;
        }
    }

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
            if ("".compareTo(timeZone) == 0) {
                tz = TimeZone.getDefault();
            } else {
                tz = TimeZone.getTimeZone(timeZone);
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

        SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
        try {
            Date inputdate = inputFormat.parse(dateString);
            dateString = yyyyMMdd.format(inputdate);
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
    public static void main(String args[]) {
        String out = Utilities.getFormatedDate("yyyy-MM-dd HH:mm:ss", new Date().getTime(), TimeZone.getTimeZone("GMT-4:00"));
        System.out.println(out);

    }
}
