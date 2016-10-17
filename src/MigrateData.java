
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Pankaj
 */
public class MigrateData {

    public static void main(String[] args) throws UnknownHostException, IOException, ParseException {
        Socket cassandraConnection = new Socket("91.121.168.138", Integer.parseInt("4242"));
        PrintStream output = new PrintStream(cassandraConnection.getOutputStream());


        String[] symbols = new String[]{"L&T", "M&M", "P&G", "S&SIND", "S&SPOWER", "SB&TINTL", "J&KBANK", "IT&T",
            "AXIS-IT&T", "M&MFIN", "AREVAT&D", "COX&KINGS", "HYDROS&S", "L&TFINANCE", "SURANAT&P", "IL&FSENGG",
            "IL&FSTRANS", "ALSTOMT&D", "L&TFH", "L&TINFRA", "GET&D",
            "HCL-HP", "D-LINK", "HCL-INSYS", "MCDOWELL-N", "MID-DAY",
            "MRO-TEK", "UTIUS64-CI", "I-FLEX", "E-SERVEINT", "AXIS-IT&T", "SHIV-VANI",
            "TV-18", "BAJAJ-AUTO", "WABCO-TVS", "CORAL-HUB", "SHIRPUR-G", "TVS-SUZUKI",
            "UTIUS64-RI", "IPRU-8484", "IDFC-D1216"};

        String[] metrics = {"india.nse.equity.s4.daily.open", "india.nse.equity.s4.daily.high", "india.nse.equity.s4.daily.low",
            "india.nse.equity.s4.daily.settle", "india.nse.equity.s4.daily.close",
            "india.nse.equity.s4.daily.volume", "india.nse.equity.s4.daily.delivered"};

        String[] metricsf = {"india.nse.future.s4.daily.open", "india.nse.future.s4.daily.high", "india.nse.future.s4.daily.low",
            "india.nse.future.s4.daily.settle", "india.nse.future.s4.daily.close",
            "india.nse.future.s4.daily.volume", "india.nse.future.s4.daily.oi"};

        String[] metricso = {"india.nse.option.s4.daily.open", "india.nse.option.s4.daily.high", "india.nse.option.s4.daily.low",
            "india.nse.option.s4.daily.settle", "india.nse.option.s4.daily.close",
            "india.nse.option.s4.daily.volume", "india.nse.option.s4.daily.oi"};

        boolean migrateFutures = false;
        boolean migrateOptions = true;
        boolean migrateStocks = false;

        if (migrateFutures) {
            for (String s : symbols) {
                System.out.println(s);

                String shorts = s.replaceAll(" ", "").replaceAll("&", "");
                if (!shorts.equals(s)) {
                    shorts = shorts.toLowerCase();

                    Object[] expiries = Utilities.getExpiriesFromKDB(shorts, 0L, 1476529827000L, "india.nse.future.s4.daily.settle");

                    //Migrate Futures
                    //Object[] expiries = Utilities.getExpiriesFromKDB(shorts, 0L, 1476529827000L, "india.nse.future.s4.daily.settle");
                    if (expiries != null && expiries.length > 0) {
                        for (Object expiry : expiries) {
                            TreeMap<Long, String> data = new TreeMap<>();
                            for (String metric : metricsf) {
                                data = Utilities.getPrices(shorts, expiry.toString(), null, null, new Date(0), new Date(), metric);
                                for (Map.Entry<Long, String> entry : data.entrySet()) {
                                    NSEData.Cassandra(entry.getValue(), entry.getKey(), metric, s.toLowerCase(), expiry.toString(), null, null, output);
                                }
                            }
                        }
                    }
                }
            }
        }
        //Migrate options

        if (migrateOptions) {
            for (String s : symbols) {
                System.out.println(s);

                String shorts = s.replaceAll(" ", "").replaceAll("&", "");
                if (!shorts.equals(s)) {
                    shorts = shorts.toLowerCase();
                    Object[] expiries = Utilities.getExpiriesFromKDB(shorts, 0L, 1476529827000L, "india.nse.option.s4.daily.settle");

                    if (expiries != null && expiries.length > 0) {
                        for (Object expiry : expiries) {
                            System.out.println("Symbol:"+s.toString()+",Expiry:"+expiry.toString());
                            Date endDate = new SimpleDateFormat("yyyyMMdd").parse(expiry.toString());
                            endDate = Utilities.addDays(endDate, 1);
                            Date startDate = Utilities.addDays(endDate, -100);
                            if (expiry.toString().matches("^00.*")) {
                                System.out.println(expiry.toString());
                                for (String metric : metricso) {
                                    Utilities.deleteData(shorts.toString(), expiry.toString(), null, null, startDate, endDate, metric);
                                }
                            } else {
                                Object[] strikes = Utilities.getOptionStrikesFromKDB(shorts, expiry.toString(), startDate.getTime(), endDate.getTime(), "india.nse.option.s4.daily.settle");
                                for (Object strike : strikes) {
                                    System.out.println("Copying Strike:" + strike.toString());
                                    TreeMap<Long, String> data = new TreeMap<>();
                                    for (String metric : metricso) {
                                        data = Utilities.getPrices(shorts, expiry.toString(), strike.toString(), "CALL", startDate, endDate, metric);
                                        for (Map.Entry<Long, String> entry : data.entrySet()) {
                                            NSEData.Cassandra(entry.getValue(), entry.getKey(), metric, s.toLowerCase(), expiry.toString(), strike.toString(), "CALL", output);
                                        }
                                    }
                                    for (String metric : metricso) {
                                        data = Utilities.getPrices(shorts, expiry.toString(), strike.toString(), "PUT", startDate, endDate, metric);
                                        for (Map.Entry<Long, String> entry : data.entrySet()) {
                                            NSEData.Cassandra(entry.getValue(), entry.getKey(), metric, s.toLowerCase(), expiry.toString(), strike.toString(), "PUT", output);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if (migrateStocks) {
            TreeMap<Long, String> data = new TreeMap<>();
            for (String s : symbols) {
                System.out.println(s);
                for (String metric : metrics) {
                    data = Utilities.getPrices(s, null, null, null, new Date(0), new Date(), metric);
                    for (Map.Entry<Long, String> entry : data.entrySet()) {
                        NSEData.Cassandra(entry.getValue(), entry.getKey(), metric, s, null, null, null, output);
                    }
                }
            }

        }
    }
}
/*
 Object[] allSymbols = Utilities.getSymbolsFromKDB(0L, 1476529827000L, "india.nse.option.s4.daily.settle");
 for (Object s1 : allSymbols) {
 System.out.println(s1.toString());
 Object[] expiries = Utilities.getExpiriesFromKDB(s1.toString(), 0L, 1476529827000L, "india.nse.option.s4.daily.settle");
 for (Object expiry : expiries) {
 if (expiry.toString().matches("^00.*")) {
 //delete data
 System.out.println(expiry.toString());
 for (String metric : metricso) {
 Utilities.deleteData(s1.toString(), expiry.toString(), null, null, new Date(0), new Date(), metric);
 }
 }
 }
 }
 */
