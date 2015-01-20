/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 *
 * @author admin
 */
public class NSEData {

    /**
     * @param args the command line arguments
     */
    private static final Logger logger = Logger.getLogger(NSEData.class.getName());
    private static Connection conn;
    private static String cassandraIP;
    private static String cassandraPort;
    private static String cassandraEquityMetric;
    private static String cassandraIndexMetric;
    private static String cassandraFutureMetric;
    private static String cassandraOptionMetric;
    private static Socket cassandraConnection;
    private static PrintStream output;
    private static String sqlTable;
    private static boolean useSQL = false;
    private static boolean useCassandra = false;
    private static boolean useFile = false;

    public static void main(String[] args) throws MalformedURLException, ClassNotFoundException, SQLException, IOException, ParseException {
        SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMdd");
        FileInputStream configFile;
        Class.forName("com.mysql.jdbc.Driver");
        File f = new File("nsedata.properties");
        if (f.exists() && !f.isDirectory()) {
            Properties p = Utilities.loadParameters(args[1]);
            cassandraIP = p.getProperty("cassandraconnection");
            cassandraPort = p.getProperty("cassandraport");
            cassandraEquityMetric = p.getProperty("equity");
            cassandraFutureMetric = p.getProperty("future");
            cassandraOptionMetric = p.getProperty("option");
            cassandraIndexMetric = p.getProperty("index");
            if (cassandraIP != null && cassandraPort != null) {
                useCassandra = true;
                cassandraConnection = new Socket(cassandraIP, Integer.parseInt(cassandraPort));
                output = new PrintStream(cassandraConnection.getOutputStream());
            }
            String sqlIP = p.getProperty("sqlconnection");
            String sqlDatabase = p.getProperty("sqldatabase");
            sqlTable = p.getProperty("sqltable");
            String sqlUserName = p.getProperty("sqlusername");
            String sqlPassword = p.getProperty("sqlpassword");
            if (sqlIP != null && sqlDatabase != null && sqlTable != null) {
                useSQL = true;
                //conn = DriverManager.getConnection("jdbc:mysql://192.187.112.162:3306/nsedata?rewriteBatchedStatements=true", "psharma", "1history23");
                conn = DriverManager.getConnection("jdbc:mysql://" + sqlIP + "/" + sqlDatabase + "?rewriteBatchedStatements=true", sqlUserName, sqlPassword);

            }
            useFile = p.getProperty("fileoutput") != null ? Boolean.parseBoolean(p.getProperty("fileoutput")) : false;

        }
        try {
            configFile = new FileInputStream("logging.properties");
            LogManager.getLogManager().readConfiguration(configFile);

        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (IOException | SecurityException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        //getSplits();
        usage();
        String[] param = args[0].split("(?!^)");

        for (String s : param) {
            switch (s) {
                case "s":
                    getSymbolChange();
                    break;
                case "e":
                    if (args.length == 2) {
                        String todayDate = Utilities.getFormatedDate("yyyyMMdd", new Date().getTime());
                        System.out.println("Working with today's date: " + todayDate);
                        getStockHistoricalData(todayDate, todayDate);
                    } else if (args.length == 4) {
                        if (Utilities.isValidDate(args[2], inputDateFormat) && Utilities.isValidDate(args[3], inputDateFormat)) {
                            getStockHistoricalData(args[2], args[3]);
                        }
                    }
                    break;
                case "i":
                    if (args.length == 2) {
                        String todayDate = Utilities.getFormatedDate("yyyyMMdd", new Date().getTime());
                        System.out.println("Working with today's date: " + todayDate);
                        getIndicesHistoricalData(todayDate, todayDate);
                    } else if (args.length == 4) {
                        if (Utilities.isValidDate(args[2], inputDateFormat) && Utilities.isValidDate(args[3], inputDateFormat)) {
                            getIndicesHistoricalData(args[2], args[3]);
                        }
                    }
                    break;
                case "f":
                    if (args.length == 2) {
                        String todayDate = Utilities.getFormatedDate("yyyyMMdd", new Date().getTime());
                        System.out.println("Working with today's date: " + todayDate);
                        getFNOHistoricalData(todayDate, todayDate);
                    } else if (args.length == 4) {
                        if (Utilities.isValidDate(args[2], inputDateFormat) && Utilities.isValidDate(args[3], inputDateFormat)) {
                            getFNOHistoricalData(args[2], args[3]);
                        }
                    }
                    break;
                default:
                    usage();
                    break;
            }
        }
    }

    static void usage() {
        System.out.println("usage: java -jar NSETools eif startdate enddate");
        System.out.println("e->imports equity, i->imports indices, f->imports futures and options");
        System.out.println("name of properities file");
        System.out.println("startdate and enddate should be specified in yyyyMMdd format");
        System.out.println("If startdate and endate are not specified, current system date is used");
    }

    static void getFNOHistoricalData(String startDate, String endDate) throws MalformedURLException, IOException, ParseException {
        Calendar start = Calendar.getInstance();
        start.setTime(Utilities.parseDate("yyyyMMdd", startDate));
        Calendar end = Calendar.getInstance();
        end.setTime(Utilities.parseDate("yyyyMMdd", endDate));
        SimpleDateFormat yyyyFormat = new SimpleDateFormat("yyyy");
        SimpleDateFormat ddMMMyyyyFormat = new SimpleDateFormat("ddMMMyyyy");
        SimpleDateFormat yyyyMMddFormat = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat ddMMMyyyHFormat = new SimpleDateFormat("dd-MMM-yyyy");
        for (Date date = start.getTime(); !start.after(end); start.add(Calendar.DATE, 1)) {
            //http://www.nseindia.com/content/historical/DERIVATIVES/2014/DEC/fo22DEC2014bhav.csv.zip
            //http://www.nseindia.com/content/historical/DERIVATIVES/2014/DEC/fo22Dec2014bhav.csv.zip             
            String dateString = ddMMMyyyyFormat.format(start.getTime()).toUpperCase();
            String year = dateString.substring(5, 9);
            String month = dateString.substring(2, 5).toUpperCase();
            String nseTrades = String.format("http://www.nseindia.com/content/historical/DERIVATIVES/%s/%s/fo%sbhav.csv.zip", year, month, dateString);
            URL nseTradesURL = new URL(nseTrades);
            if (getResponseCode(nseTrades) != 404) {
                System.out.println("Parsing URL :" + nseTrades);
                ZipInputStream zin = new ZipInputStream(nseTradesURL.openStream());
                ZipEntry ze = zin.getNextEntry();
                BufferedReader in = new BufferedReader(new InputStreamReader(zin, "UTF-8"));
                String line;
                ArrayList<HistoricalData> h = new ArrayList<>();
                while ((line = in.readLine()) != null) {
                    String symbolData[] = !line.isEmpty() ? line.split(",") : null;
                    if (symbolData != null && symbolData.length >= 15 && !symbolData[1].equals("SYMBOL")) {
                        String symbol = symbolData[1];
                        String open = symbolData[5];
                        String high = symbolData[6];
                        String low = symbolData[7];
                        String close = symbolData[8];
                        String settlePrice = symbolData[9];
                        String volume = symbolData[10];
                        String openInterest = symbolData[12];
                        String expiry = yyyyMMddFormat.format(ddMMMyyyHFormat.parse(symbolData[2]));
                        String strike = symbolData[3];
                        String optionType = symbolData[4];
                        h.add(new HistoricalData(start.getTime(), symbol, open, high, low, close, settlePrice, volume, openInterest, expiry, strike, optionType));
                    }
                }
                //data starts from 20000707
                if (useCassandra) {
                    for (HistoricalData hist : h) {
                        if (hist.optionStrike == null) {
                            if (Double.valueOf(hist.open) > 0) {
                                Cassandra(hist.open, hist.dateFormat.getTime(), cassandraFutureMetric + ".open", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.high, hist.dateFormat.getTime(), cassandraFutureMetric + ".high", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.low, hist.dateFormat.getTime(), cassandraFutureMetric + ".low", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraFutureMetric + ".close", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);

                            } else {
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraFutureMetric + ".open", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraFutureMetric + ".high", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraFutureMetric + ".low", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraFutureMetric + ".close", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);

                            }
                            Cassandra(hist.volume, hist.dateFormat.getTime(), cassandraFutureMetric + ".volume", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.openInterest, hist.dateFormat.getTime(), cassandraFutureMetric + ".oi", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.last, hist.dateFormat.getTime(), cassandraFutureMetric + ".settle", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                        } else {
                            if (Double.valueOf(hist.open) > 0) {
                                Cassandra(hist.open, hist.dateFormat.getTime(), cassandraOptionMetric + ".open", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.high, hist.dateFormat.getTime(), cassandraOptionMetric + ".high", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.low, hist.dateFormat.getTime(), cassandraOptionMetric + ".low", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraOptionMetric + ".close", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            } else {
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraOptionMetric + ".open", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraOptionMetric + ".high", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraOptionMetric + ".low", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                Cassandra(hist.close, hist.dateFormat.getTime(), cassandraOptionMetric + ".close", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            }
                            Cassandra(hist.volume, hist.dateFormat.getTime(), cassandraOptionMetric + ".volume", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.openInterest, hist.dateFormat.getTime(), cassandraOptionMetric + ".oi", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.last, hist.dateFormat.getTime(), cassandraOptionMetric + ".settle", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);

                        }
                    }
                }
            }

        }
    }

    static void getStockHistoricalData(String startDate, String endDate) throws MalformedURLException {

        Calendar start = Calendar.getInstance();
        start.setTime(Utilities.parseDate("yyyyMMdd", startDate));

        Calendar end = Calendar.getInstance();
        end.setTime(Utilities.parseDate("yyyyMMdd", endDate));
        for (Date date = start.getTime(); !start.after(end); start.add(Calendar.DATE, 1)) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                SimpleDateFormat requiredFormat = new SimpleDateFormat("ddMMMyyyy");
                date = start.getTime();
                String dateString = requiredFormat.format(date).toUpperCase();
                String year = dateString.substring(5, 9);
                String month = dateString.substring(2, 5).toUpperCase();
                String day = dateString.substring(0, 2);
                String nseTrades = String.format("http://www.nseindia.com/content/historical/EQUITIES/%s/%s/cm%sbhav.csv.zip", year, month, dateString);
                URL nseTradesURL = new URL(nseTrades);

                if (getResponseCode(nseTrades) != 404) {
                    System.out.println("Parsing URL :" + nseTrades);
                    ZipInputStream zin = new ZipInputStream(nseTradesURL.openStream());
                    ZipEntry ze = zin.getNextEntry();
                    BufferedReader in = new BufferedReader(new InputStreamReader(zin, "UTF-8"));
                    String line;
                    HashMap<String, HistoricalData> h = new HashMap<>();
                    while ((line = in.readLine()) != null) {
                        String symbolData[] = !line.isEmpty() ? line.split(",") : null;
                        try {
                            if (symbolData != null && (symbolData[1].equals("EQ") || symbolData[1].equals("EQ") || symbolData[1].equals("EQ") || symbolData[1].equals("BE") || symbolData[1].equals("EQ") || symbolData[1].equals("BT"))) {
                                //see legend here http://www.nseindia.com/content/equities/eq_serieslist.htm
                                //write to file/database
                                h.put(symbolData[0], new HistoricalData(inputFormat.format(date).toUpperCase(), symbolData[0], symbolData[2], symbolData[3], symbolData[4], symbolData[5], symbolData[6], symbolData[8]));

                            }
                        } catch (Exception e) {
                            System.out.println("Error at Line:" + line);
                            logger.log(Level.SEVERE, null, e);
                        }
                    }
                    //get delivery quantity
                    requiredFormat = new SimpleDateFormat("ddMMyyyy");
                    dateString = requiredFormat.format(date);
                    nseTrades = String.format("http://www.nseindia.com/archives/equities/mto/MTO_%s.DAT", dateString);
                    nseTradesURL = new URL(nseTrades);
                    if (getResponseCode(nseTrades) != 404) {
                        System.out.println("Parsing URL :" + nseTrades);
                        in = new BufferedReader(new InputStreamReader(nseTradesURL.openStream()));
                        int j = 0;
                        int rowsToSkip = 4;
                        while ((line = in.readLine()) != null) {
                            j = j + 1;
                            if (j > rowsToSkip) {
                                String symbolData[] = line.split(",");
                                if (symbolData.length > 5 && symbolData[3].equals("EQ")) {
                                    //System.out.println("Processing deliverable: "+ line);
                                    if (h.get(symbolData[2]) != null) {
                                        h.get(symbolData[2]).deliverable = symbolData[5] == null ? "" : symbolData[5];
                                    } else {
                                        logger.log(Level.INFO, "Processing Deliverable Quantity, No OHLCV found for {0} for date {1}", new Object[]{symbolData[2], dateString});
                                    }
                                } else if (symbolData.length > 1 && symbolData[3].equals("EQ")) {
                                    logger.log(Level.INFO, "Processing Deliverable Quantity, No Deliverable record found for {0} for date {1}", new Object[]{symbolData[2], dateString});
                                }
                            }
                        }
                    }
                    if (useSQL) {
                        writeToSQL(inputFormat.format(date).toUpperCase(), h);
                    }
                    if (useFile) {
                        writeToFile(inputFormat.format(date).toUpperCase(), h);
                    }
                    if (useCassandra) {
                        for (HistoricalData hist : h.values()) {
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                            long time = formatter.parse(hist.date).getTime();
                            Cassandra(hist.open, time, cassandraEquityMetric + ".open", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.high, time, cassandraEquityMetric + ".high", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.low, time, cassandraEquityMetric + ".low", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.last, time, cassandraEquityMetric + ".close", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.volume, time, cassandraEquityMetric + ".volume", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.close, time, cassandraEquityMetric + ".settle", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.deliverable, time, cassandraEquityMetric + ".delivered", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                        }
                    }


                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }
        }
    }

    static void getIndicesHistoricalData(String startDate, String endDate) {
        try {
            //Data in new format is available from 21FEB2012. For prior dates, we need to use another report.
            //Check if startdate < 21 FEB 2012
            //If yes, first get data for 21 FEB 2012, and keep a list of all indices
            //then retrieve historical data for others

            int date1 = Integer.parseInt(startDate);
            int date2 = Integer.parseInt(endDate);
            if (date1 < 20120221) {
                //prior processing
                //get data for 21 FEB 2012 - indices only
                ArrayList<String> legacyIndices = new ArrayList<>();
                String nseTrades = String.format("http://www.nseindia.com/content/indices/ind_close_all_%s.csv", "21022012");
                URL nseTradesURL = new URL(nseTrades);
                if (getResponseCode(nseTrades) != 404) {
                    System.out.println("Parsing URL :" + nseTrades);
                    BufferedReader in = new BufferedReader(new InputStreamReader(nseTradesURL.openStream()));
                    HashMap<String, HistoricalData> h = new HashMap<>();
                    HashMap<String, HistoricalData> hdate = new HashMap<>();
                    Date date = null;
                    int j = 0;
                    int rowsToSkip = 1;
                    String line = "";
                    while ((line = in.readLine()) != null) {
                        j = j + 1;
                        if (j > rowsToSkip) {
                            String symbolData[] = line.split(",");
                            symbolData[0] = symbolData[0].contains("S&P") ? symbolData[0].substring(3).trim() : symbolData[0];
                            symbolData[0] = symbolData[0].replaceAll("\\s", "%20");
                            legacyIndices.add(symbolData[0].toUpperCase());
                        }
                    }
                    //for each legacy index, download data and create file
                    SimpleDateFormat ddHMMMHyyyyFormat = new SimpleDateFormat("dd-MMM-yyyy");
                    SimpleDateFormat ddMMMyyyyFormat = new SimpleDateFormat("ddMMMyyyy");
                    SimpleDateFormat ddMMyyyyFormat = new SimpleDateFormat("ddMMyyyy");
                    SimpleDateFormat yyyyMMddFormat = new SimpleDateFormat("yyyyMMdd");
                    SimpleDateFormat ddmmyyyyFormat = new SimpleDateFormat("dd-MM-yyyy");

                    //Date sd = ddmmyyyyFormat.parse("03-11-1995");
                    Date sd = yyyyMMddFormat.parse(startDate);
                    Date ed = sd;

                    //for (String index : legacyIndices) {//update OHLCV
                    hdate.clear();

                    if (sd.before(ddmmyyyyFormat.parse("21-02-2012")) && ed.after(ddmmyyyyFormat.parse("21-12-2012"))) {
                        ed = ddmmyyyyFormat.parse("21-02-2012");
                    }
                    while (ed.before(ddmmyyyyFormat.parse("22-02-2012")) && ed.before(yyyyMMddFormat.parse(endDate))) {
                        h.clear();
                        for (String index : legacyIndices) {//update OHLCV
                            index = index.replaceAll(" ", "%20");
                            nseTrades = String.format("http://www.nseindia.com/content/indices/histdata/%s%s-%s.csv", index, ddmmyyyyFormat.format(sd), ddmmyyyyFormat.format(ed));
                            nseTradesURL = new URL(nseTrades);
                            if (getResponseCode(nseTrades) != 404) {
                                System.out.println("Parsing URL :" + nseTrades);
                                try {
                                    in = new BufferedReader(new InputStreamReader(nseTradesURL.openStream()));
                                } catch (Exception e) {
                                    logger.log(Level.SEVERE, null, e);
                                }
                                j = 0;
                                rowsToSkip = 1;
                                while ((line = in.readLine()) != null) {
                                    j = j + 1;
                                    if (j > rowsToSkip) {
                                        //System.out.println("Line: " + line);
                                        String symbolData[] = line.split(",");
                                        index = index.replaceAll("%20", " ");
                                        if (Utilities.isValidDate(symbolData[0].replace("\"", "").trim(), ddHMMMHyyyyFormat)) {
                                            date = ddHMMMHyyyyFormat.parse(symbolData[0].replace("\"", "").trim());
                                            String dateString = yyyyMMddFormat.format(date).toUpperCase();
                                            if (symbolData.length > 6) {
                                                hdate.put(dateString, new HistoricalData(dateString, index, symbolData[1].replace("\"", "").trim(), symbolData[2].replace("\"", "").trim(), symbolData[3].replace("\"", "").trim(), symbolData[4].replace("\"", "").trim(), symbolData[5].replace("\"", "").trim(), symbolData[6].replace("\"", "").trim(), "", "", ""));
                                                h.put(index, new HistoricalData(dateString, index, symbolData[1].replace("\"", "").trim(), symbolData[2].replace("\"", "").trim(), symbolData[3].replace("\"", "").trim(), symbolData[4].replace("\"", "").trim(), symbolData[5].replace("\"", "").trim(), symbolData[6].replace("\"", "").trim(), "", "", ""));
                                            } else {
                                                System.out.println("Index: " + index + "does not have correct data for date:" + dateString);
                                            }
                                        }
                                    }

                                }
                                //update PE
                                if (sd.after(yyyyMMddFormat.parse("19981231"))) {
                                    index = index.replaceAll(" ", "%20");
                                    nseTrades = String.format("http://www.nseindia.com/content/indices/histdata/%sall%s-TO-%s.csv", index, ddmmyyyyFormat.format(sd), ddmmyyyyFormat.format(ed));
                                    nseTradesURL = new URL(nseTrades);
                                    if (getResponseCode(nseTrades) != 404) {
                                        System.out.println("Parsing URL :" + nseTrades);
                                        in = new BufferedReader(new InputStreamReader(nseTradesURL.openStream()));
                                        j = 0;
                                        rowsToSkip = 1;
                                        index = index.replaceAll("%20", " ");
                                        while ((line = in.readLine()) != null) {
                                            j = j + 1;
                                            if (j > rowsToSkip) {
                                                String symbolData[] = line.split(",");
                                                date = ddHMMMHyyyyFormat.parse(String.valueOf(symbolData[0]));
                                                String dateString = ddMMMyyyyFormat.format(date).toUpperCase();
                                                h.get(index).PE = symbolData != null && symbolData.length > 1 ? symbolData[1] : "";
                                                h.get(index).PB = symbolData != null && symbolData.length > 2 ? symbolData[2] : "";
                                                h.get(index).dividendyield = symbolData != null && symbolData.length > 3 ? symbolData[3] : "";
                                            }
                                        }
                                    } else {
                                        //log error in retrieveing PE
                                        // logger.log(Level.INFO, "Error retrieving PE URL :{0}", nseTrades);
                                    }
                                }
                            } else {
                                //logger.log(Level.INFO, "Error retrieving Index URL :{0}", nseTrades);
                            }
                        }
                        if (date != null && !h.isEmpty()) {
                            if (useSQL) {
                                writeToSQL(yyyyMMddFormat.format(date).toUpperCase(), h);
                            }
                            if (useFile) {
                                writeToFile(yyyyMMddFormat.format(date).toUpperCase(), h);
                            }
                            if (useCassandra) {
                                for (HistoricalData hist : h.values()) {
                                    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                                    long time = formatter.parse(hist.date).getTime();
                                    Cassandra(hist.open, time, cassandraIndexMetric + ".open", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                    Cassandra(hist.high, time, cassandraIndexMetric + ".high", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                    Cassandra(hist.low, time, cassandraIndexMetric + ".low", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                    Cassandra(hist.last, time, cassandraIndexMetric + ".close", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                    Cassandra(hist.volume, time, cassandraIndexMetric + ".volume", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                    Cassandra(hist.close, time, cassandraIndexMetric + ".settle", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                    Cassandra(hist.tradedValue, time, cassandraIndexMetric + ".tradedvalue", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                                }
                            }
                        }
                        sd = addDays(ed, 1);
                        ed = sd;
                    }
                } else {
                    logger.log(Level.INFO, "Error retrieving URL :{0}", nseTrades);
                }
            }
            SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat ddMMyyyyFormat = new SimpleDateFormat("ddMMyyyy");
            SimpleDateFormat ddMMMyyyyFormat = new SimpleDateFormat("ddMMMyyyy");
            for (int i = Math.max(date1, 20120222); i <= date2; i = Integer.valueOf(inputFormat.format(addDays(inputFormat.parse(String.valueOf(i)), 1)))) {

                Date date = inputFormat.parse(String.valueOf(i));
                String dateString = ddMMyyyyFormat.format(date).toUpperCase();
                String nseTrades = String.format("http://www.nseindia.com/content/indices/ind_close_all_%s.csv", dateString);
                URL nseTradesURL = new URL(nseTrades);
                if (getResponseCode(nseTrades) != 404) {
                    dateString = inputFormat.format(date).toUpperCase();//conver to ddMMMyyyy format for writing
                    BufferedReader in = new BufferedReader(new InputStreamReader(nseTradesURL.openStream()));
                    String line;
                    HashMap<String, HistoricalData> h = new HashMap<>();
                    int j = 0;
                    int rowsToSkip = 1;
                    while ((line = in.readLine()) != null) {
                        j = j + 1;
                        if (j > rowsToSkip) {
                            String symbolData[] = line.split(",");
                            symbolData[0] = symbolData[0].contains("S&P") ? symbolData[0].substring(3).trim() : symbolData[0];
                            symbolData[0] = symbolData[0].replaceAll("%20", " ");
                            h.put(symbolData[0], new HistoricalData(dateString, symbolData[0], symbolData[2], symbolData[3], symbolData[4], symbolData[5], symbolData[8], symbolData[9], symbolData[10], symbolData[11], symbolData[12]));
                        }
                    }
                    if (useSQL) {
                        writeToSQL(inputFormat.format(date).toUpperCase(), h);
                    }
                    if (useFile) {
                        writeToFile(inputFormat.format(date).toUpperCase(), h);
                    }
                    if (useCassandra) {
                        for (HistoricalData hist : h.values()) {
                            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                            long time = formatter.parse(hist.date).getTime();
                            Cassandra(hist.open, time, cassandraIndexMetric + ".open", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.high, time, cassandraIndexMetric + ".high", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.low, time, cassandraIndexMetric + ".low", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.last, time, cassandraIndexMetric + ".close", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.volume, time, cassandraIndexMetric + ".volume", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.close, time, cassandraIndexMetric + ".settle", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                            Cassandra(hist.tradedValue, time, cassandraIndexMetric + ".tradedvalue", hist.symbol, hist.expiry, hist.optionStrike, hist.optionType, output);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    static void Cassandra(String value, long time, String metric, String symbol, String expiry, String strike, String optionType, PrintStream output) {

        try {
            symbol = symbol.replaceAll(" ", "").replaceAll("&", "");
            if (symbol.equals("NIFTY") || symbol.equalsIgnoreCase("CNXNifty")) {
                symbol = "NSENIFTY";
            }
            if(!isNumeric(value)){
                value="0";
            }
            if (expiry == null) {
                output.print("put " + metric + " " + time + " " + value + " " + "symbol=" + symbol.replaceAll("[^A-Za-z0-9]", "").toLowerCase() + System.getProperty("line.separator"));
            } else if (strike == null) {
                output.print("put " + metric + " " + time + " " + value + " " + "symbol=" + symbol.toLowerCase() + " " + "expiry=" + expiry + System.getProperty("line.separator"));
            } else {
                output.print("put " + metric + " " + time + " " + value + " " + "symbol=" + symbol.toLowerCase() + " " + "expiry=" + expiry + " " + "strike=" + strike + " " + "option=" + optionType + System.getProperty("line.separator"));
            }


        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            //output.close();
        }
    }

    static void writeToSQL(String dateString, HashMap<String, HistoricalData> h) throws SQLException {
        PreparedStatement stmt = null;
        conn.setAutoCommit(false);
        stmt = conn.prepareStatement("INSERT INTO " + sqlTable + " (date,symbol,open,high,low,close,last,volume,delivered,tradedvalue,pe,pb,yield,type) " + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?);");
        long startTime = new Date().getTime();
        int i = 0;
        for (Map.Entry<String, HistoricalData> entry : h.entrySet()) {
            try {
                entry.getValue().symbol = entry.getValue().symbol.replaceAll(" ", "");
                //entry.getValue().writer(dateString + "_equity.csv");
                entry.getValue().open = isNumeric(entry.getValue().open) ? entry.getValue().open : "0";
                entry.getValue().high = isNumeric(entry.getValue().high) ? entry.getValue().high : "0";
                entry.getValue().low = isNumeric(entry.getValue().low) ? entry.getValue().low : "0";
                entry.getValue().close = isNumeric(entry.getValue().close) ? entry.getValue().close : "0";
                entry.getValue().last = isNumeric(entry.getValue().last) ? entry.getValue().last : "0";
                entry.getValue().volume = isNumeric(entry.getValue().volume) ? entry.getValue().volume : "0";
                entry.getValue().deliverable = isNumeric(entry.getValue().deliverable) ? entry.getValue().deliverable : "0";
                entry.getValue().tradedValue = isNumeric(entry.getValue().tradedValue) ? entry.getValue().tradedValue : "0";
                entry.getValue().PB = isNumeric(entry.getValue().PB) ? entry.getValue().PB : "0";
                entry.getValue().PE = isNumeric(entry.getValue().PE) ? entry.getValue().PE : "0";
                entry.getValue().dividendyield = isNumeric(entry.getValue().dividendyield) ? entry.getValue().dividendyield : "0";
                SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                Date myDate = formatter.parse(entry.getValue().date);
                java.sql.Date sqlDate = new java.sql.Date(myDate.getTime());
                i = i + 1;
                stmt.clearParameters();
                stmt.setDate(1, sqlDate);
                stmt.setString(2, entry.getValue().symbol);
                stmt.setString(3, entry.getValue().open);
                stmt.setString(4, entry.getValue().high);
                stmt.setString(5, entry.getValue().low);
                stmt.setString(6, entry.getValue().close);
                stmt.setString(7, entry.getValue().last);
                stmt.setString(8, entry.getValue().volume);
                stmt.setString(9, entry.getValue().deliverable);
                stmt.setString(10, entry.getValue().tradedValue);
                stmt.setString(11, entry.getValue().PE);
                stmt.setString(12, entry.getValue().PB);
                stmt.setString(13, entry.getValue().dividendyield);
                stmt.setString(14, entry.getValue().type);
                stmt.execute();                
                 if ((i + 1) % 1000 == 0) {
                 stmt.executeBatch();
                 stmt.clearBatch();
                              
                 }
            } catch (Exception e) {
                logger.log(Level.INFO, null, e);
            }
        }
        stmt.executeBatch();
        conn.commit();
        stmt.close();
        System.out.println("Completed Equity Download for: " + dateString + " .Seconds to insert:" + (new Date().getTime() - startTime) / 1000);
    }

    static void writeToFile(String dateString, HashMap<String, HistoricalData> h) throws SQLException {

        for (Map.Entry<String, HistoricalData> entry : h.entrySet()) {
            try {
                entry.getValue().symbol = entry.getValue().symbol.replaceAll(" ", "");
                entry.getValue().writer(dateString + "_equity.csv");
            } catch (Exception e) {
                logger.log(Level.INFO, null, e);
            }
        }
    }

    public static boolean isNumeric(String str) {
        if (str == null) {
            return false;
        }
        Scanner scanner = new Scanner(str);
        if (scanner.hasNextInt()) {
            return true;
        } else if (scanner.hasNextDouble()) {
            return true;
        } else {
            return false;
        }
    }

    static void getSymbolChange() throws MalformedURLException, IOException, SQLException, ParseException {
        String nameChangeLink = "http://www.nse-india.com/content/equities/symbolchange.csv";
        URL nseLink = new URL(nameChangeLink);
        int j = 0;
        int rowsToSkip = 1;
        if (getResponseCode(nameChangeLink) != 404) {
            BufferedReader in = new BufferedReader(new InputStreamReader(nseLink.openStream()));

            //write to database
            String line;
            while ((line = in.readLine()) != null) {
                j = j + 1;
                if (j > rowsToSkip) {
                    String[] input = line.split(",");
                    PreparedStatement stmt = conn.prepareStatement("INSERT INTO symbolchange(date,oldsymbol,newsymbol)" + "VALUES (?, ?, ?);");
                    SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
                    Date myDate = formatter.parse(input[3]);
                    java.sql.Date sqlDate = new java.sql.Date(myDate.getTime());
                    stmt.setString(1, sqlDate.toString());
                    stmt.setString(2, input[1]);
                    stmt.setString(3, input[2]);
                    stmt.executeUpdate();
                }
            }
        }
    }

    static void getSplits() throws IOException, SQLException, ParseException {
        List<String> existingSymbolsLoad = Files.readAllLines(Paths.get("splits.csv"), StandardCharsets.UTF_8);
        int j = 0;
        int rowsToSkip = 1;
        for (String s : existingSymbolsLoad) {
            j = j + 1;
            if (j > rowsToSkip) {
                try {
                    String input[] = s.split(",");
                    PreparedStatement stmt = conn.prepareStatement("INSERT INTO splits(date,symbol,oldshares,newshares)" + "VALUES (?, ?, ?, ?);");
                    SimpleDateFormat formatter = new SimpleDateFormat("dd/mm/yyyy");
                    Date myDate = formatter.parse(input[3]);
                    java.sql.Date sqlDate = new java.sql.Date(myDate.getTime());
                    stmt.setString(1, sqlDate.toString());
                    stmt.setString(2, input[0]);
                    stmt.setString(3, input[2]);
                    stmt.setString(4, input[1]);
                    stmt.executeUpdate();
                } catch (Exception e) {
                    logger.log(Level.INFO, null, e);
                }
            }
        }
    }

    public static int getResponseCode(String urlString) throws MalformedURLException, IOException {
        URL u = new URL(urlString);
        HttpURLConnection huc = (HttpURLConnection) u.openConnection();
        huc.setRequestMethod("GET");
        huc.connect();
        return huc.getResponseCode();
    }

    public static Date addDays(Date date, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days); //minus number would decrement the days
        return cal.getTime();
    }
}
