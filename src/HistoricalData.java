/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author admin
 */
public class HistoricalData implements ReaderWriterInterface {
    String date;
    String symbol;
    String open;
    String high;
    String low;
    String close;
    String last;
    String volume;
    String deliverable=String.valueOf(0);
    String tradedValue;
    String PE;
    String PB;
    String dividendyield;
    String type;
    private static final Logger logger = Logger.getLogger(HistoricalData.class.getName());

    public HistoricalData(String date, String symbol, String open, String high, String low, String close, String last, String volume) {
        this.date = date==null?"":date;
        this.symbol = symbol==null?"":symbol;
        this.open = open==null?"":open;
        this.high = high==null?"":high;
        this.low = low==null?"":low;
        this.close = close==null?"":close;
        this.last = last==null?"":last;
        this.volume = volume==null?"":volume;
        this.type="E";
    }
    
        public HistoricalData(String date, String symbol, String open, String high, String low, String close, String volume,String tradevalue,String PE,String PB, String divyield) {
        this.date = date==null?"":date;
        this.symbol = symbol==null?"":symbol;
        this.open = open==null?"":open;
        this.high = high==null?"":high;
        this.low = low==null?"":low;
        this.close = close==null?"":close;
        this.volume = volume==null?"":volume;
        this.tradedValue = tradevalue==null?"":tradevalue;
        this.PE=PE==null?"":PE;
        this.PB=PB==null?"":PB;
        this.dividendyield=divyield==null?"":divyield;
        this.type="I";
    }

    @Override
    public void reader(String inputfile, ArrayList target) {
    }

    @Override
    public void writer(String fileName) {
         File f = new File(fileName);
        try {
           
            if (!f.exists() || f.isDirectory()) {
                String header = "Date,Symbol,Open,High,Low,Close,Last,Volume,DeliveryQuantity";
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
                out.println(header);
                out.close();
            } 
                String data = date + "," + symbol + "," + open + "," + high + "," + low + "," + close + "," + last+","+volume+","+deliverable;
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
                out.println(data);
                out.close();            
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
    }
    
    public void indexwriter(String fileName) {
         File f = new File(fileName);
        try {
           if(f.exists()){
               f.delete();
           }
            
            if (!f.exists() || f.isDirectory()) {
                String header = "Date,Index,Open,High,Low,Close,Volume,Turnover,PE,PB,DividedYield";
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
                out.println(header);
                out.close();
            } 
                String data = date + "," + symbol + "," + open + "," + high + "," + low + "," + close + "," +volume+","+tradedValue+","+PE+","+PB+","+dividendyield;
                PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
                out.println(data);
                out.close();            
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
    }
    
}
