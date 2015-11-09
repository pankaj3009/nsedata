/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pankaj
 */
public class Cassandra implements Runnable {

    String value;
    long time;
    String metric;
    String symbol;
    String expiry;
    String strike;
    String optionType;
    PrintStream output;

    public Cassandra(String value, long time, String metric, String symbol, String expiry,String strike, PrintStream output) {
        this.value = value;
        this.time = time;
        this.metric = metric;
        this.symbol = symbol.replaceAll(" ", "").replaceAll("&", "");
        if(symbol.equalsIgnoreCase("NIFTY50")){
            this.symbol="NSENIFTY";
        }
        if(symbol.equalsIgnoreCase("NIFTY")){
            this.symbol="NSENIFTY";
        }
        this.expiry = expiry;
        this.output = output;
        if(strike!=null){
            this.strike=strike;
        }
        if(optionType!=null){
            this.optionType=optionType;
        }
        

    }

    @Override
    public void run() {
        try {
            if (expiry == null) {
                output.print("put " + metric + " " + time + " " + value + " " + "symbol=" + symbol.toLowerCase() + System.getProperty("line.separator"));
            } else if(strike==null) {
                output.print("put " + metric + " " + time + " " + value + " " + "symbol=" + symbol.toLowerCase() + " " + "expiry=" + expiry + System.getProperty("line.separator"));
            } else{
                output.print("put " + metric + " " + time + " " + value + " " + "symbol=" + symbol.toLowerCase() + " " + "expiry=" + expiry + " "+ "strike="+strike+" "+"option="+optionType+System.getProperty("line.separator"));                
            }
            
            
        } catch (Exception ex) {
            Logger.getLogger(Cassandra.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            //output.close();
        }
    }
}
