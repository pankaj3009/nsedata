
import java.util.Date;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author psharma
 */
public class Cleanup {
    public static void main(String args[]){
        
        long starttime=1504242000;
        starttime=1506402000;
        long endtime=new Date().getTime()/1000;
        String metric="india.nse.future.s1.1sec";
        String symbol1="acc";
        Object[] expiries=Utilities.getExpiriesFromKDB(symbol1, starttime*1000, endtime*1000, metric+".close");
        Object[] symbols=Utilities.getSymbolsFromKDB(starttime*1000, endtime*1000, metric+".close");
        for(Object s:symbols){
            String symbol=s.toString();
            //String symbol="sreinfra";
            String expiry="20171130";
            System.out.println("Deleting "+symbol);
            Utilities.deleteData(symbol, expiry, null, null, new Date(starttime*1000),new Date(endtime*1000) , metric+".open");
            Utilities.deleteData(symbol, expiry, null, null, new Date(starttime*1000),new Date(endtime*1000) , metric+".high");
            Utilities.deleteData(symbol, expiry, null, null, new Date(starttime*1000),new Date(endtime*1000) , metric+".low");
            Utilities.deleteData(symbol, expiry, null, null, new Date(starttime*1000),new Date(endtime*1000) , metric+".close");
            Utilities.deleteData(symbol, expiry, null, null, new Date(starttime*1000),new Date(endtime*1000) , metric+".volume");
        }
    }
    
    
}
