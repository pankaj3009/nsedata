
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Pankaj
 */
public class MigrateData {

    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket cassandraConnection = new Socket("91.121.168.138", Integer.parseInt("8085"));
        PrintStream output = new PrintStream(cassandraConnection.getOutputStream());

        String[] symbols = new String[]{"L&T", "M&M", "P&G", "S&SIND", "S&SPOWER", "SB&TINTL", "J&KBANK", "IT&T",
            "AXIS-IT&T", "M&MFIN", "AREVAT&D", "COX&KINGS", "HYDROS&S", "L&TFINANCE", "SURANAT&P", "IL&FSENGG",
            "IL&FSTRANS", "ALSTOMT&D", "L&TFH", "L&TINFRA", "GET&D"};
        String[] metrics = {"india.nse.equity.daily.s4.open", "india.nse.equity.daily.s4.high", "india.nse.equity.daily.s4.low",
            "india.nse.equity.daily.s4.settle", "india.nse.equity.daily.s4.close",
            "india.nse.equity.daily.s4.volume", "india.nse.equity.daily.s4.delivered"};

        HashMap<Long, String> data = new HashMap<>();
        for (String s : symbols) {
            for (String metric : metrics) {
                data = Utilities.getPrices(s, null, null, null, new Date(0), new Date(), metric);
                for (Map.Entry<Long, String> entry : data.entrySet()) {
                    NSEData.Cassandra(entry.getValue(), entry.getKey(), metric, s, null, null, null, output);
                }
            }
        }
    }
}
