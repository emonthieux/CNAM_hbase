package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.text.ParsePosition;


// Create table Ligue 1 in HBase database
public class InsertTable {
    private static final TableName TABLE_NAME = TableName.valueOf("Ligue1");
    public static void main(String[] args) throws Exception {
        SimpleDateFormat fromUser = new SimpleDateFormat("dd/MM/yy");
        SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd");
        
        // Files to be inserted
        String[] pathnames;
        File folder = new File("data");
        pathnames = folder.list();
        
        // List of values to insert
        ArrayList<String> Metadata = new ArrayList<>(Arrays.asList("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR"));
        ArrayList<String> Statistics = new ArrayList<>(Arrays.asList("HS", "AS", "HST", "AST", "HC", "AC", "HF", "AF", "HY", "AY", "HR", "AR"));
        ArrayList<String> Betting = new ArrayList<>(Arrays.asList("B365H", "B365D", "B365A", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA"));


        // Instantiate the hbase configuration
        Configuration con = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(con);
        Table table = connection.getTable(TABLE_NAME);

        // For each file in the folder
        for (File file : folder.listFiles()) {
            System.out.println(file.getName());
            // If file exists
            if (file.isFile()) {
                // Read the file
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;

                ArrayList<Integer> Metadata_index = new ArrayList<>();
                ArrayList<Integer> Statistics_index = new ArrayList<>();
                ArrayList<Integer> Betting_index = new ArrayList<>();

                // Parse the first line to get the index of the values to insert
                if ((line = br.readLine()) != null) {
                    String[] values = line.split(",");
                    for (int i = 0; i < values.length; i++) {
                        if (Metadata.contains(values[i])) {
                            Metadata_index.add(i);
                        }
                        if (Statistics.contains(values[i])) {
                            Statistics_index.add(i);
                        }
                        if (Betting.contains(values[i])) {
                            Betting_index.add(i);
                        }
                    }
                }
                 
                // For each line in the file starting from the second line
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");

                    String date_match = myFormat.format(fromUser.parse(values[1]));
                    
                    Long ts = Timestamp.valueOf(date_match + " 00:00:00").getTime();

                    // Create the put
                    Put p = new Put(Bytes.toBytes(values[2] + "-" + values[3]));        

                    // For each value to insert in the Metadata family
                    for (int i = 0; i < Metadata_index.size(); i++) {

                        try {
                            p.addColumn(Bytes.toBytes("Metadata"), Bytes.toBytes(Metadata.get(i)), ts, Bytes.toBytes(values[Metadata_index.get(i)]));
                        }
                        catch(Exception e){
                            //pass
                        }
                    }
                    // For each value to insert in the Statistics family
                    for (int i = 0; i < Statistics_index.size(); i++) {
                        p.addColumn(Bytes.toBytes("Statistics"), Bytes.toBytes(Statistics.get(i)), ts, Bytes.toBytes(values[Statistics_index.get(i)]));
                    }
                    // For each value to insert in the Betting family
                    for (int i = 0; i < Betting_index.size(); i++) {
                        p.addColumn(Bytes.toBytes("Betting"), Bytes.toBytes(Betting.get(i)), ts, Bytes.toBytes(values[Betting_index.get(i)]));
                    }
                    // Insert the put
                    System.out.println("Put data");
                    table.put(p);
                    
                }
                

                // Close the file
                br.close();

                // Close the table
                table.close();            
            }
        }
    }
}
