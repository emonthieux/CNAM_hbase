import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;


// Create table Ligue 1 in HBase database
public class Insert_table {
    public static void main(String[] args) throws Exception {
        
        // Files to be inserted
        String[] pathnames;
        File folder = File(args[0]);
        pathnames = folder.list();
        
        // List of values to insert
        ArrayList<String> Metadata = new ArrayList<>(Arrays.asList("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR"));
        ArrayList<String> Statistics = new ArrayList<>(Arrays.asList("HS", "AS", "HST", "AST", "HC", "AC", "HF", "AF", "HY", "AY", "HR", "AR"));
        ArrayList<String> Betting = new ArrayList<>(Arrays.asList("B365H", "B365D", "B365A", "BWH", "BWD", "BWA", "IWH", "IWD", "IWA", "PSH", "PSD", "PSA", "WHH", "WHD", "WHA"));

        // Instantiate the hbase configuration
        Configuration con = HBaseConfiguration.create();
        HTable hTable = new HTable(con, 'Ligue1');

        // For each file in the folder
        for (File file : folder.listFiles()) {
            // If file exists
            if (file.isFile()) {
                // Read the file
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;

                ArrayList<String> Metadata_index = new ArrayList<>();
                ArrayList<String> Statistics_index = new ArrayList<>();
                ArrayList<String> Betting_index = new ArrayList<>();

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
                    
                    // Timestamp
                    SimpleDateFormat ts = SimpleDateFormat.new("yy/MM/dd HH:mm:ss").parse(values[1], ParsePosition.new(0)).getTime();
                    // Create the put
                    Put p = new Put(Bytes.toBytes(values[2] + "-" + values[3]));        

                    // For each value to insert in the Metadata family
                    for (int i = 0; i < Metadata_index.size(); i++) {
                        p.add(Bytes.toBytes("Metadata"), Bytes.toBytes(Metadata.get(i)), ts, Bytes.toBytes(values[Metadata_index.get(i)]));
                    }
                    // For each value to insert in the Statistics family
                    for (int i = 0; i < Statistics_index.size(); i++) {
                        p.add(Bytes.toBytes("Statistics"), Bytes.toBytes(Statistics.get(i)), ts, Bytes.toBytes(values[Statistics_index.get(i)]));
                    }
                    // For each value to insert in the Betting family
                    for (int i = 0; i < Betting_index.size(); i++) {
                        p.add(Bytes.toBytes("Betting"), Bytes.toBytes(Betting.get(i)), ts, Bytes.toBytes(values[Betting_index.get(i)]));
                    }
                    // Insert the put
                    hTable.put(p);
                }

                // Close the file
                br.close();

                // Close the table
                hTable.close();            
            }
        }

    }
}
