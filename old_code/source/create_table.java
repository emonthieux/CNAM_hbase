//Import hbase libraries
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

/* Create table Ligue 1 in HBase database
with {'Metadata': dict(), 'Statistics': dict(), 'Betting': dict()}
*/
public class create_table {
    public static void main(String[] args) throws Exception {
        //Instantiate Configuration class
        Configuration con = HBaseConfiguration.create();
        //Instantiate HBaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(con);
        //Instantiate table descriptor class
        HTableDescriptor tableDescriptor = new
        HTableDescriptor(TableName.valueOf("Ligue1"));
        //Add column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("Metadata"));
        tableDescriptor.addFamily(new HColumnDescriptor("Statistics"));
        tableDescriptor.addFamily(new HColumnDescriptor("Betting"));
        //Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println("Table created ");
    }
}