// Place this code inside Hbase connection
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;						
import org.apache.hadoop.hbase.HBaseConfiguration;							
import org.apache.hadoop.hbase.HColumnDescriptor;							
import org.apache.hadoop.hbase.HTableDescriptor;		
import org.apache.hadoop.hbase.client.HBaseAdmin;			
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnection							
{							
    public static void main(String[] args) throws IOException						
    {							
	HBaseConfiguration hc = new HBaseConfiguration(new Configuration());										
	HTableDescriptor ht = new HTableDescriptor(Bytes.toBytes("Ligue1")); 										

	ht.addFamily( new HColumnDescriptor("education"));					
	ht.addFamily( new HColumnDescriptor("projects"));										
	System.out.println( "connecting" );										
	HBaseAdmin hba = new HBaseAdmin( hc );								

	System.out.println( "Creating Table" );								
	hba.createTable( ht );							
	System.out.println("Done......");										
    }						
}
