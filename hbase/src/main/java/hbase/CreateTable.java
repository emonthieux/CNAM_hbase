package hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class CreateTable {
    private static final TableName TABLE_NAME = TableName.valueOf("Ligue1");
    private static final byte[] CF_METADATA = Bytes.toBytes("Metadata");
    private static final byte[] CF_STATISTICS = Bytes.toBytes("Statistics");
    private static final byte[] CF_BETTING = Bytes.toBytes("Betting");

    public static void createTable(final Admin admin) throws IOException {
        if(!admin.tableExists(TABLE_NAME)) {
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_METADATA))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_STATISTICS))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_BETTING))
                    .build();
            admin.createTable(desc);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {
            createTable(admin);
        }
    }
}