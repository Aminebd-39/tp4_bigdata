package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    public static final String TABLE_NAME = "students";
    public static final String CF_PERSONAL_DATA = "personal_data";
    public static final String CF_PROFESSIONAL_DATA = "professional_data";

    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "zookeeper");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "hbase-master:16000");

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);

            Admin admin = connection.getAdmin();

            // Question 1 : Creation de la table students

            TableName tableName = TableName.valueOf(TABLE_NAME);

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL_DATA));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PROFESSIONAL_DATA));

            TableDescriptor tableDescriptor = builder
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("grades"))
                    .build();

            if (!admin.tableExists(tableName)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created!");
            } else  {
                System.err.println("Already exist!");
            }

            // Question 2 : Insertion des donnees

            Table table = connection.getTable(tableName);
            Put put1 = new Put("student1".getBytes());
            put1.addColumn("info".getBytes(), "name".getBytes(), "John Doe".getBytes());
            put1.addColumn("info".getBytes(), "age".getBytes(), "20".getBytes());
            put1.addColumn("grades".getBytes(), "math".getBytes(), "B".getBytes());
            put1.addColumn("grades".getBytes(), "science".getBytes(), "A".getBytes());

            Put put2 = new Put("student2".getBytes());
            put2.addColumn("info".getBytes(), "name".getBytes(), "Jane Smith".getBytes());
            put2.addColumn("info".getBytes(), "age".getBytes(), "22".getBytes());
            put2.addColumn("grades".getBytes(), "math".getBytes(), "A".getBytes());
            put2.addColumn("grades".getBytes(), "science".getBytes(), "A".getBytes());

            table.put(put1);
            table.put(put2);
            System.out.println("Added");

            // Question 3 : Les informations disponibles pour "student1"

            Get get = new Get("student1".getBytes());
            Result result = table.get(get);

            System.out.println("Infos :");
            for (Cell cell : result.listCells()) {
                System.out.println(new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                        ":" + new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) +
                        " -> " + new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }


            // Question 4 : Modification de Student1


            Put put = new Put("student2".getBytes());
            put.addColumn("info".getBytes(), "age".getBytes(), Integer.toString(23).getBytes());
            put.addColumn("grades".getBytes(), "math".getBytes(), "A+".getBytes());
            table.put(put);
            System.out.println("Student updated");


            // Question 5 : Suppression de Student1

            Delete delete = new Delete("student1".getBytes());
            table.delete(delete);
            System.out.println("Deleted");


            // Question 6 : les informations pour tous les étudiants

            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);

            System.out.println("Les informations de tous les étudiants");
            for (Result result1 : scanner) {
                for (Cell cell : result1.listCells()) {
                    System.out.println(new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) +
                            " -> " +
                            new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                            ":" +
                            new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) +
                            " -> " +
                            new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }

            table.close();


        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
