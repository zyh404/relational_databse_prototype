package com.company;
import java.sql.*;

public class ColumnName {
    public static String getColumnName() {
        Connection con = null;
        String server_name="10.216.9.12";
        String port_number="1433";
        String url = "jdbc:sqlserver://10.216.9.12:1433;databaseName=DB_push;user=push_reader;password=4EDD872E-5AEB";
        String db = "DB_push";
        String user = "push_reader";
        String pass = "4EDD872E-5AEB";
        String col_name ="";
        try {
            con = DriverManager.getConnection(url);
            try {
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("select * from device_inssch_status");
                ResultSetMetaData md = rs.getMetaData();

                int col = md.getColumnCount();
                for (int i = 1; i <= col; i++) {
                    col_name = col_name +'"'+ md.getColumnName(i)+'"'+',';
                    if(i==col){
                        col_name = col_name + '"'+ md.getColumnName(i)+'"';
                    }
                }


            } catch (SQLException s) {
                System.out.println("SQL statement is not executed!");
                }
            }
    catch(Exception e){
        e.printStackTrace();
    }
        return col_name;
    }

    public static String GetPrimaryKeys(Connection con) {
        String key="";
        try {
            DatabaseMetaData dbmd = con.getMetaData();
            ResultSet rs = dbmd.getPrimaryKeys("DB_Push", "dbo","device_inssch_status");
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            // Display the result set data.

            while( rs.next()) {
                    key = key + '"' + rs.getString(4) + '"' + ',';
            }
            rs.close();

        }

        catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(key.substring(0, key.length() - 1));
        return key.substring(0, key.length() - 1);


    }




}

