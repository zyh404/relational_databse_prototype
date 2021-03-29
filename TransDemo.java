package com.company;
import org.apache.commons.io.FileUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class TransDemo {

    public static TransDemo transDemo;
    public static String lcd_push_tablename="device_inssch_status";
    public static String lcd_push_ADB_tablename = "push_in_ssch2";

    public static void main(String[] args)  {
        try {
            Connection con = null;
            String server_name="10.216.9.12";
            String port_number="1433";
            String url = "jdbc:sqlserver://10.216.9.12:1433;databaseName=DB_push;user=push_reader;password=4EDD872E-5AEB";
            String db = "DB_push";
            String user = "push_reader";
            String pass = "4EDD872E-5AEB";
            String col_name ="";
            KettleEnvironment.init();
            con = DriverManager.getConnection(url);
            transDemo = new TransDemo();
            TransMeta transMeta = transDemo.generateMyOwnTrans();
            String transXml = transMeta.getXML();
            String transName = "etl/update_insert_Trans.ktr";
            File file = new File(transName);
            FileUtils.writeStringToFile(file, transXml, "UTF-8");
            System.out.println("helloword");
            ColumnName.getColumnName();
            ColumnName.GetPrimaryKeys(con);
            System.out.println(databasesXML.length+"\n"+databasesXML[0]+"\n"+databasesXML[1]);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            KettleEnvironment.init();
            TransMeta meta = new TransMeta("etl/update_insert_Trans.ktr");
            Trans trans = new Trans(meta);
            trans.prepareExecution(null);
            trans.startThreads();
            trans.waitUntilFinished();
        }catch (Exception e){
            System.out.println("failed");
            return;
        }
    }


    public static final String[] databasesXML = {
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<connection>" +
                    "<name>lcd推送</name>" +
                    "<server>10.216.9.12</server>" +
                    "<type>MSSQL</type>" +
                    "<access>Native</access>" +
                    "<database>DB_push</database>" +
                    "<port>1433</port>" +
                    "<username>push_reader</username>" +
                    "<password>4EDD872E-5AEB</password>" +
                    "</connection>",
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<connection>" +
                    "<name>lcd推送ADB</name>" +
                    "<server>127.0.0.1</server>" +
                    "<type>Mysql</type>" +
                    "<access>Native</access>" +
                    "<database>lcd推送ADB</database>" +
                    "<port>3306</port>" +
                    "<username>root</username>" +
                    "<password>zhangyuehao</password>" +
                    "</connection>"
    };

    public TransMeta generateMyOwnTrans() throws KettleXMLException,KettleDatabaseException{
        Connection con = null;
        String url = "jdbc:sqlserver://10.216.9.12:1433;databaseName=DB_push;user=push_reader;password=4EDD872E-5AEB";
        try {
            con = DriverManager.getConnection(url);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("************start to generate my own transformation***********");
        TransMeta transMeta = new TransMeta();
        transMeta.setName("insert_update");
        for (int i=0; i<databasesXML.length;i++){
            DatabaseMeta databaseMeta = new DatabaseMeta(databasesXML[i]);
            transMeta.addDatabase(databaseMeta);
        }
        PluginRegistry registry = PluginRegistry.getInstance();
        TableInputMeta tableInput = new TableInputMeta();
        String tableInputPluginId = registry.getPluginId(StepPluginType.class, tableInput);

        DatabaseMeta database_lcd_push = transMeta.findDatabase("lcd推送");
        tableInput.setDatabaseMeta(database_lcd_push);
        String select_sql = "SELECT * FROM "+lcd_push_tablename;
        tableInput.setSQL(select_sql);

        StepMeta tableInputMetaStep = new StepMeta(tableInputPluginId,"table input",tableInput);
        tableInputMetaStep.setDraw(true);
        tableInputMetaStep.setLocation(100, 100);
        transMeta.addStep(tableInputMetaStep);

        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
        String insertUpdateMetaPluginId = registry.getPluginId(StepPluginType.class,insertUpdateMeta);

        DatabaseMeta database_lcd_push_adb = transMeta.findDatabase("lcd推送ADB");
        insertUpdateMeta.setDatabaseMeta(database_lcd_push_adb);
        insertUpdateMeta.setTableName(lcd_push_ADB_tablename);
        insertUpdateMeta.setKeyLookup(new String[]{ColumnName.GetPrimaryKeys(con)});
        insertUpdateMeta.setKeyStream(new String[]{ColumnName.GetPrimaryKeys(con)});
        insertUpdateMeta.setKeyStream2(new String[]{""});//一定要加上
        insertUpdateMeta.setKeyCondition(new String[]{"="});

        String[] updatelookup = {ColumnName.getColumnName()};
        String [] updateStream = {ColumnName.getColumnName()};
        Boolean[] updateOrNot = {true,true};
        insertUpdateMeta.setUpdateLookup(updatelookup);
        insertUpdateMeta.setUpdateStream(updateStream);
        insertUpdateMeta.setUpdate(updateOrNot);
        String[] lookup = insertUpdateMeta.getUpdateLookup();

        StepMeta insertUpdateStep = new StepMeta(insertUpdateMetaPluginId,"insert_update",insertUpdateMeta);
        insertUpdateStep.setDraw(true);
        insertUpdateStep.setLocation(250,100);
        transMeta.addStep(insertUpdateStep);

        transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, insertUpdateStep));
        System.out.println("***********the end************");
        return transMeta;
    }
}