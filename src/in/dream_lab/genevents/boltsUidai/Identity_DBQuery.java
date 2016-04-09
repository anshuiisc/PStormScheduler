package in.dream_lab.genevents.boltsUidai;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.logging.BatchedFileLogging;
import in.dream_lab.genevents.utils.GlobalConstants;

import java.sql.*;
import java.util.Map;
import java.util.Random;

public class Identity_DBQuery extends BaseRichBolt {


    String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public Identity_DBQuery(String csvFileNameOutSink){
        this.csvFileNameOutSink = csvFileNameOutSink;
    }
//    File inputFile;

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/EMP";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "12345";
    Connection conn = null;
    Statement stmt = null;


    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.out.println("topo-PLUG-name-is-"+topologyContext.getRawTopology().toString());
//        String s="/Users/anshushukla/data/output/temp/spout-IdentityTopology-PLUG-123-0.1-Anshus-MacBook-Pro.local.log";
//        BatchedFileLogging.writeToTemp(this, csvFileNameOutSink);
            //STEP 2: Register JDBC driver
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        //STEP 3: Open a connection
            System.out.println("Connecting to database...");
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        BatchedFileLogging.writeToTempDB(this, csvFileNameOutSink, String.valueOf(conn));
//            System.out.println("connection-DB-"+conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.collector=outputCollector;
        GlobalConstants.createBoltIdentifyingFiles(topologyContext);
    }

    @Override
    public void execute(Tuple input) {
    	String rowString = input.getString(0);
    	String msgId = input.getString(input.size()-1);
        long startTime = System.nanoTime();

        //STEP 4: Execute a query
//            select * from  Employees where id  BETWEEN 500 and 600 ;
        System.out.println("Creating statement...");
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String sql;
//        sql = "SELECT id, first, last, age FROM Employees";

        Random R=new Random(1000);
        int val=R.nextInt();

        sql="select * from  Employees where id  BETWEEN "+val+" and "+ (val+500) ;
        System.out.println("CHECK:"+sql);
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //STEP 5: Extract data from result set
        try {
            while(rs.next()){
                //Retrieve by column name
                int id  = rs.getInt("id");
                int age = rs.getInt("age");
                String first = rs.getString("first");
                String last = rs.getString("last");



//                additions here??????
                System.out.println("CHECK:"+("" + id).length() + ("" + age).length() + (first).length()+(last).length());


//                System.out.print("ID: " + id);
//                System.out.print(", Age: " + age);
//                System.out.print(", First: " + first);
//                System.out.println(", Last: " + last);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        //STEP 6: Clean-up environment
//        try {
//            rs.close();
//            stmt.close();
//            conn.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }finally{
//            //finally block used to close resources
//            try{
//                if(stmt!=null)
//                    stmt.close();
//            }catch(SQLException se2){
//            }// nothing we can do
//            try{
//                if(conn!=null)
//                    conn.close();
//            }catch(SQLException se){
//                se.printStackTrace();
//            }//end finally try
//        }//end try

        long stopTime = System.nanoTime();
        System.out.println("in millisec - "+(stopTime - startTime) / (1000000.0));

        collector.emit(new Values(rowString,msgId ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Column","MSGID"));
    }
}
