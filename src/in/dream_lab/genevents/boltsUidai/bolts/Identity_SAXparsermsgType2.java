package in.dream_lab.genevents.boltsUidai.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.logging.BatchedFileLogging;
import in.dream_lab.genevents.utils.GlobalConstants;
import test.java.operation.Operations;

import java.io.File;
import java.util.Map;

public class Identity_SAXparsermsgType2 extends BaseRichBolt {


    String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public Identity_SAXparsermsgType2(String csvFileNameOutSink){
        this.csvFileNameOutSink = csvFileNameOutSink;
    }
    File inputFile;




    OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {


        System.out.println("topo-PLUG-name-is-"+topologyContext.getRawTopology().toString());

//        String s="/Users/anshushukla/data/output/temp/spout-IdentityTopology-PLUG-123-0.1-Anshus-MacBook-Pro.local.log";
        BatchedFileLogging.writeToTemp(this, csvFileNameOutSink);



        this.collector=outputCollector;
        GlobalConstants.createBoltIdentifyingFiles(topologyContext);

        inputFile = new File("src/test/java/operation/tempSAX.xml");
        if(inputFile==null)
            inputFile = new File ("/data/storm/dataset/tempSAX.xml");

    }

    @Override
    public void execute(Tuple input) {
        long ts1=System.currentTimeMillis(); //addon
        String rowString = input.getString(0);
        String msgId = input.getString(input.size()-1);
        long startTime = System.nanoTime();
        for(int i=0;i<6;i++)
            Operations.doXMLparseOp(inputFile);
        long stopTime = System.nanoTime();
        System.out.println("time taken10loop  (in millisec) - "+(stopTime - startTime)/(1000000.0));
        long ts2=System.currentTimeMillis(); //addon
        // collector.emit(new Values(rowString,msgId ));
//        collector.emit("RED",new Values(rowString,msgId,String.valueOf(ts2-ts1)));  //addon

        String msgType=rowString.split(",")[2];
        System.out.println("Messagetype"+ msgType);
        if(msgType.equals("RED"))
            collector.emit("RED",new Values(rowString,msgId ,String.valueOf(ts2-ts1)));
        if(msgType.equals("BLUE"))
            System.out.println("wrong DATA at Identity_SAXparsermsgType2");
//            collector.emit("BLUE",new Values(rowString,msgId ));
        if(msgType.equals("GREEN"))
            collector.emit("GREEN",new Values(rowString,msgId ,String.valueOf(ts2-ts1)));

        System.out.println("CHECK:"+String.valueOf(ts2-ts1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("Column","MSGID","time"));//addon
        outputFieldsDeclarer.declareStream("RED",new Fields("Column","MSGID","time"));
        outputFieldsDeclarer.declareStream("GREEN",new Fields("Column","MSGID","time"));
    }
}