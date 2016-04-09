package in.dream_lab.genevents.samples;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import in.dream_lab.genevents.EventGen;
import in.dream_lab.genevents.ISyntheticEventGen;
import in.dream_lab.genevents.logging.BatchedFileLogging;
import in.dream_lab.genevents.utils.GlobalConstants;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SampleSpout extends BaseRichSpout implements ISyntheticEventGen {
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;

	public SampleSpout(){
//		this.csvFileName = "/home/ubuntu/sample100_sense.csv";
		System.out.println("Inside  sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
		System.out.print("the output is as follows");
	}

	public SampleSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public SampleSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		try {
		System.out.println("spout Queue count= "+this.eventQueue.size());
		// allow multiple tuples to be emitted per next tuple.
		// Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
		int count = 0, MAX_COUNT=10; // FIXME?
		while(count < MAX_COUNT) {
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) return;
			count++;
			Values values = new Values();
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}
			String rowString = rowStringBuf.toString().substring(1);
			values.add(rowString);
			msgId++;
			values.add(Long.toString(msgId));
			this._collector.emit(values);
			try {
//				msgId++;
				ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId);
				//ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
//			System.out.println("values by source are -" + values);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		}
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		System.out.println("SampleSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);
		Random r=new Random();
		msgId=r.nextInt(10000);
		_collector = collector;
		this.eventGen = new EventGen(this,this.scalingFactor);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
		this.eventGen.launch(this.csvFileName, uLogfilename); //Launch threads

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());


	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//List<String> fieldsList = EventGen.getHeadersFromCSV(csvFileName);
		//fieldsList.add("MSGID");
		//declarer.declare(new Fields(fieldsList));
		declarer.declare(new Fields("RowString", "MSGID"));
	}

	@Override
	public void receive(List<String> event) {
		// TODO Auto-generated method stub
		//System.out.println("Called IN SPOUT### ");
		try {
			this.eventQueue.put(event);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
