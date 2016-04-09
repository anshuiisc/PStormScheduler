package in.dream_lab.genevents.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import in.dream_lab.genevents.boltsUidai.Identity;
import in.dream_lab.genevents.boltsUidai.Sink;
import in.dream_lab.genevents.factory.ArgumentClass;
import in.dream_lab.genevents.factory.ArgumentParser;
import in.dream_lab.genevents.samples.SampleSpout;

//import in.dream_lab.genevents.bolts.Identity;
//import in.dream_lab.genevents.bolts.Sink;

/**
 * Created by anshushukla on 18/05/15.
 */
public class TestSpoutWbdbTopology {

    public static void main(String[] args) throws Exception {


        /** Common Code begins **/
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + "-" + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
//        GlobalConstants.setExperimentNumber(logFilePrefix);
        /** Common Code ends **/


        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("spout", new SampleSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()), 1).setNumTasks(1);


        builder.setBolt("identity", new Identity(sinkLogFileName), 1).shuffleGrouping("spout").setNumTasks(1);

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("identity").setNumTasks(1);


        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(12);
//        conf.setMaxSpoutPending(10000);
//        conf.put(Config.TOPOLOGY_WORKERS,15);

//        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,1024);
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 1024);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 1024);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
//        Config.setNumWorkers(conf,5);
//        conf.setNumWorkers(7);

        /** Common Code begins **/
        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {

//            if (GlobalConstants.getDataSetTypeFromRunID(experiRunId))
            //conf.setNumWorkers(3);
           // conf.setNumWorkers((stormTopology.get_spouts_size() + stormTopology.get_bolts_size()));
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(1000000000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
        /** Common Code ends **/

//        if (args != null && args.length > 0) {
//            conf.setNumWorkers(3);
//
//            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
//        }
//        else {
//
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("test", conf, builder.createTopology());
//            Utils.sleep(1000000);
//            cluster.killTopology("test");
//            cluster.shutdown();
//        }
    }
}


// L   NEWTopology   /Users/anshushukla/Downloads/wbdbUIDAI/eventDistCount.csv      PLUG-12  0.01   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm/12

// L   IdentityTopology   /Users/anshushukla/data/experi-smartplug-10min.csv     PLUG-106  0.1   /Users/anshushukla/data/output/temp