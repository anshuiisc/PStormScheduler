package in.dream_lab.genevents.topology.cpuIntensive;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import in.dream_lab.genevents.boltsUidai.IdentityFloat;
import in.dream_lab.genevents.boltsUidai.Sink;
import in.dream_lab.genevents.factory.ArgumentClass;
import in.dream_lab.genevents.factory.ArgumentParser;
import in.dream_lab.genevents.samples.SampleSpout;

//import in.dream_lab.genevents.bolts.Identity;
//import in.dream_lab.genevents.bolts.Sink;

/**
 * Created by anshushukla on 18/05/15.
 */
public class TestPushTopology {

    public static void main(String[] args) throws Exception {


        /** Common Code begins **/
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;



        TopologyBuilder builder = new TopologyBuilder();

//        builder.setSpout("wordspout", new WordSpout(), 6);
//        builder.setSpout("wordspout", new SampleSpout(), 1);
        builder.setSpout("spout", new SampleSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()),
                1)
        ;

        builder.setBolt("identity1",
                new IdentityFloat(sinkLogFileName), 1)
                .shuffleGrouping("spout");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("identity1");


        Config conf = new Config();
        conf.setDebug(true);
//        conf.setNumWorkers(12);
//        conf.setMaxSpoutPending(10000);
//        conf.put(Config.TOPOLOGY_WORKERS,15);



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
            Utils.sleep(100000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
        /** Common Code ends **/

    }
}


// L   IdentityTopology   /Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/test/java/operation/output/eventDist.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp