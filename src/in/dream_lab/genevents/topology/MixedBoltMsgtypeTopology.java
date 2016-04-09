package in.dream_lab.genevents.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import in.dream_lab.genevents.boltsUidai.bolts.IdentityFloatmsgType;
import in.dream_lab.genevents.boltsUidai.SinkmsgType;
import in.dream_lab.genevents.factory.ArgumentClass;
import in.dream_lab.genevents.factory.ArgumentParser;
import in.dream_lab.genevents.samples.SampleSpoutMsgType;


/**
 * Created by anshushukla on 18/05/15.
 */
public class MixedBoltMsgtypeTopology {

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
//        GlobalConstants.setExperimentNumber(logFilePrefix);
        /** Common Code ends **/


        TopologyBuilder builder = new TopologyBuilder();

//        builder.setSpout("wordspout", new WordSpout(), 6);
//        builder.setSpout("wordspout", new SampleSpout(), 1);
        builder.setSpout("SampleSpoutMsgType", new SampleSpoutMsgType(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()),
                1)
        ;
//
//        builder.setBolt("IdentitymsgType",
//                new IdentityFloatmsgType(sinkLogFileName), 1)
//                .shuffleGrouping("SampleSpoutMsgType","RED");


        builder.setBolt("IdentitymsgType",
                new IdentityFloatmsgType(sinkLogFileName), 1)
                .shuffleGrouping("SampleSpoutMsgType","RED");

        builder.setBolt("sink", new SinkmsgType(sinkLogFileName), 1).shuffleGrouping("IdentitymsgType","RED");


        Config conf = new Config();
        conf.setDebug(true);
//        conf.setNumWorkers(12);
//        conf.setMaxSpoutPending(10000);

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


    }
}


// L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/finalInput_SC.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp