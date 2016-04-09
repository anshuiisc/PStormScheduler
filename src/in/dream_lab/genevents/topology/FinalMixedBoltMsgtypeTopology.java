package in.dream_lab.genevents.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import in.dream_lab.genevents.boltsUidai.SinkmsgType;
import in.dream_lab.genevents.boltsUidai.bolts.*;
import in.dream_lab.genevents.factory.ArgumentClass;
import in.dream_lab.genevents.factory.ArgumentParser;
import in.dream_lab.genevents.samples.SampleSpoutMsgType;


/**
 * Created by anshushukla on 18/05/15.
 */
public class FinalMixedBoltMsgtypeTopology {

    public static void main(String[] args) throws Exception {

        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;


        TopologyBuilder builder = new TopologyBuilder();

//        builder.setSpout("wordspout", new SampleSpout(), 1);
//        builder.setSpout("wordspout", new WordSpout(), 6);
        builder.setSpout("SampleSpoutMsgType", new SampleSpoutMsgType(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()),
                1)
        ;
//
//        builder.setBolt("IdentitymsgType",
//                new IdentityFloatmsgType(sinkLogFileName), 1)
//                .shuffleGrouping("SampleSpoutMsgType","RED");


        builder.setBolt("IdentityFloatmsgType1",
                new IdentityFloatmsgType1(sinkLogFileName), 1)
                .shuffleGrouping("SampleSpoutMsgType","RED").shuffleGrouping("SampleSpoutMsgType","GREEN");

        builder.setBolt("Identity_SAXparsermsgType8",
                new Identity_SAXparsermsgType8(sinkLogFileName), 1)
                .shuffleGrouping("SampleSpoutMsgType","BLUE").shuffleGrouping("SampleSpoutMsgType","RED");

        builder.setBolt("Identity_SAXparsermsgType6",
                new Identity_SAXparsermsgType6(sinkLogFileName), 1)
                .shuffleGrouping("SampleSpoutMsgType","GREEN").shuffleGrouping("SampleSpoutMsgType","BLUE");





        builder.setBolt("Identity_SAXparsermsgType2",
                new Identity_SAXparsermsgType2(sinkLogFileName), 1)
                .shuffleGrouping("IdentityFloatmsgType1","RED").shuffleGrouping("IdentityFloatmsgType1","GREEN");

        builder.setBolt("IdentityFloatmsgType7",
                new IdentityFloatmsgType7(sinkLogFileName), 1)
                .shuffleGrouping("Identity_SAXparsermsgType6","GREEN")
                .shuffleGrouping("Identity_SAXparsermsgType8","BLUE").shuffleGrouping("Identity_SAXparsermsgType8","RED");

        builder.setBolt("IdentityFloatmsgType3",
                new IdentityFloatmsgType3(sinkLogFileName), 1)
                .shuffleGrouping("Identity_SAXparsermsgType2","RED").shuffleGrouping("Identity_SAXparsermsgType2","GREEN")
                .shuffleGrouping("IdentityFloatmsgType7","RED").shuffleGrouping("IdentityFloatmsgType7","BLUE").shuffleGrouping("IdentityFloatmsgType7","GREEN");

       // 4th is SINK

        builder.setBolt("IdentityFloatmsgType5",
                new IdentityFloatmsgType5(sinkLogFileName), 1)
                .shuffleGrouping("IdentityFloatmsgType3","GREEN").shuffleGrouping("Identity_SAXparsermsgType6","BLUE");


        builder.setBolt("sink", new SinkmsgType(sinkLogFileName), 1).shuffleGrouping("IdentityFloatmsgType3","RED")
                .shuffleGrouping("IdentityFloatmsgType3","BLUE")
                .shuffleGrouping("IdentityFloatmsgType5","GREEN").shuffleGrouping("IdentityFloatmsgType5","BLUE");


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