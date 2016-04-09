package in.dream_lab.genevents.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import in.dream_lab.genevents.boltsUidai.*;
import in.dream_lab.genevents.factory.ArgumentClass;
import in.dream_lab.genevents.factory.ArgumentParser;
import in.dream_lab.genevents.samples.SampleSpoutMsgType;
//import bolts.*;
//import spouts.Spout;


public class UIDAI_Topology {
    public static void main(String[] args) throws Exception{


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

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();

//        builder.setSpout("read-packet-spout", new Spout());

        builder.setSpout("read-packet-spout", new SampleSpoutMsgType(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()), 1).addConfiguration("site", "tamu");;

        builder.setBolt("packet-extraction-bolt", new PacketExtractionBolt(sinkLogFileName))
                .shuffleGrouping("read-packet-spout").addConfiguration("site", "tamu");;

        builder.setBolt("demographic-check-bolt", new DemoDedupCheckBolt(sinkLogFileName))
                .shuffleGrouping("packet-extraction-bolt").addConfiguration("site", "tamu");;

//        builder.setBolt("biometric-dedup-check-bolt", new BioDedupCheckBolt())
//                .shuffleGrouping("demographic-check-bolt");

//        builder.setBolt("quality-dedup-check-bolt", new QualityCheckBolt())
//                .shuffleGrouping("demographic-check-bolt");

        builder.setBolt("quality-dedup-check-bolt", new QualityCheckBolt(sinkLogFileName))
                .shuffleGrouping("demographic-check-bolt").addConfiguration("site", "uh");;

        builder.setBolt("packet-validation-bolt", new PacketValidationBolt(sinkLogFileName))
                .shuffleGrouping("quality-dedup-check-bolt").addConfiguration("site", "uh");;

        builder.setBolt("biometric-dedup-bolt", new BioDedupBolt(sinkLogFileName))
                .shuffleGrouping("packet-validation-bolt").addConfiguration("site", "uh");;

        builder.setBolt("manual-dedup", new ManualDedupCheckBolt(sinkLogFileName))
                .shuffleGrouping("biometric-dedup-bolt").addConfiguration("site", "ufl");;

        builder.setBolt("aadhar-gen-bolt", new AadharGenerationBolt(sinkLogFileName))
                .shuffleGrouping("biometric-dedup-bolt").addConfiguration("site", "ufl");;
//                .shuffleGrouping("manual-dedup");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("aadhar-gen-bolt").addConfiguration("site", "ufl");;

//        builder.setBolt("fail-bolt", new FailBolt())
//                .shuffleGrouping("packet-extraction-bolt")
//                .shuffleGrouping("demographic-check-bolt")
//                .shuffleGrouping("biometric-dedup-check-bolt")
//                .shuffleGrouping("quality-dedup-check-bolt")
//                .shuffleGrouping("packet-validation-bolt")
//                .shuffleGrouping("biometric-dedup-bolt")
//                .shuffleGrouping("manual-dedup");

//
//        if (args != null && args.length > 0) {
//            config.setNumWorkers(1);
//            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
//        }
//        else {
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("UIDAI_Topology", config, builder.createTopology());
//        }



        StormTopology stormTopology = builder.createTopology();
        if (argumentClass.getDeploymentMode().equals("C")) {

            config.setNumWorkers((stormTopology.get_spouts_size() + stormTopology.get_bolts_size()));
            StormSubmitter.submitTopology(argumentClass.getTopoName(), config, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), config, stormTopology);
            Utils.sleep(1000000000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }

    }
}

//L   IdentityTopology   /Users/anshushukla/data/experi-smartplug-10min.csv     PLUG-106  0.1   /Users/anshushukla/data/output/temp

//L   UIDAI_Topology   /Users/anshushukla/Downloads/wbdbUIDAI/eventDistCount.csv      PLUG-106  0.001   /Users/anshushukla/data/output/temp