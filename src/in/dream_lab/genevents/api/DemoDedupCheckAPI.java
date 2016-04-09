package in.dream_lab.genevents.api;

import in.dream_lab.genevents.config.LatencyConfig;

public class DemoDedupCheckAPI {

    // Functions used by DemoDedupCheckBolt
    public static void DedupCheck(){
        LatencyConfig.doStringOp(40);
//        sleep(LatencyConfig.DEDUP_CHECK_LATENCY);
    }

//    public static void packetDDCQuery(){
//        sleep(LatencyConfig.packetDDCQueryLatency);
//    }

}
