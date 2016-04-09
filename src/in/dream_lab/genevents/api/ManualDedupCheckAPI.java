package in.dream_lab.genevents.api;

import in.dream_lab.genevents.config.LatencyConfig;

public class ManualDedupCheckAPI {

    // Functions used by ManualDedupCheckBolt
    public static void manualDedup(){
        LatencyConfig.doStringOp(90);
//        sleep(LatencyConfig.MANUAL_DEDUP_LATENCY);
    }

}
