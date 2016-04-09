package in.dream_lab.genevents.api;


import in.dream_lab.genevents.config.LatencyConfig;

public class QualityCheckAPI {

    // Functions used by QualityCheck
    public static void qualityCheck(){
        LatencyConfig.doStringOp(225);
//        sleep(LatencyConfig.QUALITY_CHECK_LATENCY);
    }

}
