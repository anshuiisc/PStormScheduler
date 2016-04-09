package in.dream_lab.genevents.api;
import in.dream_lab.genevents.config.LatencyConfig;

public class BioDedupAPI {

    // Functions used by BioDedupBolt
    public static void insertBiometric(){
        LatencyConfig.doStringOp(45);
    }



    public static void identifyBiometric(){

    }

}
