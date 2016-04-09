package in.dream_lab.genevents.api;


//import config.LatencyConfig;

import in.dream_lab.genevents.config.LatencyConfig;

public class AadharGenerationAPI {

    // Functions used by AadharGenerationBolt
    public static void getAadharNo(String fileContent){


            LatencyConfig.doStringOp(fileContent);


    }



}
