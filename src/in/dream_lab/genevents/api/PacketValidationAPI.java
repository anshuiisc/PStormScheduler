package in.dream_lab.genevents.api;


import in.dream_lab.genevents.config.LatencyConfig;

public class PacketValidationAPI {

    // Functions used by packetValidationBolt
    public static void packetValidation(){
        LatencyConfig.doStringOp(40);
//        sleep(LatencyConfig.PACKET_VALIDATION_LATENCY);
    }

//    public static void packteOSIValidation(){
//        sleep(LatencyConfig.packetOSIValidationLatency);
//    }

}
