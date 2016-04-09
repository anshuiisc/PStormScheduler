package in.dream_lab.genevents.api;

import in.dream_lab.genevents.config.LatencyConfig;

public class PacketExtractionAPI {

    // Functions used by PacketExtractionBolt
    public static void packetExtraction(){
        LatencyConfig.doStringOp(48);
//        sleep(LatencyConfig.PACKET_EXTRACTION_LATENCY);
    }

//    public static void saveToMySQl(){
//        sleep(LatencyConfig.saveToMySQlLatency);
//    }
//
//    public static void saveToSOLR(){
//        sleep(LatencyConfig.saveToSOLRLatency);
//    }
//
//    public static void saveToXFS(){
//        sleep(LatencyConfig.saveToXFSLatency);
//    }
//
//    public static void replicateToDC(){
//        sleep(LatencyConfig.replicateToDCLatency);
//    }

}
