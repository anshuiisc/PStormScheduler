package in.dream_lab.genevents.factory;

import in.dream_lab.genevents.config.LatencyConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by anshushukla on 06/10/15.
 */
public class computelogic {


    public static void main(String[] args) {


        String rowString = null;
        try {
            rowString = LatencyConfig.readFileWithSize("src/file_6MB", StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.out.println("files not readable");
            e.printStackTrace();
        }

//        System.out.println(rowString);

        long startTime = System.nanoTime();
//Insert your logic here
//        for(int j=0;j<4;j++) {
            String upper = rowString.toUpperCase();
//        }
// COmpute Logic ends
        long stopTime = System.nanoTime();
        System.out.println((stopTime - startTime));
        System.out.println((stopTime - startTime)/(1000000.0));


    }
}
