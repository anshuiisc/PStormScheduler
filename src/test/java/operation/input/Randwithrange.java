package test.java.operation.input;

import java.util.Random;

/**
 * Created by anshushukla on 26/01/16.
 */
public class Randwithrange {
    public static void main(String[] args) {
        int i=10000;
        while(i-->0) {
            Random r = new Random();
            int p=r.nextInt(1000);
            if(p<100)
            System.out.println(p);
        }

    }
}
