package test;

import java.io.IOException;

public class Exp4 {
    public static void main(String args[]){
        try {
            IndustryCount.run(args[0], args[1], args[2]);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
