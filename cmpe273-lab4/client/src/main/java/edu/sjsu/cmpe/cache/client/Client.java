package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.Unirest;

import java.util.*;
import java.lang.*;
import java.io.*;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CRDTClient crdtClient = new CRDTClient();

        boolean result = crdtClient.put(1, "a");
        System.out.println("result is " + result);
        Thread.sleep(30*1000);
        System.out.println("1: put(1 => a); sleeping 30s");

        crdtClient.put(1, "b");
        Thread.sleep(30*1000);
        System.out.println("2: put(1 => b); sleeping 30s");

        String value = crdtClient.get(1);
        System.out.println("3: get(1) => " + value);

        System.out.println("Exiting Client...");
        Unirest.shutdown();
    }

}
