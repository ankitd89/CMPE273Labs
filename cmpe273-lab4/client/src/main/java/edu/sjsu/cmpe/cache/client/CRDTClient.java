package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;


public class CRDTClient {

    private ConcurrentHashMap<String, CacheServiceInterface> servers;
    private ArrayList<String> successServersList;
    private ConcurrentHashMap<String, ArrayList<String>> finalResults;

    private static CountDownLatch allDoneSignal;

    public CRDTClient() {

        servers = new ConcurrentHashMap<String, CacheServiceInterface>(3);
        CacheServiceInterface cache0 = new DistributedCacheService("http://localhost:3000", this);
        CacheServiceInterface cache1 = new DistributedCacheService("http://localhost:3001", this);
        CacheServiceInterface cache2 = new DistributedCacheService("http://localhost:3002", this);
        servers.put("http://localhost:3000", cache0);
        servers.put("http://localhost:3001", cache1);
        servers.put("http://localhost:3002", cache2);
    }

    // Callbacks
    public void putFailed(Exception e) {
        System.out.println("The request has failed");
        allDoneSignal.countDown();
    }

    public void putCompleted(HttpResponse<JsonNode> response, String serverUrl) {
        int code = response.getCode();
        System.out.println("completed the put response! code " + code + " on server " + serverUrl);
        successServersList.add(serverUrl);
        allDoneSignal.countDown();
    }

    public void getFailed(Exception e) {
        System.out.println("The request has failed");
        allDoneSignal.countDown();
    }

    
    public void getCompleted(HttpResponse<JsonNode> response, String serverUrl) {

        String value = null;
        if (response != null && response.getCode() == 200) {
            value = response.getBody().getObject().getString("value");
                System.out.println("value from server " + serverUrl + "is " + value);
            ArrayList serversWithValue = finalResults.get(value);
            if (serversWithValue == null) {
                serversWithValue = new ArrayList(3);
            }
            serversWithValue.add(serverUrl);

            // Save Arraylist of servers into finalResults
            finalResults.put(value, serversWithValue);
        }

        allDoneSignal.countDown();
    }



    public boolean put(long key, String value) throws InterruptedException {
        successServersList = new ArrayList(servers.size());
        allDoneSignal = new CountDownLatch(servers.size());

        for (CacheServiceInterface cache : servers.values()) {
            cache.put(key, value);
        }

        allDoneSignal.await();

        boolean isSuccess = Math.round((float)successServersList.size() / servers.size()) == 1;

        if (! isSuccess) {
            // Send delete for the same key
            delete(key, value);
        }
        return isSuccess;
    }

    public void delete(long key, String value) {

        for (final String serverUrl : successServersList) {
            CacheServiceInterface server = servers.get(serverUrl);
            server.delete(key);
        }
    }


    // dictResult = {"value" : [serverUrl1, serverUrl2...]]}
    public String get(long key) throws InterruptedException {
        finalResults = new ConcurrentHashMap<String, ArrayList<String>>();
        allDoneSignal = new CountDownLatch(servers.size());

        for (final CacheServiceInterface server : servers.values()) {
            server.get(key);
        }
        allDoneSignal.await();

        // Take the first element
        String rightValue = finalResults.keys().nextElement();

        // Discrepancy in results (either more than one value gotten, or null gotten somewhere)
        if (finalResults.keySet().size() > 1 || finalResults.get(rightValue).size() != servers.size()) {
            // Most frequent value in finalResults
            ArrayList<String> maxValues = maxKeyForTable(finalResults);
//            System.out.println("maxValues: " + maxValues);
            if (maxValues.size() == 1) {
                // Max value - iterate through dict keys to repair
                rightValue = maxValues.get(0);

                ArrayList<String> repairServers = new ArrayList(servers.keySet());
                repairServers.removeAll(finalResults.get(rightValue));
//                System.out.println("repairServers: " + repairServers);

                for (String serverUrl : repairServers) {
                    // Repair all servers that don't have the correct value
                    System.out.println("repairing: " + serverUrl + " value: " + rightValue);
                    CacheServiceInterface server = servers.get(serverUrl);
                    server.put(key, rightValue);

                }

            } else {
                // Multiple or no max keys? - do nothing
            }
        }

        return rightValue;

    }


    // Returns array of keys with the maximum value
    // If array contains only 1 value, then it is the highest value in the hash map
    public ArrayList<String> maxKeyForTable(ConcurrentHashMap<String, ArrayList<String>> table) {
        ArrayList<String> maxKeys= new ArrayList<String>();
        int maxValue = -1;
        for(Map.Entry<String, ArrayList<String>> entry : table.entrySet()) {
            if(entry.getValue().size() > maxValue) {
                maxKeys.clear(); /* New max remove all current keys */
                maxKeys.add(entry.getKey());
                maxValue = entry.getValue().size();
            }
            else if(entry.getValue().size() == maxValue)
            {
                maxKeys.add(entry.getKey());
            }
        }
        return maxKeys;
    }





}