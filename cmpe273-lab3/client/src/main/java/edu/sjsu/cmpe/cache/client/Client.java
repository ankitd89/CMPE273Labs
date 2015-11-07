package edu.sjsu.cmpe.cache.client;
import java.util.ArrayList;
import com.google.common.hash.Hashing;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CacheServiceInterface cache = new DistributedCacheService(
                "http://localhost:3000");
        CacheServiceInterface cache1 = new DistributedCacheService(
                "http://localhost:3001");
        CacheServiceInterface cache2 = new DistributedCacheService(
                "http://localhost:3002");
        String [] valueArray = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        ArrayList<CacheServiceInterface> serverList = new ArrayList<CacheServiceInterface>();
        serverList.add(cache);
        serverList.add(cache1);
        serverList.add(cache2);

        System.out.println("Inserting values");
        for (int keyIndex = 1; keyIndex < 11; keyIndex++) {
            int key = Hashing.consistentHash(Hashing.md5().hashString(Integer.toString(keyIndex)), serverList.size());
            System.out.println("Put(" + keyIndex + " => " + valueArray[keyIndex-1]  + ") to server " + serverList.get(key).getCacheServerUrl());
            serverList.get(key).put(keyIndex, valueArray[keyIndex-1]);
        }

        System.out.println("Retrieving values");
        for (int keyIndex = 1; keyIndex < 11; keyIndex++) {
            int key = Hashing.consistentHash(Hashing.md5().hashString(Integer.toString(keyIndex)), serverList.size());
            System.out.println("Get(" + keyIndex + " => " + valueArray[keyIndex-1] + ") from server " + serverList.get(key).getCacheServerUrl());
            serverList.get(key).get(keyIndex);
        }
//        cache.put(1, "foo");
//        System.out.println("put(1 => foo)");
//
//        String value = cache.get(1);
//        System.out.println("get(1) => " + value);

        System.out.println("Existing Cache Client...");
    }

}
