package edu.umich.clarity.service;

import edu.umich.clarity.service.util.ServiceTypes;
import edu.umich.clarity.service.util.TClient;
import edu.umich.clarity.thrift.*;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

/**
 * The client for testing the IMM service.
 *
 * @author Hailong on 6/24/15.
 */
public class IMClient {

    public static final String IMG_PATH = "/home/hailong/IdeaProjects/Mulage/test.jpg";

    public static SchedulerService.Client ccClient;

    public static IPAService.Client imClient;

    public static String CC_SERVICE_IP = "141.212.107.226";

    public static final int CC_SERVICE_PORT = 8888;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            ccClient = TClient.creatSchedulerClient(CC_SERVICE_IP, CC_SERVICE_PORT);
            THostPort hostPort = ccClient.consultAddress(ServiceTypes.IMM_SERVICE);
            System.out.println("Command Center returns " + hostPort.getIp() + ":" + hostPort.getPort());
            imClient = TClient.creatIPAClient(hostPort.getIp(), hostPort.getPort());
            QuerySpec query = new QuerySpec();
            query.setName("test-query");
            File imgFile = new File(IMG_PATH);
            // Files.readAllBytes(imgFile.toPath());
            QueryInput queryInput = new QueryInput();
            queryInput.setInput(Files.readAllBytes(imgFile.toPath()));
            Map<String, QueryInput> inputSet = new HashMap<String, QueryInput>();
            inputSet.put(ServiceTypes.SERVICE_INPUT_IMAGE, queryInput);
            query.setInputset(inputSet);
            System.out.println(new String(imClient.submitQuery(query).array()));
        } catch (IOException | TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
