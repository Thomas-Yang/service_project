package edu.umich.clarity.service;

import edu.umich.clarity.service.util.ServiceTypes;
import edu.umich.clarity.service.util.TClient;
import edu.umich.clarity.thrift.*;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The client for testing the QA service.
 *
 * @author Hailong on 6/24/15.
 */
public class QAClient {
    private static IPAService.Client qaClient;

    private static SchedulerService.Client ccClient;

    private static final String CC_SERVICE_IP = "141.212.107.226";

    private static final int CC_SERVICE_PORT = 8888;

    public static void main(String[] args) {
        try {
            ccClient = TClient.creatSchedulerClient(CC_SERVICE_IP, CC_SERVICE_PORT);
            THostPort hostPort = ccClient.consultAddress(ServiceTypes.QA_SERVICE);
            System.out.println("Command Center returns " + hostPort.getIp() + ":" + hostPort.getPort());
            qaClient = TClient.creatIPAClient(hostPort.getIp(), hostPort.getPort());
            QuerySpec query = new QuerySpec();
            query.setName("test-query");
            String question = "what is the speed of the light?";
            QueryInput queryInput = new QueryInput();
            queryInput.setInput(question.getBytes());
            Map<String, QueryInput> inputSet = new HashMap<String, QueryInput>();
            inputSet.put(ServiceTypes.SERVICE_INPUT_TEXT, queryInput);
            query.setInputset(inputSet);
            System.out.println(new String(qaClient.submitQuery(query).array()));
        } catch (IOException | TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
