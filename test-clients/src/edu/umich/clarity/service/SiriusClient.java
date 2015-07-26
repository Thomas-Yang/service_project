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
 * The client for testing the Sirius service.
 *
 * @author Hailong on 6/24/15.
 */
public class SiriusClient {
    public static final String IMG_PATH = "/home/hailong/IdeaProjects/Mulage/test.jpg";
    public static final String AUDIO_PATH = "/home/hailong/IdeaProjects/Mulage/what.is.the.author.of.harry.potter.wav";
    public static IPAService.Client siriusClient;

    public static SchedulerService.Client ccClient;

    public static String CC_SERVICE_IP = "141.212.107.226";

    public static final int CC_SERVICE_PORT = 8888;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            ccClient = TClient.creatSchedulerClient(CC_SERVICE_IP, CC_SERVICE_PORT);
            THostPort hostPort = ccClient.consultAddress(ServiceTypes.SIRIUS_SERVICE);
            System.out.println("Command Center returns " + hostPort.getIp() + ":" + hostPort.getPort());
            siriusClient = TClient.creatIPAClient(hostPort.getIp(), hostPort.getPort());
            QuerySpec query = new QuerySpec();
            query.setName("test-query");
            File imgFile = new File(IMG_PATH);
            File audioFile = new File(AUDIO_PATH);
            QueryInput imgQueryInput = new QueryInput();
            imgQueryInput.setInput(Files.readAllBytes(imgFile.toPath()));
            QueryInput audioQueryInput = new QueryInput();
            audioQueryInput.setInput(Files.readAllBytes(audioFile.toPath()));
            Map<String, QueryInput> inputSet = new HashMap<String, QueryInput>();
            inputSet.put(ServiceTypes.SERVICE_INPUT_AUDIO, audioQueryInput);
            inputSet.put(ServiceTypes.SERVICE_INPUT_IMAGE, imgQueryInput);
            query.setInputset(inputSet);
            System.out.println(new String(siriusClient.submitQuery(query).array()));
        } catch (IOException | TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
