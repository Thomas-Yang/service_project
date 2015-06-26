package edu.umich.clarity.service;

import edu.umich.clarity.service.util.ServiceTypes;
import edu.umich.clarity.service.util.TClient;
import edu.umich.clarity.thrift.*;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The client for testing the QA service. This client generates concurrent loads following Poisson distribution with tunable mean interval to stress the QA service.
 *
 * @author Hailong on 6/24/15.
 */
public class StressQAClient {
    private static final double mean = 100;
    private static final int num_client = 16;

    public static void main(String[] args) {
        StressQAClient client = new StressQAClient();
        client.genPoissonLoad(mean, num_client);
    }

    /**
     * Generate the load to QA service that follows Poisson distribution.
     *
     * @param mean       the mean time interval to submit each query
     * @param num_client that submitting the query concurrently
     */
    public void genPoissonLoad(double mean, int num_client) {
        PoissonDistribution poi_dist = new PoissonDistribution(mean);
        for (int i = 0; i < num_client; i++) {
            new Thread(new ConcurrentClient(poi_dist.sample())).start();
        }
    }

    private class ConcurrentClient implements Runnable {
        private IPAService.Client qaClient;

        private SchedulerService.Client ccClient;

        private static final String CC_SERVICE_IP = "141.212.107.226";

        private static final int CC_SERVICE_PORT = 8888;
        private double nap_time;

        public ConcurrentClient(double nap_time) {
            this.nap_time = nap_time;
        }

        @Override
        public void run() {
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
                Thread.sleep(Math.round(nap_time));
                System.out.println(new String(qaClient.submitQuery(query).array()).replaceAll("\n", ""));
            } catch (IOException | TException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
