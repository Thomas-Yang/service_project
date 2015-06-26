package edu.umich.clarity.service.ipa;

import edu.umich.clarity.service.util.ServiceTypes;
import edu.umich.clarity.service.util.TClient;
import edu.umich.clarity.service.util.TServers;
import edu.umich.clarity.thrift.*;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SiriusService implements IPAService.Iface {
    private static final String SERVICE_IP = "localhost";
    private static final int SERVICE_PORT = 7788;
    private static final String SCHEDULER_IP = "localhost";
    private static final int SCHEDULER_PORT = 8888;
    private static final Logger LOG = Logger.getLogger(SiriusService.class);

    private static SchedulerService.Client scheduler_client;
    // private static IPAService.Client service_client;

    public void initialize() {
        try {
            scheduler_client = TClient.creatSchedulerClient(SCHEDULER_IP,
                    SCHEDULER_PORT);
        } catch (IOException ex) {
            LOG.error("Error creating thrift scheduler client"
                    + ex.getMessage());
        }
        try {
            THostPort hostPort = new THostPort(SERVICE_IP, SERVICE_PORT);
            RegMessage regMessage = new RegMessage(ServiceTypes.SIRIUS_SERVICE, hostPort);
            LOG.info("registering to command center runnig @" + SCHEDULER_IP
                    + ":" + SCHEDULER_PORT);
            scheduler_client.registerBackend(regMessage);
            LOG.info("service " + ServiceTypes.SIRIUS_SERVICE
                    + " successfully registered with command center");
        } catch (TException ex) {
            LOG.error("Error registering backend service " + ex.getMessage());
        }
    }

    private ByteBuffer invokeService(String serviceType, QuerySpec query) {
        ByteBuffer sResult = null;
        IPAService.Client asrClient = null;
        try {
            THostPort hostPort = scheduler_client.consultAddress(serviceType);
            LOG.info(serviceType + " query " + "is sent to service @" + hostPort.getIp() + ":" + hostPort.getPort());
            try {
                asrClient = TClient.creatIPAClient(hostPort.getIp(), hostPort.getPort());
            } catch (IOException e) {
            }
            sResult = asrClient.submitQuery(query);
            LOG.info(serviceType + " results are: " + new String(sResult.array()));
        } catch (TException ex) {
        }
        return sResult;
    }

    @Override
    public ByteBuffer submitQuery(QuerySpec query) throws TException {
        ByteBuffer wfResult = null;
        long start_time = System.currentTimeMillis();
        Map<String, QueryInput> inputSet = query.getInputset();
        if (inputSet.containsKey(ServiceTypes.SERVICE_INPUT_AUDIO) && inputSet.containsKey(ServiceTypes.SERVICE_INPUT_IMAGE)) {
            QueryClientRunnable immClient = new QueryClientRunnable(scheduler_client.consultAddress(ServiceTypes.IMM_SERVICE), inputSet.get(ServiceTypes.SERVICE_INPUT_IMAGE));
            QueryClientRunnable asrClient = new QueryClientRunnable(scheduler_client.consultAddress(ServiceTypes.ASR_SERVICE), inputSet.get(ServiceTypes.SERVICE_INPUT_AUDIO));
            Thread immThread = new Thread(immClient);
            Thread asrThread = new Thread(asrClient);
            immThread.start();
            asrThread.start();
            try {
                immThread.join();
                asrThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String tempResult = new String(immClient.getServiceResult().array()) + " " + new String(asrClient.getServiceResult().array());
            Map<String, QueryInput> inputset = new HashMap<String, QueryInput>();
            QueryInput queryInput = new QueryInput();
            queryInput.setInput(tempResult.getBytes());
            inputset.put(ServiceTypes.SERVICE_INPUT_TEXT, queryInput);
            query.setInputset(inputset);
            wfResult = invokeService(ServiceTypes.QA_SERVICE, query);
        } else if (inputSet.containsKey(ServiceTypes.SERVICE_INPUT_AUDIO)) {
            // do some pre-process of the data
            // e.g. format conversion
            wfResult = invokeService(ServiceTypes.ASR_SERVICE, query);
        } else if (inputSet.containsKey(ServiceTypes.SERVICE_INPUT_IMAGE)) {
            // do some pre-process of the data
            // e.g. format conversion
            wfResult = invokeService(ServiceTypes.IMM_SERVICE, query);
        } else if (inputSet.containsKey(ServiceTypes.SERVICE_INPUT_TEXT)) {
            // do some pre-process of the data
            // e.g. format conversion
            wfResult = invokeService(ServiceTypes.QA_SERVICE, query);
        } else {
            LOG.error("unrecognized data format within the sirius workflow");
        }
        long end_time = System.currentTimeMillis();
        LatencyStat latencyStat = new LatencyStat();
        latencyStat.setHostport(new THostPort(SERVICE_IP, SERVICE_PORT));
        latencyStat.setLatency(end_time - start_time);
        LOG.info("the latency for " + ServiceTypes.SIRIUS_SERVICE + " service is " + (end_time - start_time) + " ms");
        LOG.info("update the latency statistics in command center....");
        scheduler_client.updateLatencyStat(ServiceTypes.SIRIUS_SERVICE, latencyStat);
        return wfResult;
    }

    private class QueryClientRunnable implements Runnable {
        private volatile ByteBuffer serviceResult;
        private IPAService.Client service_client;
        private QueryInput queryInput;
        THostPort hostPort;

        public QueryClientRunnable(THostPort hostPort, QueryInput queryInput) {
            this.hostPort = hostPort;
            this.queryInput = queryInput;
        }

        public ByteBuffer getServiceResult() {
            return this.serviceResult;
        }

        @Override
        public void run() {
            try {
                service_client = TClient.creatIPAClient(hostPort.getIp(), hostPort.getPort());
                Map<String, QueryInput> inputset = new HashMap<String, QueryInput>();
                inputset.put(ServiceTypes.SERVICE_INPUT_TEXT, this.queryInput);
                QuerySpec querySpec = new QuerySpec();
                querySpec.setInputset(inputset);
                try {
                    this.serviceResult = service_client.submitQuery(querySpec);
                } catch (TException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, TException {
        SiriusService qaService = new SiriusService();
        IPAService.Processor<IPAService.Iface> processor = new IPAService.Processor<IPAService.Iface>(
                qaService);
        TServers.launchThreadedThriftServer(SERVICE_PORT, processor);
        LOG.info("starting " + ServiceTypes.SIRIUS_SERVICE + " service @" + SERVICE_IP
                + ":" + SERVICE_PORT);
        qaService.initialize();
    }
}
