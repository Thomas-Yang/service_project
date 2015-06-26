package edu.umich.clarity.service.ipa;

import edu.umich.clarity.service.util.Configurations;
import edu.umich.clarity.service.util.ServiceTypes;
import edu.umich.clarity.service.util.TClient;
import edu.umich.clarity.service.util.TServers;
import edu.umich.clarity.thrift.*;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The implementation of Sirius application using the services of the ClarEco system.
 * Note: Sirius is also a service within ClarEco system which makes application and service interchangable.
 *
 * @author Hailong on on 6/24/15.
 */
public class SiriusService implements IPAService.Iface {
    private static final String CONFIG_PATH = "conf.properties";
    private static Properties properties;
    private String SERVICE_IP;
    private int SERVICE_PORT;
    private String SCHEDULER_IP;
    private int SCHEDULER_PORT;
    private static final Logger LOG = Logger.getLogger(SiriusService.class);
    private static SchedulerService.Client scheduler_client;

    public SiriusService(String ccIp, String ccPort, String sIp, String sPort) {
        this.SCHEDULER_IP = ccIp;
        this.SCHEDULER_PORT = new Integer(ccPort);
        this.SERVICE_IP = sIp;
        this.SERVICE_PORT = new Integer(sPort);
    }

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
//            LOG.info("service " + ServiceTypes.SIRIUS_SERVICE
//                    + " successfully registered with command center");
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
            LOG.info("receiving sirius query of speech recognition and image matching...");
            QueryClientRunnable immClient = new QueryClientRunnable(ServiceTypes.IMM_SERVICE, inputSet);
            QueryClientRunnable asrClient = new QueryClientRunnable(ServiceTypes.ASR_SERVICE, inputSet);
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
            String asrResult = new String(asrClient.getServiceResult().array());
            String immResult = new String(immClient.getServiceResult().array());
            String tempResult = asrResult + " " + immResult;
            LOG.info("speech recognition result is: " + asrResult.replaceAll("\n", ""));
            LOG.info("image matching result is: " + immResult);
            LOG.info("generating question for the QA service: " + tempResult);
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
        private ByteBuffer serviceResult;
        private IPAService.Client service_client;
        private Map<String, QueryInput> inputSet;
        private String service_type;
        private SchedulerService.Client scheduler_client;

        public QueryClientRunnable(String service_type, Map<String, QueryInput> inputSet) {
            this.inputSet = inputSet;
            this.service_type = service_type;
        }

        public ByteBuffer getServiceResult() {
            return this.serviceResult;
        }

        @Override
        public void run() {
            try {
                scheduler_client = TClient.creatSchedulerClient(SCHEDULER_IP,
                        SCHEDULER_PORT);
                THostPort hostPort = null;
                try {
                    hostPort = scheduler_client.consultAddress(service_type);
                } catch (TException e) {
                    e.printStackTrace();
                }
                service_client = TClient.creatIPAClient(hostPort.getIp(), hostPort.getPort());
                Map<String, QueryInput> inputset = new HashMap<String, QueryInput>();
                if (service_type.equalsIgnoreCase(ServiceTypes.IMM_SERVICE)) {
                    inputset.put(ServiceTypes.SERVICE_INPUT_IMAGE, inputSet.get(ServiceTypes.SERVICE_INPUT_IMAGE));
                } else if (service_type.equalsIgnoreCase(ServiceTypes.ASR_SERVICE)) {
                    inputset.put(ServiceTypes.SERVICE_INPUT_AUDIO, inputSet.get(ServiceTypes.SERVICE_INPUT_AUDIO));
                } else if (service_type.equalsIgnoreCase(ServiceTypes.QA_SERVICE)) {
                    inputset.put(ServiceTypes.SERVICE_INPUT_TEXT, inputSet.get(ServiceTypes.SERVICE_INPUT_TEXT));
                }
                QuerySpec querySpec = new QuerySpec();
                querySpec.setInputset(inputset);
                try {
                    serviceResult = service_client.submitQuery(querySpec);
                } catch (TException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, TException {
        InputStream conf = SiriusService.class.getClassLoader().getResourceAsStream(CONFIG_PATH);
        properties = new Properties();
        properties.load(conf);
        String ccIp = properties.getProperty(Configurations.COMMAND_CENTER_IP);
        String ccPort = properties.getProperty(Configurations.COMMAND_CENTER_PORT);
        String sIp = properties.getProperty(Configurations.SERVICE_IP);
        String sPort = properties.getProperty(Configurations.SERVICE_PORT);
        SiriusService siriusService = new SiriusService(ccIp, ccPort, sIp, sPort);
        IPAService.Processor<IPAService.Iface> processor = new IPAService.Processor<IPAService.Iface>(
                siriusService);
        TServers.launchThreadedThriftServer(new Integer(sPort), processor);
        LOG.info("starting " + ServiceTypes.SIRIUS_SERVICE + " service @" + sIp
                + ":" + sPort);
        siriusService.initialize();
        LOG.info("service sirius is ready...");
    }
}
