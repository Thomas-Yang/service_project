package edu.umich.clarity.service.scheduler;

import edu.umich.clarity.service.util.Configurations;
import edu.umich.clarity.service.util.TServers;
import edu.umich.clarity.thrift.LatencyStat;
import edu.umich.clarity.thrift.RegMessage;
import edu.umich.clarity.thrift.SchedulerService;
import edu.umich.clarity.thrift.THostPort;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * CommandCenter maintains the real time membership of the ClarEco service system, and perform certain policies(e.g. load balancing) when assigning a service to a client.
 * TODO add the heartbeat mechanism to keep the real time information about services.
 * @author Hailong on 6/24/15.
 */
public class CommandCenter implements SchedulerService.Iface {

    private static final String CONFIG_PATH = "conf.properties";
    private static Properties properties;
    private static final Logger LOG = Logger.getLogger(CommandCenter.class);
    private String SCHEDULER_IP;
    private int SCHEDULER_PORT;
    private static ConcurrentMap<String, List<THostPort>> serviceMap = new ConcurrentHashMap<String, List<THostPort>>();
    private static ConcurrentMap<String, List<LatencyStat>> latencyStatMap = new ConcurrentHashMap<String, List<LatencyStat>>();

    public CommandCenter(String Ip, String Port) {
        this.SCHEDULER_IP = Ip;
        this.SCHEDULER_PORT = new Integer(Port);
    }

    @Override
    public void updateLatencyStat(String name, LatencyStat latencyStat) throws TException {
        if (latencyStatMap.containsKey(name)) {
            latencyStatMap.get(name).add(latencyStat);
        } else {
            List<LatencyStat> latencyStatList = new LinkedList<LatencyStat>();
            latencyStatList.add(latencyStat);
            latencyStatMap.put(name, latencyStatList);
        }
        LOG.info("receiving query statistic update from service " + name + "@" + latencyStat.getHostport().getIp() + ":"
                + latencyStat.getHostport().getPort() + " for " + latencyStat.getLatency() + " ms");
    }

    /**
     * Register the host and port of certain type of service.
     *
     * @param message contains the host and port of the service
     * @throws TException
     */
    @Override
    public void registerBackend(RegMessage message) throws TException {
        String appName = message.getApp_name();
        THostPort hostPort = message.getEndpoint();
        LOG.info("receiving register message from service " + appName
                + " running @" + hostPort.getIp() + ":" + hostPort.getPort());
        if (serviceMap.containsKey(appName)) {
            serviceMap.get(appName).add(hostPort);

        } else {
            // TODO if the same hostport alreay exists in the list, just renew it
            List<THostPort> serviceList = new LinkedList<THostPort>();
            serviceList.add(hostPort);
            serviceMap.put(appName, serviceList);
        }
    }

    @Override
    public THostPort consultAddress(String serviceType) throws TException {
        THostPort hostPort = null;
        List<THostPort> service_list = serviceMap.get(serviceType);
        if (service_list != null && service_list.size() != 0)
            hostPort = randomAssignService(service_list);
        return hostPort;
    }

    /**
     * Randomly choose a service of the required type.
     *
     * @param service_list the service candidates
     * @return the chosen service
     */
    private THostPort randomAssignService(List<THostPort> service_list) {
        THostPort hostPort;
        Random rand = new Random();
        hostPort = service_list.get(rand.nextInt(service_list.size()));
        return hostPort;
    }

    public static void main(String[] args) throws IOException {
        InputStream conf = CommandCenter.class.getClassLoader().getResourceAsStream(CONFIG_PATH);
        properties = new Properties();
        properties.load(conf);
        String ccIp = properties.getProperty(Configurations.COMMAND_CENTER_IP);
        String ccPort = properties.getProperty(Configurations.COMMAND_CENTER_PORT);
        CommandCenter commandCenter = new CommandCenter(ccIp, ccPort);
        SchedulerService.Processor<SchedulerService.Iface> processor = new SchedulerService.Processor<SchedulerService.Iface>(
                commandCenter);
        TServers.launchThreadedThriftServer(new Integer(ccPort), processor);
        LOG.info("starting command center @" + ccIp + ":"
                + ccPort);
    }
}
