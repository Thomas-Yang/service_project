package edu.umich.clarity.service.scheduler;

import edu.umich.clarity.service.util.TServers;
import edu.umich.clarity.thrift.LatencyStat;
import edu.umich.clarity.thrift.RegMessage;
import edu.umich.clarity.thrift.SchedulerService;
import edu.umich.clarity.thrift.THostPort;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CommandCenter implements SchedulerService.Iface {

    private static final Logger LOG = Logger.getLogger(CommandCenter.class);
    private static final String SCHEDULER_IP = "localhost";
    private static final int SCHEDULER_PORT = 8888;
    private static ConcurrentMap<String, List<THostPort>> serviceMap = new ConcurrentHashMap<String, List<THostPort>>();
    private static ConcurrentMap<String, List<LatencyStat>> latencyStatMap = new ConcurrentHashMap<String, List<LatencyStat>>();

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

    private THostPort randomAssignService(List<THostPort> service_list) {
        THostPort hostPort;
        Random rand = new Random();
        hostPort = service_list.get(rand.nextInt(service_list.size()));
        return hostPort;
    }

    public static void main(String[] args) throws IOException {
        CommandCenter commandCenter = new CommandCenter();
        SchedulerService.Processor<SchedulerService.Iface> processor = new SchedulerService.Processor<SchedulerService.Iface>(
                commandCenter);
        TServers.launchThreadedThriftServer(SCHEDULER_PORT, processor);
        LOG.info("starting command center @" + SCHEDULER_IP + ":"
                + SCHEDULER_PORT);
    }
}
