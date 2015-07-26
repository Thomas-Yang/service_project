include "types.thrift"

namespace java edu.umich.clarity.thrift

# interfaces for command center
service SchedulerService {
	// allow the backend services to register themselfves
	void registerBackend(1: types.RegMessage message),
	// request the service candidate for a certain type
	types.THostPort consultAddress(1: string serviceType)
	// query latency updates from each service
	void updateLatencyStat(1: string name, 2: types.LatencyStat latencyStat);
}

# interfaces for clarEco services
service IPAService {
	// allow to submit query to the service
	binary submitQuery(1: types.QuerySpec query)
}
