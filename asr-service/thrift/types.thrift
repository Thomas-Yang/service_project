namespace java edu.umich.clarity.thrift

struct THostPort {
	1: string ip;
	2: i32 port;
}

struct QueryInput {
	1: optional list<string> tags;
	2: binary input;
}

struct QuerySpec {
	1: optional string name;
	2: map<string, QueryInput> inputset;
}

struct RegMessage {
	1: string app_name;
	2: THostPort endpoint;
}

struct LatencyStat {
	1: THostPort hostport;
	2: i64 latency;
}