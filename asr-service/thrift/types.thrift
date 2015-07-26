namespace java edu.umich.clarity.thrift

struct THostPort {
	1: string ip;
	2: i32 port;
}

struct QueryInput {
	1: optional list<string> tags;
	2: list<binary> input;
	3: string type;	//type of data (e.g. audio, image, etc)
}

struct QuerySpec {
	1: optional string name;
	// 2: map<string, QueryInput> inputset;
	2: list<QueryInput> inputset;
}

struct RegMessage {
	1: string app_name;
	2: THostPort endpoint;
}

struct LatencyStat {
	1: THostPort hostport;
	2: i64 latency;
}
