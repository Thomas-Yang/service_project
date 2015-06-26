This is the service ecosystem including the command center as well as corresponding services that comply to the API.

#### Command Center

The command center maintains the membership of all the services and serves as a consultant for services talking to each other. The thrift directory contains the definition of service interfaces. When changing the interfaces, keep in mind it affects all the services within the ecosystem. Also the command center provides logging support so that all the historical service information is kept for future inspection.

#### ASR Service

The ASR service only contains a naive implementation of pocketsphinx, however it serves as a template on how to integrate thrift with C++/C applications.

#### IMM Service

The IMM service is also an example on how to tightly integrate thrift with C++/C applications. It also cleans up the origin implementation(removes the non-used protobuf codes).

#### QA Service

The QA service is more robust than the original one under multiple queries. It initializes one instance of OpenEphyra during the setup and reuse the instance for the coming queries. The parallelism could come from multiple QA services. However, this is a quick fix for the crash of creating multiple OpenEphyra instances, a permanent fix is still required for the production run.

#### Sirius Service

The Sirius service is a demonstration about how to use the services within the ecosystem to build another service with a particular workflow.

#### Test Clients

It contains multiple clients serving the purpose to test individual services. Also a QA client that generates load following poisson distribution is includes to stress the service with concurrent queries.

