# Proglog

Proglog is a personal practice project which is learned from [Distributed Services with Go](https://pragprog.com/titles/tjgo/distributed-services-with-go/).

## 1. Build a commit log http server prototype
* Build HTTP server and provide basic JSON API.
* Add log prototype implementation.
* Add http test code for API handler.

## 2. Define log protobuf message
* Define log struct in proto file.
* Add basic Makefile to help compile proto file and run unit test.

## 3. Implement Log package
* Implement store, index, segment, log struct.
* Add unit test for the above struct.

## 4. Provide gRPC API for log function
* Support to access log function through gRPC API.
* Add unit tests for the gRPC API of log function.

## 5. Improve the security level of the service
* Encryption of communication between client and server side.
* Implement two-ways authentication.
* Use casbin library to implement basic ACL mechanism.
* Integrate authorization logic in gRPC API and complete unit.

## 6. Improve observability
* Integrate basic metrics and trace function for gRPC API.

## 7. Add service discovery support and replicator logic
* Import serf library to implement service discovery function.
* Support to replicate log data from other peer node.
* Create Agent struct to manage the different components.
* (Bug) Data replication loop cycle.

## 8. Add distribute log function
* Use Raft library to solve distributed consensus problem, support to set up leader-follower log cluster.
* Integrate it with discovery and agent, complete related test cases.
* Remove custom replicator part in chapter7.

## 9. Support client discovery mechanism
* Custom Resolver and Picker logic, let leader process produce call, and followers process consume call.
* Add API to get all raft server address.

## 10. Make proglog deployable on local k8s
* Add Dockerfile and Helm Chart definition file.
* Import grpc health protocol on gRPC server.
* Support CLI launch mode.

## 11. Deploy proglog to GKE
* Unfinished, meet some issues about MetaController trigger part.
