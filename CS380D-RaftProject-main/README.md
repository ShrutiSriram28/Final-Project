First, run your frontend server and make sure it's listening on port 8001. 

Then, run "go run testfrontend.go" from the testing folder. 

The testing mechanism interacts with the Frontend using GRPCs, which the frontend directs to the RAFT servers. The generatedfiles/server.proto file contains the specification of the GRPCs I used. 