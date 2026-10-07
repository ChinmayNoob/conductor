package main

import (
	"flag"
	"hash/fnv"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/ChinmayNoob/conductor/pkg/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	coordinatorAddr = flag.String("coordinator", "localhost:8080", "The Coordinator Address")
	workerPort      = flag.String("worker_port", ":9000", "The Worker Service Port")
	workerIDFlag    = flag.Uint("worker_id", 0, "The Worker ID")
)

func main() {
	flag.Parse()

	workerID := uint32(*workerIDFlag)

	if workerID == 0 {
		if idStr := os.Getenv("WORKER_ID"); idStr != "" {
			if id, err := strconv.ParseUint(idStr, 10, 32); err == nil {
				workerID = uint32(id)
			}
		}
	}

	workerAddress := os.Getenv("WORKER_ADDRESS")
	if workerAddress == "" {
		// Replicas share one service name, so advertise this instance's own IP
		// rather than a hostname that could resolve to any of them.
		workerAddress = localIP(*coordinatorAddr)
	}
	workerAddress += *workerPort

	if workerID == 0 {
		// Derive the ID from the address so replicas get distinct IDs.
		h := fnv.New32a()
		h.Write([]byte(workerAddress))
		workerID = max(h.Sum32(), 1)
	}

	log.Printf("Starting Worker Service (ID=%d, Address=%s)...", workerID, workerAddress)

	//connect to coord

	conn, err := grpc.NewClient(*coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	coordinatorClient := grpcapi.NewCoordinatorServiceClient(conn)
	log.Printf("Connected to coordinator at %s", *coordinatorAddr)

	lis, err := net.Listen("tcp", *workerPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	workerServer := worker.NewServer(workerID, workerAddress, coordinatorClient)
	grpcapi.RegisterWorkerServiceServer(grpcServer, workerServer)

	log.Printf("Worker server listening on %s", *workerPort)
	log.Println("Ready to receive tasks from coordinator...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// localIP returns the IP this machine uses to reach the coordinator.
func localIP(coordinatorAddr string) string {
	// Dialing UDP sends nothing; it only picks the outgoing interface.
	conn, err := net.Dial("udp", coordinatorAddr)
	if err != nil {
		log.Printf("Could not determine local IP, using localhost: %v", err)
		return "localhost"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
