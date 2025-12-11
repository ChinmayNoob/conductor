package main

import (
	"flag"
	"log"
	"net"

	"github.com/ChinmayNoob/conductor/pkg/coordinator"
	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"google.golang.org/grpc"
)

var coordinatorPort = flag.String("coordinator-port", ":8080", "The Coordinator port")

func main() {
	flag.Parse()

	log.Println("Starting Coorindator Service...")

	database, err := db.New()

	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)

	}

	defer database.Close()
	log.Println("Connected to database")

	//grpc

	lis, err := net.Listen("tcp", *coordinatorPort)

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	coordServer := coordinator.NewServer(database)

	grpcapi.RegisterCoordinatorServiceServer(grpcServer, coordServer)

	log.Printf("Coordinator server listening on %s", *coordinatorPort)
	log.Println("Ready to receive tasks from clients...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
