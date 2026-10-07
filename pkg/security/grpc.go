package security

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/ChinmayNoob/conductor/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const authHeader = "authorization"

// ServerOptions returns gRPC server options that require the cluster token on
// every call, and serve TLS when a certificate is configured. With a CA
// configured, clients must also present a certificate signed by it (mTLS).
func ServerOptions(cfg *config.Config) ([]grpc.ServerOption, error) {
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryTokenCheck(cfg.ClusterToken)),
		grpc.ChainStreamInterceptor(streamTokenCheck(cfg.ClusterToken)),
	}
	if cfg.TLS.Enabled() {
		tlsCfg, err := serverTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	return opts, nil
}

// DialOptions returns gRPC client options that send the cluster token and use
// TLS when configured.
func DialOptions(cfg *config.Config) ([]grpc.DialOption, error) {
	token := "Bearer " + cfg.ClusterToken
	opts := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, authHeader, token)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
			method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = metadata.AppendToOutgoingContext(ctx, authHeader, token)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}
	if cfg.TLS.Enabled() {
		tlsCfg, err := clientTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return opts, nil
}

func checkToken(ctx context.Context, token string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	want := []byte("Bearer " + token)
	for _, got := range md.Get(authHeader) {
		if subtle.ConstantTimeCompare([]byte(got), want) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "invalid or missing cluster token")
}

func unaryTokenCheck(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkToken(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func streamTokenCheck(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkToken(ss.Context(), token); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func serverTLS(c config.TLS) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pool, err := loadCA(c.CAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsCfg, nil
}

func clientTLS(c config.TLS) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	// Components dial each other by IP, so verify against a fixed name that
	// the shared cluster certificate carries instead of the dialed host.
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   c.ServerName,
		MinVersion:   tls.VersionTLS12,
	}
	if c.CAFile != "" {
		pool, err := loadCA(c.CAFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}
	return tlsCfg, nil
}

func loadCA(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("CA file contains no valid certificates")
	}
	return pool, nil
}
