package security

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// serve starts a gRPC server with the health service and returns its address.
func serve(t *testing.T, cfg *config.Config) string {
	t.Helper()
	opts, err := ServerOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(s, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func check(t *testing.T, addr string, opts ...grpc.DialOption) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func dial(t *testing.T, cfg *config.Config) []grpc.DialOption {
	t.Helper()
	opts, err := DialOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return opts
}

func TestClusterToken(t *testing.T) {
	cfg := &config.Config{ClusterToken: "secret"}
	addr := serve(t, cfg)

	if err := check(t, addr, dial(t, cfg)...); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}

	wrong := &config.Config{ClusterToken: "wrong"}
	if err := check(t, addr, dial(t, wrong)...); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: got %v, want Unauthenticated", err)
	}

	err := check(t, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token: got %v, want Unauthenticated", err)
	}
}

func TestMutualTLS(t *testing.T) {
	dir := t.TempDir()
	ca, caKey := writeCert(t, dir, "ca", nil, nil, "conductor-test-ca")
	writeCert(t, dir, "cluster", ca, caKey, "conductor")
	otherCA, otherKey := writeCert(t, dir, "other-ca", nil, nil, "someone-else")
	writeCert(t, dir, "intruder", otherCA, otherKey, "conductor")

	tlsCfg := func(name string) config.TLS {
		return config.TLS{
			CertFile:   filepath.Join(dir, name+".crt"),
			KeyFile:    filepath.Join(dir, name+".key"),
			CAFile:     filepath.Join(dir, "ca.crt"),
			ServerName: "conductor",
		}
	}
	server := &config.Config{ClusterToken: "secret", TLS: tlsCfg("cluster")}
	addr := serve(t, server)

	if err := check(t, addr, dial(t, server)...); err != nil {
		t.Fatalf("mTLS with a valid certificate failed: %v", err)
	}

	// A client certificate from another CA must be refused.
	intruder := &config.Config{ClusterToken: "secret", TLS: tlsCfg("intruder")}
	intruder.TLS.CAFile = filepath.Join(dir, "ca.crt")
	if err := check(t, addr, dial(t, intruder)...); err == nil {
		t.Fatal("client certificate from an unknown CA was accepted")
	}

	// Plaintext clients must be refused.
	if err := check(t, addr, dial(t, &config.Config{ClusterToken: "secret"})...); err == nil {
		t.Fatal("plaintext client was accepted by a TLS server")
	}
}

// writeCert creates name.crt and name.key in dir. With a nil parent it makes a
// self-signed CA; otherwise a leaf for cn signed by parent.
func writeCert(t *testing.T, dir, name string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, cn string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	if parent == nil {
		tmpl.IsCA = true
		tmpl.BasicConstraintsValid = true
		tmpl.KeyUsage = x509.KeyUsageCertSign
		parent, parentKey = tmpl, key
	} else {
		tmpl.DNSNames = []string{cn}
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, filepath.Join(dir, name+".crt"), "CERTIFICATE", der)
	writePEM(t, filepath.Join(dir, name+".key"), "EC PRIVATE KEY", keyDER)

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
