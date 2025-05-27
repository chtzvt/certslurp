package api

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/chtzvt/certslurp/internal/cluster"
)

// Server wraps the HTTP API and its config/state
type Server struct {
	Cluster cluster.Cluster
	Addr    string
	Logger  *log.Logger
	Config  *Config
	server  *http.Server
}

type Config struct {
	ListenAddr string   `mapstructure:"listen_addr"`
	AuthTokens []string `mapstructure:"auth_tokens"`
}

func NewServer(cluster cluster.Cluster, config Config, logger *log.Logger) *Server {
	return &Server{
		Cluster: cluster,
		Addr:    config.ListenAddr,
		Config:  &config,
		Logger:  logger,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	// Health endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	protected := http.NewServeMux()
	RegisterJobHandlers(protected, s.Cluster)

	mux.Handle("/api/", TokenAuthMiddleware(s.Config.AuthTokens, protected))

	s.server = &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutdownCtx)
	}()
	s.Logger.Printf("API server listening on %s", s.Addr)
	return s.server.ListenAndServe()
}
