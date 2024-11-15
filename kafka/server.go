package kafka

import (
	"context"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/xuanswe/mini-kafka/internal/encoders"
	"github.com/xuanswe/mini-kafka/internal/models"
	"github.com/xuanswe/mini-kafka/internal/support"
	"io"
	"net"
	"sync"
	"time"
)

type ServerInterface interface {
	Start() error
	ForceShutdown() error
	Shutdown() error
	Config() ServerConfig
}

type Server struct {
	config   ServerConfig
	listener net.Listener
	conns    map[net.Conn]struct{}
}

type ServerConfig struct {
	Host            string
	Port            string
	ConnIdleTimeout time.Duration
}

// onceCloseListener wraps a net.Listener, protecting it from
// multiple Close calls.
type onceCloseListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *onceCloseListener) Close() error {
	oc.once.Do(func() {
		oc.closeErr = oc.Listener.Close()
	})
	return oc.closeErr
}

// onceCloseListener wraps a net.Listener, protecting it from
// multiple Close calls.
type onceCloseConn struct {
	net.Conn
	once     sync.Once
	closeErr error
}

func (oc *onceCloseConn) Close() error {
	oc.once.Do(func() {
		log.Debug().Msgf("Closing connection %v", oc.Conn.RemoteAddr())
		oc.closeErr = oc.Conn.Close()
		if oc.closeErr == nil {
			log.Debug().Msgf("Closed connection %v", oc.Conn.RemoteAddr())
		}
	})
	return oc.closeErr
}

func NewServer(config ServerConfig) (ServerInterface, error) {
	if config.ConnIdleTimeout <= 0 {
		return nil, errors.New("ConnIdleTimeout must be greater than 0")
	}

	return &Server{
		config: config,
		conns:  make(map[net.Conn]struct{}),
	}, nil
}

func (s *Server) Config() ServerConfig {
	return s.config
}

// ForceShutdown immediately closes all active net.Listeners, connections,
// and other resources.
// For a graceful shutdown, use [Server.Shutdown].
func (s *Server) ForceShutdown() error {
	log.Info().Msg("Force shutting down kafka server")
	if err := s.listener.Close(); err != nil {
		return err
	}

	for conn := range s.conns {
		if err := conn.Close(); err != nil {
			log.Error().Err(err).Msgf("Error closing connection %v", conn.RemoteAddr())
		}
		delete(s.conns, conn)
	}

	return nil
}

// Shutdown gracefully shuts down the server without interrupting any active
// connections and resources.
func (s *Server) Shutdown() error {
	// TODO: close gracefully
	//log.Info().Msg("Gracefully shutting down kafka server")
	return s.ForceShutdown()
}

// Start starts the server and block
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", net.JoinHostPort(s.config.Host, s.config.Port))
	if err != nil {
		log.Error().Err(err).Msgf("Failed to bind to %s:%s", s.config.Host, s.config.Port)
		return err
	}
	s.listener = &onceCloseListener{Listener: listener}
	defer func(l net.Listener) {
		if err := l.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing listener")
		}
	}(s.listener)

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	log.Info().Msg("kafka server started")

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Debug().Msg("Listener is closed")
				cancelCtx()
				return nil
			}

			log.Error().Err(err).Msg("Error accepting connection")
			continue
		}
		conn = &onceCloseConn{Conn: conn}
		s.conns[conn] = struct{}{}

		go func() {
			err := handleConnection(ctx, conn, s.config)
			if err != nil {
				log.Error().Err(err).Msg("Error handling connection")
			}
			delete(s.conns, conn)
		}()
	}
}

func handleConnection(ctx context.Context, conn net.Conn, config ServerConfig) error {
	// TODO: Close in-progress connections gracefully?
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Error().Err(err).Msgf("Error closing connection %v", conn.RemoteAddr())
		}
	}(conn)

	connCtx, cancelCtx := context.WithCancelCause(ctx)
	defer cancelCtx(nil)

	requestChan, requestErrChan := createRequestChan(connCtx, conn, config)
	responseChan := createResponseChan(connCtx, requestChan)

	go func() {
		err := sendResponses(connCtx, conn, responseChan)
		cancelCtx(err)
	}()

	select {
	case <-connCtx.Done():
		err := context.Cause(connCtx)
		if err != nil {
			return err
		}
		return connCtx.Err()
	case err := <-requestErrChan:
		cancelCtx(err)
		return err
	}
}

func createRequestChan(ctx context.Context, conn net.Conn, config ServerConfig) (<-chan *models.Request, <-chan error) {
	reader := support.EnsureBufferedReader(conn)
	requestChan := make(chan *models.Request)
	errChan := make(chan error)

	remoteAddr := conn.RemoteAddr().String()
	readRequestChan := createReadRequestChan(reader)

	go func() {
		defer close(requestChan)
		defer close(errChan)

		requestCtx, cancelCtx := context.WithTimeout(ctx, config.ConnIdleTimeout)
		defer cancelCtx()

		for {
			select {
			case <-requestCtx.Done():
				return
			case result := <-readRequestChan:
				err := result.err
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
						log.Debug().Err(err).Msgf(
							"No more data to read, connection %v is already closed. This error can be safely ignored.",
							remoteAddr,
						)
					} else {
						log.Error().Err(err).Msg("Error reading request")
						errChan <- err
					}

					return
				}
				result.request.RemoteAddr = remoteAddr
				requestChan <- result.request
			}
		}
	}()

	return requestChan, errChan
}

// read requests from reader until the reader is closed or throws an error
func createReadRequestChan(reader io.Reader) <-chan struct {
	request *models.Request
	err     error
} {
	readRequestChan := make(chan struct {
		request *models.Request
		err     error
	})

	go func() {
		defer close(readRequestChan)

		for {
			// This blocking call will return
			// when a valid request is read or the [reader] is closed.
			// Closing reader is managed outside this goroutine.
			var request *models.Request
			var err error
			request, err = encoders.ReadRequest(reader)

			readRequestChan <- struct {
				request *models.Request
				err     error
			}{request, err}

			if err != nil {
				return
			}
		}
	}()

	return readRequestChan
}

func createResponseChan(ctx context.Context, requestChan <-chan *models.Request) <-chan []byte {
	responseChan := make(chan []byte)

	go func() {
		defer close(responseChan)

		var wg sync.WaitGroup
		for request := range requestChan {
			select {
			case <-ctx.Done():
				return
			default:
				wg.Add(1)

				go func(req *models.Request) {
					defer wg.Done()

					if response, err := handleRequest(req); err != nil {
						log.Error().Err(err).Msg("Error processing request")
					} else {
						responseChan <- response
					}
				}(request)
			}
		}
		wg.Wait()
	}()

	return responseChan
}

func sendResponses(ctx context.Context, w io.Writer, responseChan <-chan []byte) error {
	bf := support.EnsureBufferedWriter(w)
	for bytes := range responseChan {
		select {
		case <-ctx.Done():
			return nil
		default:
			if _, err := bf.Write(bytes); err != nil {
				log.Error().Err(err).Msg("Error writing response")
				return err
			}
			if err := bf.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}
