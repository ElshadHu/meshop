package mesh

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// MaxFrameBytes caps a single wire frame at 1 MiB
const MaxFrameBytes = 1 << 20

// ErrFrameTooLarge is returned when a wire frame exceeds MaxFrameBytes
var ErrFrameTooLarge = errors.New("mesh: frame exceeds MaxFrameBytes")

// lengthPrefixBytes is the size of the wire-framing length prefix
const lengthPrefixBytes = 4

// Link is one TCP connection to one direct neighbor
type Link struct {
	localID  PeerID
	remoteID PeerID
	conn     net.Conn
	session  *Session
	sendMu   sync.Mutex
	logger   *slog.Logger
}

// NewLink builds a Link. session may be nil during the handshake
func NewLink(localID, remoteID PeerID, conn net.Conn, session *Session, logger *slog.Logger) *Link {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Link{
		localID:  localID,
		remoteID: remoteID,
		conn:     conn,
		session:  session,
		logger:   logger,
	}
}

// LocalID returns this peer's PeerID
func (l *Link) LocalID() PeerID { return l.localID }

// RemoteID returns the neighbor's PeerID
func (l *Link) RemoteID() PeerID { return l.remoteID }

// Session returns the per-link Noise session
func (l *Link) Session() *Session { return l.session }

// SendEnvelope marshals env as JSON and writes one length-prefixed
// frame. Safe for concurrent callers
func (l *Link) SendEnvelope(ctx context.Context, env Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("mesh: marshal envelope: %w", err)
	}
	l.sendMu.Lock()
	defer l.sendMu.Unlock()
	return l.writeFrameLocked(ctx, body)
}

// ReceiveEnvelope reads one envelope from the wire. NOT safe for
// concurrent callers. Used by tests that drive the link directly.
func (l *Link) ReceiveEnvelope(ctx context.Context) (Envelope, error) {
	body, err := l.readFrame(ctx)
	if err != nil {
		return Envelope{}, err
	}
	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Envelope{}, fmt.Errorf("mesh: decode envelope: %w", err)
	}
	return env, nil
}

// Run is the background read loop. It reads envelopes from the wire
// and calls onEnv for each one. Returns when ctx is cancelled, the
// conn closes (io.EOF), or any other read error
func (l *Link) Run(ctx context.Context, onEnv func(remoteID PeerID, env Envelope)) error {
	for {
		env, err := l.ReceiveEnvelope(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		onEnv(l.remoteID, env)
	}
}

// Close closes the underlying connection
func (l *Link) Close() error {
	if err := l.conn.Close(); err != nil {
		return fmt.Errorf("mesh: close: %w", err)
	}
	return nil
}

// writeFrame writes a single length-prefixed frame
func (l *Link) writeFrame(ctx context.Context, frame []byte) error {
	l.sendMu.Lock()
	defer l.sendMu.Unlock()
	return l.writeFrameLocked(ctx, frame)
}

// writeFrameLocked writes body as a length-prefixed frame
func (l *Link) writeFrameLocked(ctx context.Context, body []byte) error {
	if len(body) > MaxFrameBytes {
		return fmt.Errorf("mesh: write frame: %w (size %d)", ErrFrameTooLarge, len(body))
	}
	var header [lengthPrefixBytes]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	_ = l.conn.SetWriteDeadline(time.Time{})
	stop := l.bindWriteCancel(ctx)
	defer stop()
	if _, err := l.conn.Write(header[:]); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("mesh: write frame: %w", ctxErr)
		}
		return fmt.Errorf("mesh: write frame length: %w", err)
	}
	if _, err := l.conn.Write(body); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("mesh: write frame: %w", ctxErr)
		}
		return fmt.Errorf("mesh: write frame body: %w", err)
	}
	return nil
}

// readFrame reads one length-prefixed frame and returns its body
func (l *Link) readFrame(ctx context.Context) ([]byte, error) {
	_ = l.conn.SetReadDeadline(time.Time{})
	stop := l.bindReadCancel(ctx)
	defer stop()

	var header [lengthPrefixBytes]byte
	if _, err := io.ReadFull(l.conn, header[:]); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("mesh: readFrame: %w", ctxErr)
		}
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("mesh: read frame length: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:])
	if length > MaxFrameBytes {
		return nil, fmt.Errorf("mesh: readFrame: %w (size %d)", ErrFrameTooLarge, length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(l.conn, body); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("mesh: readFrame: %w", ctxErr)
		}
		return nil, fmt.Errorf("mesh: read frame body: %w", err)
	}
	return body, nil
}

// bindReadCancel spawns a watch goroutine that sets the conn's read
// deadline to time.Now() when ctx is cancelled
func (l *Link) bindReadCancel(ctx context.Context) func() {
	if ctx == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = l.conn.SetReadDeadline(time.Now())
		case <-done:
		}
	}()
	return func() { close(done) }
}

// bindWriteCancel is the write-side of bindReadCancel
func (l *Link) bindWriteCancel(ctx context.Context) func() {
	if ctx == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = l.conn.SetWriteDeadline(time.Now())
		case <-done:
		}
	}()
	return func() { close(done) }
}
