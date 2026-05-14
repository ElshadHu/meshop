package mesh

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/flynn/noise"
)

const (
	defaultInboxSize       = 64
	defaultDedupSize       = 1024
	handshakeRelayTimeout  = 10 * time.Second
	pendingResponderTTL    = 20 * time.Second
	tcpKeepAlivePeriod     = 30 * time.Second
	dialTimeout            = 10 * time.Second
	noiseHandshakeMsg1Type = "noise/xx/1"
	noiseHandshakeMsg2Type = "noise/xx/2"
	noiseHandshakeMsg3Type = "noise/xx/3"
)

// ErrRouterClosed is returned by Recv after Close is called
var ErrRouterClosed = errors.New("mesh: router closed")

type pendingHandshake struct {
	remoteID  PeerID
	initiator bool
	hs        *noise.HandshakeState
	done      chan handshakeResult
}

type handshakeResult struct {
	session *Session
	err     error
}

// Router is the top-level mesh peer. It owns one identity, many Links (direct neighbors)
type Router struct {
	key      StaticKey
	localID  PeerID
	links    map[PeerID]*Link
	sessions map[PeerID]*Session
	pending  map[PeerID]*pendingHandshake
	dedup    *dedupCache
	inbox    chan Envelope
	mu       sync.Mutex
	logger   *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewRouter(key StaticKey, logger *slog.Logger) *Router {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Router{
		key:      key,
		localID:  key.PeerID(),
		links:    make(map[PeerID]*Link),
		sessions: make(map[PeerID]*Session),
		pending:  make(map[PeerID]*pendingHandshake),
		dedup:    newDedupCache(defaultDedupSize),
		inbox:    make(chan Envelope, defaultInboxSize),
		ctx:      ctx,
		cancel:   cancel,
		logger:   logger,
	}
}

// LocalID returns this router's PeerID
func (r *Router) LocalID() PeerID { return r.localID }

// Listen binds a TCP listener on addr and starts accepting neighbors
func (r *Router) Listen(addr string) (string, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("mesh: router listen %s: %w", addr, err)
	}
	r.serve(l)
	return l.Addr().String(), nil
}

func (r *Router) serve(l net.Listener) {
	r.wg.Add(1)
	go r.acceptLoop(l)
}

func (r *Router) acceptLoop(l net.Listener) {
	defer r.wg.Done()
	defer func() { _ = l.Close() }()

	go func() {
		<-r.ctx.Done()
		_ = l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go r.acceptConn(conn)
	}
}

func (r *Router) acceptConn(conn net.Conn) {
	configureTCP(conn, r.logger)
	hsCTX, cancel := context.WithTimeout(r.ctx, handshakeRelayTimeout)
	defer cancel()
	link, err := Handshake(hsCTX, conn, HandshakeConfig{
		StaticKey: r.key,
		Initiator: false,
	}, r.logger)
	if err != nil {
		r.logger.Warn("inbound handshake failed", "err", err)
		_ = conn.Close()
		return
	}
	r.addLink(link)

}

func configureTCP(c net.Conn, logger *slog.Logger) {
	t, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	if err := t.SetKeepAlive(true); err != nil {
		logger.Warn("keepalive set failed", "err", err)
	}
	if err := t.SetKeepAlivePeriod(tcpKeepAlivePeriod); err != nil {
		logger.Warn("keepalive period set failed", "err", err)
	}
}

// Connect dials addr, runs the link-level Noise handshake, and verifies the remove PeerID matches
func (r *Router) Connect(ctx context.Context, addr string, expected PeerID) error {
	if expected == "" {
		return fmt.Errorf("mesh: router connect: expected PeerID required")
	}
	d := &net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("mesh: router dial %s:%w", addr, err)
	}
	configureTCP(conn, r.logger)
	link, err := Handshake(ctx, conn, HandshakeConfig{
		StaticKey:      r.key,
		ExpectedPeerID: expected,
		Initiator:      true,
	}, r.logger)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("mesh: router connect %s:%w", addr, err)
	}
	r.addLink(link)
	return nil
}

func (r *Router) addLink(link *Link) {
	r.mu.Lock()
	r.links[link.remoteID] = link
	r.sessions[link.remoteID] = link.Session()
	r.mu.Unlock()
	r.logger.Info("link up", "remote", link.remoteID)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := link.Run(r.ctx, r.onEnvelope)
		r.removeLink(link)
		if err != nil && r.ctx.Err() == nil {
			r.logger.Warn("link read loop ended", "remote", link.remoteID, "err", err)
		}
	}()
}

func (r *Router) removeLink(link *Link) {
	r.mu.Lock()
	if r.links[link.remoteID] == link {
		delete(r.links, link.remoteID)
	}
	r.mu.Unlock()
	_ = link.Close()
}

func (r *Router) onEnvelope(origin PeerID, env Envelope) {
	if r.dedup.seenBefore(env.ID) {
		return
	}
	r.dedup.mark(env.ID)
	if env.To == r.localID {
		r.deliverLocal(env)
		return
	}
	if env.TTL == 0 {
		return
	}
	env.TTL -= 1
	r.forwardToAllExcept(origin, env)
}

func (r *Router) deliverLocal(env Envelope) {
	if strings.HasPrefix(env.Type, "noise/xx/") {
		r.handleHandshakeStep(env)
		return
	}
	r.mu.Lock()
	sess := r.sessions[env.From]
	r.mu.Unlock()
	if sess == nil {
		r.logger.Warn("no session for sender", "from", env.From)
		return
	}
	plain, err := sess.Decrypt(env)
	if err != nil {
		r.logger.Warn("decrypt failed", "from", env.From, "err", err)
		return
	}

	select {
	case r.inbox <- plain:
	case <-r.ctx.Done():
	}
}

func (r *Router) forwardToAllExcept(origin PeerID, env Envelope) {
	r.mu.Lock()
	targets := make([]*Link, 0, len(r.links))
	for id, link := range r.links {
		if id == origin {
			continue
		}
		targets = append(targets, link)
	}
	r.mu.Unlock()
	for _, link := range targets {
		if err := link.SendEnvelope(r.ctx, env); err != nil {
			r.logger.Warn("forward failed", "to", link.remoteID, "err", err)
		}
	}
}

// Send delivers payload to to. If no Session with to exists, Send runs the on-demand handshake-over-relay first
func (r *Router) Send(ctx context.Context, to PeerID, msgType string, payload []byte) error {
	r.mu.Lock()
	sess := r.sessions[to]
	r.mu.Unlock()
	if sess == nil {
		var err error
		sess, err = r.startHandshakeOverRelay(ctx, to)
		if err != nil {
			return err
		}
	}

	env, err := NewEnvelope(r.localID, to, msgType, payload)
	if err != nil {
		return err
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	enc, err := sess.encryptLocked(env)
	if err != nil {
		return err
	}
	r.dedup.mark(enc.ID)
	r.forwardToAllExcept("", enc)
	return nil
}

func (r *Router) startHandshakeOverRelay(ctx context.Context, to PeerID) (*Session, error) {
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   defaultCipherSuite(),
		Random:        rand.Reader,
		Pattern:       noise.HandshakeXX,
		Initiator:     true,
		Prologue:      prologue,
		StaticKeypair: r.key.dh,
	})
	if err != nil {
		return nil, fmt.Errorf("mesh: relay handshake init: %w", err)
	}
	msg1, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("mesh: relay handshake msg1: %w", err)
	}
	pending := &pendingHandshake{
		remoteID:  to,
		initiator: true,
		hs:        hs,
		done:      make(chan handshakeResult, 1),
	}
	r.mu.Lock()
	if r.pending[to] != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("mesh: handshake already in progress with %s", to)
	}
	r.pending[to] = pending
	r.mu.Unlock()

	env, err := NewEnvelope(r.localID, to, noiseHandshakeMsg1Type, msg1)
	if err != nil {
		r.deletePending(to, pending)
		return nil, err
	}
	r.dedup.mark(env.ID)
	r.forwardToAllExcept("", env)

	hsCtx, cancel := context.WithTimeout(ctx, handshakeRelayTimeout)
	defer cancel()
	select {
	case res := <-pending.done:
		r.deletePending(to, pending)
		if res.err != nil {
			return nil, res.err
		}
		return res.session, nil
	case <-hsCtx.Done():
		r.deletePending(to, pending)
		return nil, fmt.Errorf("mesh: relay handshake timeout with %s", to)
	case <-r.ctx.Done():
		r.deletePending(to, pending)
		return nil, ErrRouterClosed
	}
}

func (r *Router) deletePending(to PeerID, pending *pendingHandshake) {
	r.mu.Lock()
	if r.pending[to] == pending {
		delete(r.pending, to)
	}
	r.mu.Unlock()
}

// Close shuts down the router. Closes all links and stops the accept
func (r *Router) Close() error {
	r.cancel()
	r.mu.Lock()
	links := make([]*Link, 0, len(r.links))
	for _, l := range r.links {
		links = append(links, l)
	}
	r.links = make(map[PeerID]*Link)
	r.mu.Unlock()
	for _, l := range links {
		_ = l.Close()
	}
	r.wg.Wait()
	return nil
}

func (r *Router) handleHandshakeStep(env Envelope) {
	switch env.Type {
	case noiseHandshakeMsg1Type:
		r.handleHandshakeMsg1(env)
	case noiseHandshakeMsg2Type:
		r.handleHandshakeMsg2(env)
	case noiseHandshakeMsg3Type:
		r.handleHandshakeMsg3(env)
	default:
		r.logger.Warn("unknown handshake type", "type", env.Type)
	}
}

func (r *Router) handleHandshakeMsg1(env Envelope) {
	r.mu.Lock()
	if r.sessions[env.From] != nil {
		r.mu.Unlock()
		r.logger.Debug("already have session, ignoring msg1", "from", env.From)
		return
	}
	if r.pending[env.From] != nil {
		r.mu.Unlock()
		r.logger.Warn("handshake collision, ignoring msg1", "from", env.From)
		return
	}
	r.mu.Unlock()
	hs, err := noise.NewHandshakeState(noise.Config{
		CipherSuite:   defaultCipherSuite(),
		Random:        rand.Reader,
		Pattern:       noise.HandshakeXX,
		Initiator:     false,
		Prologue:      prologue,
		StaticKeypair: r.key.dh,
	})

	if err != nil {
		r.logger.Warn("responder init failed", "err", err)
		return
	}
	if _, _, _, err := hs.ReadMessage(nil, env.Payload); err != nil {
		r.logger.Warn("msg1 parse failed", "err", err)
		return
	}
	msg2, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		r.logger.Warn("msg2 build failed", "err", err)
		return
	}
	pending := &pendingHandshake{
		remoteID:  env.From,
		initiator: false,
		hs:        hs,
		done:      make(chan handshakeResult, 1),
	}
	r.mu.Lock()
	r.pending[env.From] = pending
	r.mu.Unlock()

	// Clean up if msg3 never arrives
	time.AfterFunc(pendingResponderTTL, func() { r.deletePending(env.From, pending) })
	out, err := NewEnvelope(r.localID, env.From, noiseHandshakeMsg2Type, msg2)
	if err != nil {
		r.logger.Warn("msg2 envelope failed", "err", err)
		return
	}
	r.dedup.mark(out.ID)
	r.forwardToAllExcept("", out)
}

func (r *Router) handleHandshakeMsg2(env Envelope) {
	r.mu.Lock()
	pending := r.pending[env.From]
	r.mu.Unlock()
	if pending == nil || !pending.initiator {
		r.logger.Warn("msg2 with no pending initiator", "from", env.From)
		return
	}
	if _, _, _, err := pending.hs.ReadMessage(nil, env.Payload); err != nil {
		pending.done <- handshakeResult{err: fmt.Errorf("mesh: msg2 parse: %w", err)}
		return
	}
	msg3, cs1, cs2, err := pending.hs.WriteMessage(nil, nil)
	if err != nil {
		pending.done <- handshakeResult{err: fmt.Errorf("mesh: msg3 build: %w", err)}
		return
	}
	if cs1 == nil || cs2 == nil {
		pending.done <- handshakeResult{err: fmt.Errorf("mesh: msg3 missing cipher states")}
		return
	}
	if peerIDFromPublicKey(pending.hs.PeerStatic()) != env.From {
		pending.done <- handshakeResult{err: fmt.Errorf("mesh: msg2 PeerID mismatch")}
		return
	}
	out, err := NewEnvelope(r.localID, env.From, noiseHandshakeMsg3Type, msg3)
	if err != nil {
		pending.done <- handshakeResult{err: err}
		return
	}
	r.dedup.mark(out.ID)
	r.forwardToAllExcept("", out)

	session := &Session{sendCS: cs1, recvCS: cs2, remoteID: env.From}
	r.mu.Lock()
	r.sessions[env.From] = session
	r.mu.Unlock()
	pending.done <- handshakeResult{session: session}
}

func (r *Router) handleHandshakeMsg3(env Envelope) {
	r.mu.Lock()
	pending := r.pending[env.From]
	r.mu.Unlock()
	if pending == nil || pending.initiator {
		r.logger.Warn("msg3 with no pending responder", "from", env.From)
		return
	}
	_, cs1, cs2, err := pending.hs.ReadMessage(nil, env.Payload)
	if err != nil {
		r.logger.Warn("msg3 parse failed", "err", err)
		return
	}
	if cs1 == nil || cs2 == nil {
		r.logger.Warn("msg3 missing cipher states")
		return
	}
	if peerIDFromPublicKey(pending.hs.PeerStatic()) != env.From {
		r.logger.Warn("msg3 PeerID mismatch", "from", env.From)
		return
	}
	session := &Session{sendCS: cs2, recvCS: cs1, remoteID: env.From}
	r.mu.Lock()
	r.sessions[env.From] = session
	delete(r.pending, env.From)
	r.mu.Unlock()
}

func (r *Router) Recv(ctx context.Context) (Envelope, error) {
	select {
	case env := <-r.inbox:
		return env, nil
	case <-ctx.Done():
		return Envelope{}, ctx.Err()
	case <-r.ctx.Done():
		return Envelope{}, ErrRouterClosed
	}
}

// attachConn is for tests that wire two Routers with net.Pipe. It
// runs the link-level handshake on conn and adds the resulting Link.
func (r *Router) attachConn(ctx context.Context, conn net.Conn, initiator bool, expected PeerID) error {
	link, err := Handshake(ctx, conn, HandshakeConfig{
		StaticKey:      r.key,
		ExpectedPeerID: expected,
		Initiator:      initiator,
	}, r.logger)
	if err != nil {
		_ = conn.Close()
		return err
	}
	r.addLink(link)
	return nil
}
