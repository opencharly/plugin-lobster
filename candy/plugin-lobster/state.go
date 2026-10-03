package pluginlobster

// state.go — the resume-state store: the wire token, the persisted state, the short
// approval index, and the consumed tombstone.
//
// Transcribed from upstream `src/workflows/state/store.ts` + `token.ts`. Two of these are
// WIRE contracts, not implementation detail, and are reproduced byte for byte:
//
//   - the resume TOKEN: base64url (UNPADDED, no prefix) of
//     `{"protocolVersion":1,"v":1,"kind":"workflow-file","stateKey":"…"}` — so a token
//     minted here is answerable by upstream lobster and vice versa.
//   - the state DIR: `$LOBSTER_STATE_DIR`, else `$HOME/.lobster/state`. Deliberately not
//     XDG: a state a different implementation cannot find is not shared state.
//
// The consumed tombstone is what makes a resume token non-replayable: a resumed run
// writes the tombstone BEFORE it starts executing the effect the token gate protects, so
// a crash mid-run cannot be answered by replaying the same token.

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/opencharly/sdk/kit"
	"github.com/opencharly/spec/lock"
)

// consumedMarkerKind names the tombstone file's own contract, so one engine can tell its
// marker from an unrelated file that happens to sit next to the state.
const consumedMarkerKind = "lobster.consumed-resume-state.v1"

// tokenKind is the token payload's `kind` for a workflow-file resume.
const tokenKind = "workflow-file"

// ---------------------------------------------------------------------------
// the wire token
// ---------------------------------------------------------------------------

// resumeTokenPayload is the exact token payload upstream encodes.
type resumeTokenPayload struct {
	ProtocolVersion int    `json:"protocolVersion"`
	V               int    `json:"v"`
	Kind            string `json:"kind"`
	StateKey        string `json:"stateKey"`
}

// encodeToken renders a state key as the resume token.
func encodeToken(stateKey string) string {
	b, _ := json.Marshal(resumeTokenPayload{ProtocolVersion: 1, V: 1, Kind: tokenKind, StateKey: stateKey})
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeToken parses a resume token. A malformed token is "Invalid token" — upstream's
// message, because the operator's next move is the same either way.
func decodeToken(token string) (resumeTokenPayload, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return resumeTokenPayload{}, fmt.Errorf("Invalid token")
	}
	var p resumeTokenPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return resumeTokenPayload{}, fmt.Errorf("Invalid token")
	}
	if p.Kind != tokenKind || p.StateKey == "" {
		return resumeTokenPayload{}, fmt.Errorf("Invalid token")
	}
	return p, nil
}

// ---------------------------------------------------------------------------
// the persisted state
// ---------------------------------------------------------------------------

// resumeState is upstream's WorkflowResumeState plus the decision fields the wire
// request contributes (never persisted).
type resumeState struct {
	FilePath           string                 `json:"filePath"`
	ResumeAtIndex      int64                  `json:"resumeAtIndex"`
	Steps              map[string]*stepResult `json:"steps"`
	Args               map[string]any         `json:"args"`
	ApprovalStepID     string                 `json:"approvalStepId,omitempty"`
	ApprovalIdentity   *approvalIdentity      `json:"approvalIdentity,omitempty"`
	InputStepID        string                 `json:"inputStepId,omitempty"`
	InputKind          string                 `json:"inputKind,omitempty"`
	InputSchema        map[string]any         `json:"inputSchema,omitempty"`
	InputSubject       any                    `json:"inputSubject,omitempty"`
	SupersededStateKey []string               `json:"supersededResumeStateKeys,omitempty"`
	CreatedAt          string                 `json:"createdAt"`

	// --- the decision, from the wire request (never persisted) ---
	StateKey    string         `json:"-"`
	HasApproved bool           `json:"-"`
	Approved    bool           `json:"-"`
	Cancel      bool           `json:"-"`
	HasResponse bool           `json:"-"`
	Response    map[string]any `json:"-"`
}

// approvalIdentity is upstream's WorkflowApprovalIdentity.
type approvalIdentity struct {
	InitiatedBy              string `json:"initiatedBy,omitempty"`
	RequiredApprover         string `json:"requiredApprover,omitempty"`
	RequireDifferentApprover bool   `json:"requireDifferentApprover,omitempty"`
}

// MarshalJSON emits exactly upstream's result field set: the persisted state is read back
// by THIS engine, but keeping the shape identical means a state written here and one
// written by upstream are interchangeable, which is what "wire-compatible" claims.
func (r *stepResult) MarshalJSON() ([]byte, error) {
	m := map[string]any{"id": r.ID}
	if r.Stdout != "" {
		m["stdout"] = r.Stdout
	}
	if r.HasJSON && r.JSON != nil {
		m["json"] = r.JSON
	}
	if r.HasResp {
		m["response"] = r.Response
	}
	if r.HasSubject {
		m["subject"] = r.Subject
	}
	if r.Approved != nil {
		m["approved"] = *r.Approved
	}
	if r.ApprovedBy != "" {
		m["approvedBy"] = r.ApprovedBy
	}
	if r.Skipped {
		m["skipped"] = true
	}
	if r.Error {
		m["error"] = true
		m["errorMessage"] = r.ErrorMessage
	}
	return json.Marshal(m)
}

func (r *stepResult) UnmarshalJSON(data []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	decode := func(key string, dst any) bool {
		raw, ok := m[key]
		if !ok {
			return false
		}
		return json.Unmarshal(raw, dst) == nil
	}
	_ = decode("id", &r.ID)
	if decode("stdout", &r.Stdout) {
		// a present-but-empty stdout is still present
	}
	if v, ok := m["json"]; ok {
		if err := json.Unmarshal(v, &r.JSON); err == nil {
			r.HasJSON = true
		}
	}
	if _, ok := m["response"]; ok {
		if err := json.Unmarshal(m["response"], &r.Response); err == nil {
			r.HasResp = true
		}
	}
	if _, ok := m["subject"]; ok {
		if err := json.Unmarshal(m["subject"], &r.Subject); err == nil {
			r.HasSubject = true
		}
	}
	if _, ok := m["approved"]; ok {
		var b bool
		if err := json.Unmarshal(m["approved"], &b); err == nil {
			r.Approved = &b
		}
	}
	_ = decode("approvedBy", &r.ApprovedBy)
	_ = decode("skipped", &r.Skipped)
	_ = decode("error", &r.Error)
	_ = decode("errorMessage", &r.ErrorMessage)
	return nil
}

// ---------------------------------------------------------------------------
// the store
// ---------------------------------------------------------------------------

// stateStore is the file-backed resume state under the state dir.
type stateStore struct {
	dir string
}

func newStateStore(env map[string]string) *stateStore {
	return &stateStore{dir: defaultStateDir(env)}
}

// defaultStateDir is upstream's resolution: an explicit dir, else the fixed home path.
func defaultStateDir(env map[string]string) string {
	if v := strings.TrimSpace(env["LOBSTER_STATE_DIR"]); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.TempDir()
	}
	return filepath.Join(home, ".lobster", "state")
}

// stateKeyPath is upstream's `keyToPath`: lowercase, every run of non-[a-z0-9._-] to `_`,
// runs collapsed, then the leading/trailing `_` stripped.
func (s *stateStore) stateKeyPath(key string) (string, error) {
	safe := strings.ToLower(key)
	var b strings.Builder
	prevUnderscore := false
	for _, r := range safe {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			b.WriteRune(r)
			prevUnderscore = r == '_'
			continue
		}
		if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "", fmt.Errorf("state key is empty/invalid")
	}
	return filepath.Join(s.dir, out+".json"), nil
}

// lock acquires the per-key advisory lock. Resume state is read-modify-written by a run
// and a resume that may overlap in time, so the lock is not optional.
func (s *stateStore) lock(key string) (func(), error) {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	lockPath := filepath.Join(s.dir, ".lock."+sanitizeKeySegment(key))
	release, err := lock.AcquireFileLockWithin(lockPath, true, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("lock state %s: %w", key, err)
	}
	return func() { _ = release() }, nil
}

func sanitizeKeySegment(key string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(key) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "state"
	}
	if len(out) > 120 {
		out = out[:120]
	}
	return out
}

// save writes a resume state and returns its key.
func (s *stateStore) save(ctx context.Context, st *resumeState) (string, error) {
	key := "workflow_resume_" + newUUID()
	if st.CreatedAt == "" {
		st.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	path, err := s.stateKeyPath(key)
	if err != nil {
		return "", err
	}
	unlock, err := s.lock(key)
	if err != nil {
		return "", err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	body, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal resume state: %w", err)
	}
	body = append(body, '\n')
	if err := kit.AtomicWriteFile(path, body, 0o600); err != nil {
		return "", err
	}
	return key, nil
}

// load reads a resume state by key.
func (s *stateStore) load(key string) (*resumeState, error) {
	path, err := s.stateKeyPath(key)
	if err != nil {
		return nil, err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("Workflow resume state not found")
		}
		return nil, err
	}
	var st resumeState
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, fmt.Errorf("parse resume state: %w", err)
	}
	st.StateKey = key
	return &st, nil
}

// delete removes a state and its short approval index.
func (s *stateStore) delete(ctx context.Context, key string) error {
	path, err := s.stateKeyPath(key)
	if err != nil {
		return err
	}
	unlock, lerr := s.lock(key)
	if lerr != nil {
		return lerr
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.cleanupApprovalIndex(key)
}

// deleteByToken removes the state a resume token names, ignoring an invalid token.
func (s *stateStore) deleteByToken(ctx context.Context, token string) error {
	p, err := decodeToken(token)
	if err != nil {
		return nil
	}
	return s.delete(ctx, p.StateKey)
}

// markConsumed writes the tombstone that makes a token non-replayable. It is written
// BEFORE the resumed effect runs, so a crash between the two leaves the effect gated
// rather than replayable.
func (s *stateStore) markConsumed(key string) error {
	path := s.consumedPath(key)
	body, _ := json.Marshal(map[string]string{
		"kind":       consumedMarkerKind,
		"stateKey":   key,
		"consumedAt": time.Now().UTC().Format(time.RFC3339Nano),
	})
	body = append(body, '\n')
	if err := kit.AtomicWriteFile(path, body, 0o600); err != nil {
		return err
	}
	return nil
}

// restoreConsumed removes the tombstone when a resumed run fails before committing the
// successor state, so the original token stays usable.
func (s *stateStore) restoreConsumed(key string) error {
	err := os.Remove(s.consumedPath(key))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *stateStore) consumedPath(key string) string {
	return filepath.Join(s.dir, ".consumed."+sanitizeKeySegment(key))
}

// ---------------------------------------------------------------------------
// the short approval index
// ---------------------------------------------------------------------------

// createApprovalIndex allocates a unique 8-hex approval id for a state key. It is created
// EXCLUSIVELY: an id that already maps somewhere must never be silently re-pointed, so a
// collision is retried with a fresh id rather than overwritten.
func (s *stateStore) createApprovalIndex(stateKey string) (string, error) {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return "", fmt.Errorf("create state dir: %w", err)
	}
	for attempt := 0; attempt < 16; attempt++ {
		id, err := randomApprovalID()
		if err != nil {
			return "", err
		}
		path := filepath.Join(s.dir, "approval_"+id+".json")
		body, _ := json.Marshal(map[string]string{
			"stateKey":  stateKey,
			"createdAt": time.Now().UTC().Format(time.RFC3339Nano),
		})
		body = append(body, '\n')
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return "", err
		}
		if _, werr := f.Write(body); werr != nil {
			_ = f.Close()
			return "", werr
		}
		if cerr := f.Close(); cerr != nil {
			return "", cerr
		}
		return id, nil
	}
	return "", fmt.Errorf("Could not allocate a unique approval ID")
}

// resolveApprovalID maps a short approval id back to its state key.
func (s *stateStore) resolveApprovalID(approvalID string) (string, error) {
	id := strings.TrimSpace(approvalID)
	if id == "" {
		return "", fmt.Errorf("Workflow resume state not found")
	}
	path := filepath.Join(s.dir, "approval_"+sanitizeKeySegment(id)+".json")
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("Workflow resume state not found")
		}
		return "", err
	}
	var idx struct {
		StateKey string `json:"stateKey"`
	}
	if err := json.Unmarshal(body, &idx); err != nil {
		return "", fmt.Errorf("parse approval index: %w", err)
	}
	if idx.StateKey == "" {
		return "", fmt.Errorf("Workflow resume state not found")
	}
	return idx.StateKey, nil
}

// cleanupApprovalIndex removes every approval id pointing at a state key.
func (s *stateStore) cleanupApprovalIndex(stateKey string) error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "approval_") || !strings.HasSuffix(name, ".json") {
			continue
		}
		path := filepath.Join(s.dir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var idx struct {
			StateKey string `json:"stateKey"`
		}
		if json.Unmarshal(body, &idx) != nil || idx.StateKey != stateKey {
			continue
		}
		_ = os.Remove(path)
	}
	return nil
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

// randomApprovalID is upstream's `randomBytes(4).toString("hex")` — 8 lowercase hex chars.
func randomApprovalID() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// newUUID renders a random UUIDv4 in its canonical hyphenated form.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A crypto/rand failure is not a condition to paper over: without entropy the state
		// key is guessable and the resume gate is decorative.
		panic(fmt.Sprintf("lobster: crypto/rand unavailable: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
