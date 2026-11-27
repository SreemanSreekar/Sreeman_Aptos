package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const TimestampFormatMicros = "2006-01-02T15:04:05.000000Z"

// NamespaceDayClose matches RFC 4122 NameSpaceURL (6ba7b811-9dad-11d1-80b4-00c04fd430c8)
// used by PostgreSQL day_close_event_id().
var NamespaceDayClose = uuid.NameSpaceURL

// DayCloseEventID returns the deterministic UUIDv5 for (storeID, businessDay YYYY-MM-DD).
func DayCloseEventID(storeID, businessDay string) uuid.UUID {
	return uuid.NewSHA1(NamespaceDayClose, []byte(storeID+":"+businessDay))
}

// CanonicalJSON encodes v as compact, sorted-key RFC 8259 JSON without HTML escaping.
func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// PayloadHash computes the Phase 1 §3.1 SHA-256 hash over:
// event_type|store_id|terminal_id|YYYY-MM-DDTHH:MM:SS.ffffffZ|canonical_payload_json
func PayloadHash(eventType, storeID, terminalID string, occurredAt time.Time, canonicalPayloadJSON []byte) []byte {
	ts := occurredAt.UTC().Truncate(time.Microsecond).Format(TimestampFormatMicros)
	raw := fmt.Sprintf("%s|%s|%s|%s|%s", eventType, storeID, terminalID, ts, string(canonicalPayloadJSON))
	sum := sha256.Sum256([]byte(raw))
	return sum[:]
}
