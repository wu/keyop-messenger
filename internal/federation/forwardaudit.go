package federation

import (
	"time"

	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// auditOutboundForwards logs an EventForward audit record with Direction
// "outbound" for each envelope in a batch the local instance just delivered to
// peer and that peer acked. sendDur is the batch's send→ack round-trip time,
// recorded on each record (all envelopes in the batch share it). It mirrors the
// inbound forward auditing done by PeerReceiver. Records that fail to unmarshal
// are skipped — they were already validated before being queued, so this is
// defensive only. A nil auditL is a no-op, matching the noopAuditLogger used by
// ephemeral clients and tests.
func auditOutboundForwards(auditL audit.AuditLogger, peer string, rawLines [][]byte, sendDur time.Duration) {
	if auditL == nil {
		return
	}
	durMS := float64(sendDur.Microseconds()) / 1000.0
	for _, rec := range rawLines {
		env, err := envelope.Unmarshal(rec)
		if err != nil {
			continue
		}
		_ = auditL.Log(audit.Event{
			Event:          audit.EventForward,
			MessageID:      env.ID,
			Channel:        env.Channel,
			Peer:           peer,
			Direction:      "outbound",
			SendDurationMS: durMS,
		})
	}
}
