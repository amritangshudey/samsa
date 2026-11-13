package wire

// SASL authentication (ADR 0019). Fields follow SaslHandshakeRequest.json,
// SaslHandshakeResponse.json, SaslAuthenticateRequest.json and
// SaslAuthenticateResponse.json in the Apache Kafka repository (trunk,
// read 2026-11-14).

// --- SaslHandshake (17), versions 1 (0 is never advertised) -------------

// SaslHandshakeRequest is the body of a SaslHandshake request.
type SaslHandshakeRequest struct{ Mechanism string }

// Decode implements Message.
func (m *SaslHandshakeRequest) Decode(r *Reader, _ int16) { m.Mechanism = r.String(false) }

// Encode implements Message.
func (m *SaslHandshakeRequest) Encode(w *Writer, _ int16) { w.String(false, m.Mechanism) }

// SaslHandshakeResponse is the body of a SaslHandshake response.
type SaslHandshakeResponse struct {
	ErrorCode  int16
	Mechanisms []string
}

// Decode implements Message.
func (m *SaslHandshakeResponse) Decode(r *Reader, _ int16) {
	m.ErrorCode = r.Int16()
	n := r.ArrayLen(false)
	for i := 0; i < n && r.Err() == nil; i++ {
		m.Mechanisms = append(m.Mechanisms, r.String(false))
	}
}

// Encode implements Message.
func (m *SaslHandshakeResponse) Encode(w *Writer, _ int16) {
	w.Int16(m.ErrorCode)
	w.ArrayLen(false, len(m.Mechanisms))
	for _, s := range m.Mechanisms {
		w.String(false, s)
	}
}

// --- SaslAuthenticate (36), versions 0 to 2 -------------------------------

// SaslAuthenticateRequest is the body of a SaslAuthenticate request.
type SaslAuthenticateRequest struct{ AuthBytes []byte }

// Decode implements Message.
func (m *SaslAuthenticateRequest) Decode(r *Reader, v int16) {
	flex := v >= 2
	m.AuthBytes = nonNull(r.Bytes(flex)) // not nullable: a null on the wire is read as empty
	r.Tags(flex)
}

// Encode implements Message.
func (m *SaslAuthenticateRequest) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.Bytes(flex, nonNull(m.AuthBytes))
	w.Tags(flex)
}

// SaslAuthenticateResponse is the body of a SaslAuthenticate response.
// SessionLifetimeMs (v1+) is always 0: no re-authentication is required.
type SaslAuthenticateResponse struct {
	ErrorCode    int16
	ErrorMessage string // null when empty
	AuthBytes    []byte
}

// Decode implements Message.
func (m *SaslAuthenticateResponse) Decode(r *Reader, v int16) {
	flex := v >= 2
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(flex)
	m.AuthBytes = nonNull(r.Bytes(flex)) // not nullable: a null on the wire is read as empty
	if v >= 1 {
		r.Int64() // SessionLifetimeMs
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *SaslAuthenticateResponse) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.Int16(m.ErrorCode)
	w.optString(flex, m.ErrorMessage)
	w.Bytes(flex, nonNull(m.AuthBytes))
	if v >= 1 {
		w.Int64(0)
	}
	w.Tags(flex)
}

// nonNull is b, or an empty slice for nil: AuthBytes is not nullable, and
// Writer.Bytes writes nil as null, which the Java client refuses
// ("non-nullable field authBytes was serialized as null").
func nonNull(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
