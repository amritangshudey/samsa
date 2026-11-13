package wire

// DescribeClientQuotas (48), which kafka-configs.sh --describe sends for
// users and clients. Samsa has no quotas, so the answer is always an empty
// list (docs/design/c8b-credentials-forwarding.md, section 6). Fields
// follow DescribeClientQuotas*.json (trunk, read 2026-11-14): versions 0-1,
// flexible from 1.

// QuotaComponent is one filter component of a DescribeClientQuotas request.
type QuotaComponent struct {
	EntityType string
	MatchType  int8
	Match      string // null when empty
}

// DescribeClientQuotasRequest asks for the quotas matching Components.
type DescribeClientQuotasRequest struct {
	Components []QuotaComponent
	Strict     bool
}

// Decode implements Message.
func (m *DescribeClientQuotasRequest) Decode(r *Reader, v int16) {
	flex := v >= 1
	m.Components = Array(r, flex, func() (c QuotaComponent) {
		c.EntityType = r.String(flex)
		c.MatchType = r.Int8()
		c.Match, _ = r.NullableString(flex)
		r.Tags(flex)
		return c
	})
	m.Strict = r.Bool()
	r.Tags(flex)
}

// Encode implements Message.
func (m *DescribeClientQuotasRequest) Encode(w *Writer, v int16) {
	flex := v >= 1
	w.ArrayLen(flex, len(m.Components))
	for _, c := range m.Components {
		w.String(flex, c.EntityType)
		w.Int8(c.MatchType)
		w.optString(flex, c.Match)
		w.Tags(flex)
	}
	w.Bool(m.Strict)
	w.Tags(flex)
}

// DescribeClientQuotasResponse is the body of the response. Its Entries
// array is always empty: Samsa has no quotas to describe.
type DescribeClientQuotasResponse struct {
	ErrorCode    int16
	ErrorMessage string // null when empty
}

// Decode implements Message.
func (m *DescribeClientQuotasResponse) Decode(r *Reader, v int16) {
	flex := v >= 1
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(flex)
	if n := r.ArrayLen(flex); n > 0 {
		r.fail("%d quota entries; Samsa sends none", n)
	}
	r.Tags(flex)
}

// Encode implements Message.
func (m *DescribeClientQuotasResponse) Encode(w *Writer, v int16) {
	flex := v >= 1
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(flex, m.ErrorMessage)
	w.ArrayLen(flex, 0)
	w.Tags(flex)
}
