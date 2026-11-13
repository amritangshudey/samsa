package wire

// SCRAM credential management (docs/design/c8b-credentials-forwarding.md).
// Fields follow DescribeUserScramCredentials*.json and
// AlterUserScramCredentials*.json (trunk, read 2026-11-14): version 0 only,
// flexible.

// --- DescribeUserScramCredentials (50) --------------------------------------

// DescribeUserScramCredentialsRequest asks for some users' credentials, or
// for all of them when AllUsers (a null array) or Users is empty.
type DescribeUserScramCredentialsRequest struct {
	AllUsers bool
	Users    []string
}

// Decode implements Message.
func (m *DescribeUserScramCredentialsRequest) Decode(r *Reader, _ int16) {
	n := r.ArrayLen(true)
	m.AllUsers = n < 0
	for i := 0; i < n && r.Err() == nil; i++ {
		m.Users = append(m.Users, r.String(true))
		r.Tags(true)
	}
	r.Tags(true)
}

// Encode implements Message.
func (m *DescribeUserScramCredentialsRequest) Encode(w *Writer, _ int16) {
	if m.AllUsers {
		w.ArrayLen(true, -1)
	} else {
		w.ArrayLen(true, len(m.Users))
		for _, u := range m.Users {
			w.String(true, u)
			w.Tags(true)
		}
	}
	w.Tags(true)
}

// CredentialInfo is one mechanism of a described user.
type CredentialInfo struct {
	Mechanism  int8
	Iterations int32
}

// UserCredentials is one described user.
type UserCredentials struct {
	User         string
	ErrorCode    int16
	ErrorMessage string // null when empty
	Infos        []CredentialInfo
}

// DescribeUserScramCredentialsResponse is the body of the response.
type DescribeUserScramCredentialsResponse struct {
	ErrorCode    int16
	ErrorMessage string // null when empty
	Results      []UserCredentials
}

// Decode implements Message.
func (m *DescribeUserScramCredentialsResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(true)
	m.Results = Array(r, true, func() (u UserCredentials) {
		u.User = r.String(true)
		u.ErrorCode = r.Int16()
		u.ErrorMessage, _ = r.NullableString(true)
		u.Infos = Array(r, true, func() CredentialInfo {
			c := CredentialInfo{Mechanism: r.Int8(), Iterations: r.Int32()}
			r.Tags(true)
			return c
		})
		r.Tags(true)
		return u
	})
	r.Tags(true)
}

// Encode implements Message.
func (m *DescribeUserScramCredentialsResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(true, m.ErrorMessage)
	w.ArrayLen(true, len(m.Results))
	for _, u := range m.Results {
		w.String(true, u.User)
		w.Int16(u.ErrorCode)
		w.optString(true, u.ErrorMessage)
		w.ArrayLen(true, len(u.Infos))
		for _, c := range u.Infos {
			w.Int8(c.Mechanism)
			w.Int32(c.Iterations)
			w.Tags(true)
		}
		w.Tags(true)
	}
	w.Tags(true)
}

// --- AlterUserScramCredentials (51) -----------------------------------------

// CredentialDeletion removes one user's credential for one mechanism.
type CredentialDeletion struct {
	Name      string
	Mechanism int8
}

// CredentialUpsertion sets one user's credential for one mechanism, from a
// salted password the client computed.
type CredentialUpsertion struct {
	Name           string
	Mechanism      int8
	Iterations     int32
	Salt           []byte
	SaltedPassword []byte
}

// AlterUserScramCredentialsRequest is the body of the request.
type AlterUserScramCredentialsRequest struct {
	Deletions  []CredentialDeletion
	Upsertions []CredentialUpsertion
}

// Decode implements Message.
func (m *AlterUserScramCredentialsRequest) Decode(r *Reader, _ int16) {
	m.Deletions = Array(r, true, func() CredentialDeletion {
		d := CredentialDeletion{Name: r.String(true), Mechanism: r.Int8()}
		r.Tags(true)
		return d
	})
	m.Upsertions = Array(r, true, func() CredentialUpsertion {
		u := CredentialUpsertion{Name: r.String(true), Mechanism: r.Int8(), Iterations: r.Int32(), Salt: r.Bytes(true), SaltedPassword: r.Bytes(true)}
		r.Tags(true)
		return u
	})
	r.Tags(true)
}

// Encode implements Message.
func (m *AlterUserScramCredentialsRequest) Encode(w *Writer, _ int16) {
	w.ArrayLen(true, len(m.Deletions))
	for _, d := range m.Deletions {
		w.String(true, d.Name)
		w.Int8(d.Mechanism)
		w.Tags(true)
	}
	w.ArrayLen(true, len(m.Upsertions))
	for _, u := range m.Upsertions {
		w.String(true, u.Name)
		w.Int8(u.Mechanism)
		w.Int32(u.Iterations)
		w.Bytes(true, nonNull(u.Salt))
		w.Bytes(true, nonNull(u.SaltedPassword))
		w.Tags(true)
	}
	w.Tags(true)
}

// AlterUserScramCredentialsResponse has one result per affected user.
type AlterUserScramCredentialsResponse struct {
	Results []UserResult
}

// UserResult is the outcome of the changes to one user.
type UserResult struct {
	User         string
	ErrorCode    int16
	ErrorMessage string // null when empty
}

// Decode implements Message.
func (m *AlterUserScramCredentialsResponse) Decode(r *Reader, _ int16) {
	r.Int32() // ThrottleTimeMs
	m.Results = Array(r, true, func() UserResult {
		u := UserResult{User: r.String(true), ErrorCode: r.Int16()}
		u.ErrorMessage, _ = r.NullableString(true)
		r.Tags(true)
		return u
	})
	r.Tags(true)
}

// Encode implements Message.
func (m *AlterUserScramCredentialsResponse) Encode(w *Writer, _ int16) {
	w.Int32(0)
	w.ArrayLen(true, len(m.Results))
	for _, u := range m.Results {
		w.String(true, u.User)
		w.Int16(u.ErrorCode)
		w.optString(true, u.ErrorMessage)
		w.Tags(true)
	}
	w.Tags(true)
}
