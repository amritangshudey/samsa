package wire

// The ACL APIs (docs/design/c8-acls.md). Fields follow CreateAcls*,
// DeleteAcls* and DescribeAcls*.json in the Apache Kafka repository (trunk,
// read 2026-11-14). All three accept versions 1 to 3, flexible from 2.
// Version 3 only adds the USER resource type, which changes no field.

// ACL is one access control entry on a resource pattern.
type ACL struct {
	ResourceType int8
	ResourceName string
	PatternType  int8
	Principal    string
	Host         string
	Operation    int8
	Permission   int8
}

func (a *ACL) decode(r *Reader, flex bool) {
	a.ResourceType = r.Int8()
	a.ResourceName = r.String(flex)
	a.PatternType = r.Int8()
	a.Principal = r.String(flex)
	a.Host = r.String(flex)
	a.Operation = r.Int8()
	a.Permission = r.Int8()
}

func (a *ACL) encode(w *Writer, flex bool) {
	w.Int8(a.ResourceType)
	w.String(flex, a.ResourceName)
	w.Int8(a.PatternType)
	w.String(flex, a.Principal)
	w.String(flex, a.Host)
	w.Int8(a.Operation)
	w.Int8(a.Permission)
}

// ACLFilter selects ACLs. A nil name, principal or host matches any.
type ACLFilter struct {
	ResourceType int8
	ResourceName *string
	PatternType  int8
	Principal    *string
	Host         *string
	Operation    int8
	Permission   int8
}

func readNullable(r *Reader, flex bool) *string {
	s, null := r.NullableString(flex)
	if null {
		return nil
	}
	return &s
}

func writeNullable(w *Writer, flex bool, s *string) {
	if s == nil {
		w.NullString(flex)
	} else {
		w.String(flex, *s)
	}
}

func (f *ACLFilter) decode(r *Reader, flex bool) {
	f.ResourceType = r.Int8()
	f.ResourceName = readNullable(r, flex)
	f.PatternType = r.Int8()
	f.Principal = readNullable(r, flex)
	f.Host = readNullable(r, flex)
	f.Operation = r.Int8()
	f.Permission = r.Int8()
}

func (f *ACLFilter) encode(w *Writer, flex bool) {
	w.Int8(f.ResourceType)
	writeNullable(w, flex, f.ResourceName)
	w.Int8(f.PatternType)
	writeNullable(w, flex, f.Principal)
	writeNullable(w, flex, f.Host)
	w.Int8(f.Operation)
	w.Int8(f.Permission)
}

// --- CreateAcls (30) -------------------------------------------------------

// CreateAclsRequest is the body of a CreateAcls request.
type CreateAclsRequest struct{ Creations []ACL }

// Decode implements Message.
func (m *CreateAclsRequest) Decode(r *Reader, v int16) {
	flex := v >= 2
	m.Creations = Array(r, flex, func() (a ACL) {
		a.decode(r, flex)
		r.Tags(flex)
		return a
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *CreateAclsRequest) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.ArrayLen(flex, len(m.Creations))
	for i := range m.Creations {
		m.Creations[i].encode(w, flex)
		w.Tags(flex)
	}
	w.Tags(flex)
}

// ACLResult is the outcome of one creation. ErrorMessage is null when
// empty.
type ACLResult struct {
	ErrorCode    int16
	ErrorMessage string
}

// CreateAclsResponse is the body of a CreateAcls response, one result per
// creation, in order.
type CreateAclsResponse struct{ Results []ACLResult }

// Decode implements Message.
func (m *CreateAclsResponse) Decode(r *Reader, v int16) {
	flex := v >= 2
	r.Int32() // ThrottleTimeMs
	m.Results = Array(r, flex, func() (a ACLResult) {
		a.ErrorCode = r.Int16()
		a.ErrorMessage, _ = r.NullableString(flex)
		r.Tags(flex)
		return a
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *CreateAclsResponse) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.Int32(0)
	w.ArrayLen(flex, len(m.Results))
	for _, a := range m.Results {
		w.Int16(a.ErrorCode)
		w.optString(flex, a.ErrorMessage)
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- DeleteAcls (31) -------------------------------------------------------

// DeleteAclsRequest is the body of a DeleteAcls request.
type DeleteAclsRequest struct{ Filters []ACLFilter }

// Decode implements Message.
func (m *DeleteAclsRequest) Decode(r *Reader, v int16) {
	flex := v >= 2
	m.Filters = Array(r, flex, func() (f ACLFilter) {
		f.decode(r, flex)
		r.Tags(flex)
		return f
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *DeleteAclsRequest) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.ArrayLen(flex, len(m.Filters))
	for i := range m.Filters {
		m.Filters[i].encode(w, flex)
		w.Tags(flex)
	}
	w.Tags(flex)
}

// DeleteAclsMatch is one ACL a filter matched, and whether it was deleted.
type DeleteAclsMatch struct {
	ACLResult
	ACL
}

// DeleteAclsResult is the outcome of one filter.
type DeleteAclsResult struct {
	ACLResult
	Matching []DeleteAclsMatch
}

// DeleteAclsResponse is the body of a DeleteAcls response, one result per
// filter, in order.
type DeleteAclsResponse struct{ Results []DeleteAclsResult }

// Decode implements Message.
func (m *DeleteAclsResponse) Decode(r *Reader, v int16) {
	flex := v >= 2
	r.Int32() // ThrottleTimeMs
	m.Results = Array(r, flex, func() (f DeleteAclsResult) {
		f.ErrorCode = r.Int16()
		f.ErrorMessage, _ = r.NullableString(flex)
		f.Matching = Array(r, flex, func() (a DeleteAclsMatch) {
			a.ErrorCode = r.Int16()
			a.ErrorMessage, _ = r.NullableString(flex)
			a.decode(r, flex)
			r.Tags(flex)
			return a
		})
		r.Tags(flex)
		return f
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *DeleteAclsResponse) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.Int32(0)
	w.ArrayLen(flex, len(m.Results))
	for _, f := range m.Results {
		w.Int16(f.ErrorCode)
		w.optString(flex, f.ErrorMessage)
		w.ArrayLen(flex, len(f.Matching))
		for i := range f.Matching {
			a := &f.Matching[i]
			w.Int16(a.ErrorCode)
			w.optString(flex, a.ErrorMessage)
			a.encode(w, flex)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}

// --- DescribeAcls (29) -----------------------------------------------------

// DescribeAclsRequest is the body of a DescribeAcls request: one filter.
type DescribeAclsRequest struct{ ACLFilter }

// Decode implements Message.
func (m *DescribeAclsRequest) Decode(r *Reader, v int16) {
	flex := v >= 2
	m.decode(r, flex)
	r.Tags(flex)
}

// Encode implements Message.
func (m *DescribeAclsRequest) Encode(w *Writer, v int16) {
	flex := v >= 2
	m.encode(w, flex)
	w.Tags(flex)
}

// ACLEntry is one ACL of a described resource pattern.
type ACLEntry struct {
	Principal  string
	Host       string
	Operation  int8
	Permission int8
}

// ACLResource is a resource pattern and the matching ACLs on it.
type ACLResource struct {
	ResourceType int8
	ResourceName string
	PatternType  int8
	ACLs         []ACLEntry
}

// DescribeAclsResponse is the body of a DescribeAcls response.
type DescribeAclsResponse struct {
	ACLResult
	Resources []ACLResource
}

// Decode implements Message.
func (m *DescribeAclsResponse) Decode(r *Reader, v int16) {
	flex := v >= 2
	r.Int32() // ThrottleTimeMs
	m.ErrorCode = r.Int16()
	m.ErrorMessage, _ = r.NullableString(flex)
	m.Resources = Array(r, flex, func() (res ACLResource) {
		res.ResourceType = r.Int8()
		res.ResourceName = r.String(flex)
		res.PatternType = r.Int8()
		res.ACLs = Array(r, flex, func() (e ACLEntry) {
			e.Principal = r.String(flex)
			e.Host = r.String(flex)
			e.Operation = r.Int8()
			e.Permission = r.Int8()
			r.Tags(flex)
			return e
		})
		r.Tags(flex)
		return res
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *DescribeAclsResponse) Encode(w *Writer, v int16) {
	flex := v >= 2
	w.Int32(0)
	w.Int16(m.ErrorCode)
	w.optString(flex, m.ErrorMessage)
	w.ArrayLen(flex, len(m.Resources))
	for _, res := range m.Resources {
		w.Int8(res.ResourceType)
		w.String(flex, res.ResourceName)
		w.Int8(res.PatternType)
		w.ArrayLen(flex, len(res.ACLs))
		for _, e := range res.ACLs {
			w.String(flex, e.Principal)
			w.String(flex, e.Host)
			w.Int8(e.Operation)
			w.Int8(e.Permission)
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Tags(flex)
}
