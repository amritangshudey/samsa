package wire

// --- IncrementalAlterConfigs (44) ------------------------------------------
//
// IncrementalAlterConfigsRequest.json and IncrementalAlterConfigsResponse.json
// in the Apache Kafka repository: versions 0-1, 1 is the first flexible
// version, and neither adds a field (ADR 0026).

// Config operations, in the order the schema names them.
const (
	ConfigSet      int8 = 0
	ConfigDelete   int8 = 1
	ConfigAppend   int8 = 2
	ConfigSubtract int8 = 3
)

// AlterableConfig is one change to one configuration.
type AlterableConfig struct {
	Name      string
	Operation int8
	Value     string
	Null      bool
}

// AlterConfigsResource is the changes to one resource.
type AlterConfigsResource struct {
	Type    int8
	Name    string
	Configs []AlterableConfig
}

// IncrementalAlterConfigsRequest is the body of an IncrementalAlterConfigs
// request (key 44).
type IncrementalAlterConfigsRequest struct {
	Resources    []AlterConfigsResource
	ValidateOnly bool
}

// Decode implements Message.
func (m *IncrementalAlterConfigsRequest) Decode(r *Reader, v int16) {
	flex := v >= 1
	m.Resources = Array(r, flex, func() AlterConfigsResource {
		res := AlterConfigsResource{Type: r.Int8(), Name: r.String(flex)}
		res.Configs = Array(r, flex, func() AlterableConfig {
			c := AlterableConfig{Name: r.String(flex), Operation: r.Int8()}
			c.Value, c.Null = r.NullableString(flex)
			r.Tags(flex)
			return c
		})
		r.Tags(flex)
		return res
	})
	m.ValidateOnly = r.Bool()
	r.Tags(flex)
}

// Encode implements Message.
func (m *IncrementalAlterConfigsRequest) Encode(w *Writer, v int16) {
	flex := v >= 1
	w.ArrayLen(flex, len(m.Resources))
	for _, res := range m.Resources {
		w.Int8(res.Type)
		w.String(flex, res.Name)
		w.ArrayLen(flex, len(res.Configs))
		for _, c := range res.Configs {
			w.String(flex, c.Name)
			w.Int8(c.Operation)
			if c.Null {
				w.NullString(flex)
			} else {
				w.String(flex, c.Value)
			}
			w.Tags(flex)
		}
		w.Tags(flex)
	}
	w.Bool(m.ValidateOnly)
	w.Tags(flex)
}

// AlterConfigsResult is the answer for one resource.
type AlterConfigsResult struct {
	ErrorCode int16
	// ErrorMessage is sent as null when empty.
	ErrorMessage string
	Type         int8
	Name         string
}

// IncrementalAlterConfigsResponse is the body of an IncrementalAlterConfigs
// response.
type IncrementalAlterConfigsResponse struct{ Results []AlterConfigsResult }

// Decode implements Message.
func (m *IncrementalAlterConfigsResponse) Decode(r *Reader, v int16) {
	flex := v >= 1
	r.Int32() // ThrottleTimeMs
	m.Results = Array(r, flex, func() AlterConfigsResult {
		res := AlterConfigsResult{ErrorCode: r.Int16()}
		res.ErrorMessage, _ = r.NullableString(flex)
		res.Type, res.Name = r.Int8(), r.String(flex)
		r.Tags(flex)
		return res
	})
	r.Tags(flex)
}

// Encode implements Message.
func (m *IncrementalAlterConfigsResponse) Encode(w *Writer, v int16) {
	flex := v >= 1
	w.Int32(0)
	w.ArrayLen(flex, len(m.Results))
	for _, res := range m.Results {
		w.Int16(res.ErrorCode)
		if res.ErrorMessage == "" {
			w.NullString(flex)
		} else {
			w.String(flex, res.ErrorMessage)
		}
		w.Int8(res.Type)
		w.String(flex, res.Name)
		w.Tags(flex)
	}
	w.Tags(flex)
}
