package yaks

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/atolab/zenoh-go"
)

// Properties is a (string,string) map
type Properties map[string]string

// Listener defines the callback function that has to be registered for subscriptions
type Listener func([]Change)

// SubscriptionID identifies a Yaks subscription
type SubscriptionID = zenoh.Subscriber

// Eval defines the callback function that has to be registered for evals
type Eval func(path *Path, props Properties) Value

////////////////
//    Path    //
////////////////

// Path is a path in Yaks
type Path struct {
	path string
}

// NewPath returns a new Path from the string p, if it's a valid path specification.
// Otherwise, it returns an error.
func NewPath(p string) (*Path, error) {
	if len(p) == 0 {
		return nil, &YError{"Invalid path (empty String)", nil}
	}

	for i, c := range p {
		if c == '?' || c == '#' || c == '[' || c == ']' || c == '*' {
			return nil, &YError{"Invalid path: " + p + " (forbidden character at index " + strconv.Itoa(i) + ")", nil}
		}
	}
	result := removeUselessSlashes(p)
	return &Path{result}, nil
}

// ToString returns the Path as a string
func (p *Path) ToString() string {
	return p.path
}

// Length returns length of the path string
func (p *Path) Length() int {
	return len(p.path)
}

// IsRelative returns true if the Path is not absolute (i.e. it doesn't start with '/')
func (p *Path) IsRelative() bool {
	return p.Length() == 0 || p.path[0] != '/'
}

// AddPrefix returns a new Path made from the concatenation of the prefix and this path.
func (p *Path) AddPrefix(prefix *Path) *Path {
	result, _ := NewPath(prefix.path + p.path)
	return result
}

var slashesRegexp = regexp.MustCompile("/+")

func removeUselessSlashes(s string) string {
	result := slashesRegexp.ReplaceAllString(s, "/")
	return strings.TrimSuffix(result, "/")
}

////////////////
//  Selector  //
////////////////

// Selector is a selector in Yaks
type Selector struct {
	path         string
	predicate    string
	properties   string
	fragment     string
	optionalPart string
	toString     string
}

const (
	regexPath       string = "[^\\[\\]?#]+"
	regexPredicate  string = "[^\\[\\]\\(\\)#]+"
	regexProperties string = ".*"
	regexFragment   string = ".*"
)

var pattern = regexp.MustCompile(
	fmt.Sprintf("(%s)(\\?(%s)?(\\((%s)\\))?)?(#(%s))?", regexPath, regexPredicate, regexProperties, regexFragment))

// NewSelector returns a new Selector from the string s, if it's a valid path specification.
// Otherwise, it returns an error.
func NewSelector(s string) (*Selector, error) {
	if len(s) == 0 {
		return nil, &YError{"Invalid selector (empty String)", nil}
	}

	if !pattern.MatchString(s) {
		return nil, &YError{"Invalid selector (not matching regex)", nil}
	}

	groups := pattern.FindStringSubmatch(s)
	path := groups[1]
	predicate := groups[3]
	properties := groups[5]
	fragment := groups[7]

	return newSelector(path, predicate, properties, fragment), nil
}

func newSelector(path string, predicate string, properties string, fragment string) *Selector {
	propertiesPart := ""
	if len(properties) > 0 {
		propertiesPart = "(" + properties + ")"
	}
	fragmentPart := ""
	if len(fragment) > 0 {
		fragmentPart = "#" + fragment
	}
	optionalPart := fmt.Sprintf("%s%s%s", predicate, propertiesPart, fragmentPart)
	toString := path
	if len(optionalPart) > 0 {
		toString += "?" + optionalPart
	}

	return &Selector{path, predicate, properties, fragment, optionalPart, toString}
}

// Path returns the path part of the Selector
func (s *Selector) Path() string {
	return s.path
}

// Predicate returns the predicate part of the Selector
func (s *Selector) Predicate() string {
	return s.predicate
}

// Properties returns the properties part of the Selector
func (s *Selector) Properties() string {
	return s.properties
}

// Fragment returns the fragment part of the Selector
func (s *Selector) Fragment() string {
	return s.fragment
}

// OptionalPart returns the optional part of the Selector
// (i.e. the part starting from the '?' character to the end of string)
func (s *Selector) OptionalPart() string {
	return s.optionalPart
}

// ToString returns the Selector as a string
func (s *Selector) ToString() string {
	return s.toString
}

// IsRelative returns true if the Path is not absolute (i.e. it doesn't start with '/')
func (s *Selector) IsRelative() bool {
	return len(s.path) == 0 || s.path[0] != '/'
}

// AddPrefix returns a new Selector made from the concatenation of the prefix and this path.
func (s *Selector) AddPrefix(prefix *Path) *Selector {
	return newSelector(prefix.path+s.path, s.predicate, s.properties, s.fragment)
}

///////////////////
//   PathValue   //
///////////////////

// PathValue is a Patth + Value tuple
type PathValue struct {
	path  *Path
	value Value
}

// Path returns the path of the PathValue tuple
func (pv *PathValue) Path() *Path {
	return pv.path
}

// Value returns the value of the PathValue tuple
func (pv *PathValue) Value() Value {
	return pv.value
}

////////////////
//   Change   //
////////////////

// ChangeKind is a kind of change
type ChangeKind = uint8

const (
	// PUT represents a change made by a put on Yaks
	PUT ChangeKind = 0x00
	// UPDATE represents a change made by an update on Yaks
	UPDATE ChangeKind = 0x01
	// REMOVE represents a change made by a remove on Yaks
	REMOVE ChangeKind = 0x02
)

// Change represents a change made on a path/value in Yaks
type Change struct {
	path  *Path
	kind  ChangeKind
	time  uint64
	value Value
}

// Path returns the path impacted by the change
func (c *Change) Path() *Path {
	return c.path
}

// Kind returns the kind of change
func (c *Change) Kind() ChangeKind {
	return c.kind
}

// Time returns the time of change (as registered in Yaks)
func (c *Change) Time() uint64 {
	return c.time
}

// Value returns the value that changed
func (c *Change) Value() Value {
	return c.value
}

////////////////
//  Encoding  //
////////////////

// Encoding is the encoding kind of a Value
type Encoding = uint8

// Known encodings
const (
	RAW        Encoding = 0x00
	STRING     Encoding = 0x02
	PROPERTIES Encoding = 0x03
	JSON       Encoding = 0x04
	SQL        Encoding = 0x05
)

var valueDecoders = map[Encoding]ValueDecoder{}

// RegisterValueDecoder registers a ValueDecoder function with it's Encoding
func RegisterValueDecoder(encoding Encoding, decoder ValueDecoder) error {
	if valueDecoders[encoding] != nil {
		return &YError{"Already registered ValueDecoder for Encoding " + strconv.Itoa(int(encoding)), nil}
	}
	valueDecoders[encoding] = decoder
	return nil
}

func init() {
	RegisterValueDecoder(RAW, rawDecoder)
	RegisterValueDecoder(STRING, stringDecoder)
	RegisterValueDecoder(PROPERTIES, propertiesDecoder)
}

////////////////
//   Value    //
////////////////

// Value represents a value stored by Yaks
type Value interface {
	Encoding() Encoding
	Encode() []byte
	ToString() string
}

// ValueDecoder is a decoder for a Value
type ValueDecoder func([]byte) (Value, error)

///////////////////
//   RAW Value   //
///////////////////

// RawValue is a RAW value (i.e. a bytes buffer)
type RawValue struct {
	buf []byte
}

// NewRawValue returns a new RawValue
func NewRawValue(buf []byte) *RawValue {
	return &RawValue{buf}
}

// Encoding returns the encoding flag for a RawValue
func (v *RawValue) Encoding() Encoding {
	return RAW
}

// Encode returns the value encoded as a []byte
func (v *RawValue) Encode() []byte {
	return v.buf
}

// ToString returns the value as a string
func (v *RawValue) ToString() string {
	return fmt.Sprintf("[x %d]", v.buf)
}

func rawDecoder(buf []byte) (Value, error) {
	return &RawValue{buf}, nil
}

//////////////////////
//   STRING Value   //
//////////////////////

// StringValue is a STRING value (i.e. just a string)
type StringValue struct {
	s string
}

// NewStringValue returns a new StringValue
func NewStringValue(s string) *StringValue {
	return &StringValue{s}
}

// Encoding returns the encoding flag for a StringValue
func (v *StringValue) Encoding() Encoding {
	return STRING
}

// Encode returns the value encoded as a []byte
func (v *StringValue) Encode() []byte {
	return []byte(v.s)
}

// ToString returns the value as a string
func (v *StringValue) ToString() string {
	return v.s
}

func stringDecoder(buf []byte) (Value, error) {
	return &StringValue{string(buf)}, nil
}

//////////////////////////
//   PROPERTIES Value   //
//////////////////////////

// PropertiesValue is a PROPERTIES value (i.e. a map[string]string)
type PropertiesValue struct {
	p Properties
}

// NewPropertiesValue returns a new PropertiesValue
func NewPropertiesValue(p Properties) *PropertiesValue {
	return &PropertiesValue{p}
}

// Encoding returns the encoding flag for a PropertiesValue
func (v *PropertiesValue) Encoding() Encoding {
	return PROPERTIES
}

// Encode returns the value encoded as a []byte
func (v *PropertiesValue) Encode() []byte {
	return []byte(v.ToString())
}

const (
	propSep = ";"
	kvSep   = "="
)

// ToString returns the value as a string
func (v *PropertiesValue) ToString() string {
	builder := new(strings.Builder)
	i := 0
	for key, val := range v.p {
		builder.WriteString(key)
		builder.WriteString(kvSep)
		builder.WriteString(val)
		i++
		if i < len(v.p) {
			builder.WriteString(propSep)
		}
	}
	return builder.String()
}

func propertiesOfString(s string) Properties {
	p := make(Properties)
	if len(s) > 0 {
		for _, kv := range strings.Split(s, propSep) {
			i := strings.Index(kv, kvSep)
			if i < 0 {
				p[kv] = ""
			} else {
				p[kv[:i]] = kv[i+1:]
			}
		}
	}
	return p
}

func propertiesDecoder(buf []byte) (Value, error) {
	return &PropertiesValue{propertiesOfString(string(buf))}, nil
}
