package redcon

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func isEmptyRESP(resp RESP) bool {
	return resp.Type == 0 && resp.Count == 0 &&
		resp.Data == nil && resp.Raw == nil
}

func expectBad(t *testing.T, payload string) {
	t.Helper()
	n, resp := ReadNextRESP([]byte(payload))
	if n > 0 || !isEmptyRESP(resp) {
		t.Fatalf("expected empty resp")
	}
}

func respVOut(a RESP) string {
	var data string
	var raw string
	if a.Data == nil {
		data = "nil"
	} else {
		data = strconv.Quote(string(a.Data))
	}
	if a.Raw == nil {
		raw = "nil"
	} else {
		raw = strconv.Quote(string(a.Raw))
	}
	return fmt.Sprintf("{Type: %d, Count: %d, Data: %s, Raw: %s}",
		a.Type, a.Count, data, raw,
	)
}

func respEquals(a, b RESP) bool {
	if a.Count != b.Count {
		return false
	}
	if a.Type != b.Type {
		return false
	}
	if (a.Data == nil && b.Data != nil) || (a.Data != nil && b.Data == nil) {
		return false
	}
	if string(a.Data) != string(b.Data) {
		return false
	}
	if (a.Raw == nil && b.Raw != nil) || (a.Raw != nil && b.Raw == nil) {
		return false
	}
	if string(a.Raw) != string(b.Raw) {
		return false
	}
	return true
}

func expectGood(t *testing.T, payload string, exp RESP) {
	t.Helper()
	n, resp := ReadNextRESP([]byte(payload))
	if n != len(payload) || isEmptyRESP(resp) {
		t.Fatalf("expected good resp")
	}
	if string(resp.Raw) != payload {
		t.Fatalf("expected '%s', got '%s'", payload, resp.Raw)
	}
	exp.Raw = []byte(payload)
	switch exp.Type {
	case Integer, String, Error:
		exp.Data = []byte(payload[1 : len(payload)-2])
	}
	if !respEquals(resp, exp) {
		t.Fatalf("expected %v, got %v", respVOut(exp), respVOut(resp))
	}
}

func TestRESP(t *testing.T) {
	expectBad(t, "")
	expectBad(t, "^hello\r\n")
	expectBad(t, "+hello\r")
	expectBad(t, "+hello\n")
	expectBad(t, ":\r\n")
	expectBad(t, ":-\r\n")
	expectBad(t, ":-abc\r\n")
	expectBad(t, ":abc\r\n")
	expectGood(t, ":-123\r\n", RESP{Type: Integer})
	expectGood(t, ":123\r\n", RESP{Type: Integer})
	expectBad(t, "+\r")
	expectBad(t, "+\n")
	expectGood(t, "+\r\n", RESP{Type: String})
	expectGood(t, "+hello world\r\n", RESP{Type: String})
	expectBad(t, "-\r")
	expectBad(t, "-\n")
	expectGood(t, "-\r\n", RESP{Type: Error})
	expectGood(t, "-hello world\r\n", RESP{Type: Error})
	expectBad(t, "$")
	expectBad(t, "$\r")
	expectBad(t, "$\r\n")
	expectGood(t, "$-1\r\n", RESP{Type: Bulk})
	expectGood(t, "$0\r\n\r\n", RESP{Type: Bulk, Data: []byte("")})
	expectBad(t, "$5\r\nhello\r")
	expectBad(t, "$5\r\nhello\n\n")
	expectGood(t, "$5\r\nhello\r\n", RESP{Type: Bulk, Data: []byte("hello")})
	expectBad(t, "*a\r\n")
	expectBad(t, "*3\r\n")
	expectBad(t, "*3\r\n:hello\r")
	expectGood(t, "*3\r\n:1\r\n:2\r\n:3\r\n",
		RESP{Type: Array, Count: 3, Data: []byte(":1\r\n:2\r\n:3\r\n")})

	var xx int
	_, r := ReadNextRESP([]byte("*4\r\n:1\r\n:2\r\n:3\r\n:4\r\n"))
	r.ForEach(func(resp RESP) bool {
		xx++
		x, _ := strconv.Atoi(string(resp.Data))
		if x != xx {
			t.Fatalf("expected %v, got %v", x, xx)
		}
		if xx == 3 {
			return false
		}
		return true
	})
	if xx != 3 {
		t.Fatalf("expected %v, got %v", 3, xx)
	}
}

func TestNextCommand(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()
	for time.Since(start) < time.Second {
		// keep copy of pipeline args for final compare
		var plargs [][][]byte

		// create a pipeline of random number of commands with random data.
		N := rand.Int() % 10000
		var data []byte
		for i := 0; i < N; i++ {
			nargs := rand.Int() % 10
			data = AppendArray(data, nargs)
			var args [][]byte
			for j := 0; j < nargs; j++ {
				arg := make([]byte, rand.Int()%100)
				if _, err := rand.Read(arg); err != nil {
					t.Fatal(err)
				}
				data = AppendBulk(data, arg)
				args = append(args, arg)
			}
			plargs = append(plargs, args)
		}

		// break data into random number of chunks
		chunkn := rand.Int() % 100
		if chunkn == 0 {
			chunkn = 1
		}
		if len(data) < chunkn {
			continue
		}
		var chunks [][]byte
		var chunksz int
		for i := 0; i < len(data); i += chunksz {
			chunksz = rand.Int() % (len(data) / chunkn)
			var chunk []byte
			if i+chunksz < len(data) {
				chunk = data[i : i+chunksz]
			} else {
				chunk = data[i:]
			}
			chunks = append(chunks, chunk)
		}

		// process chunks
		var rbuf []byte
		var fargs [][][]byte
		for _, chunk := range chunks {
			var data []byte
			if len(rbuf) > 0 {
				data = append(rbuf, chunk...)
			} else {
				data = chunk
			}
			for {
				complete, args, _, leftover, err := ReadNextCommand(data, nil)
				data = leftover
				if err != nil {
					t.Fatal(err)
				}
				if !complete {
					break
				}
				fargs = append(fargs, args)
			}
			rbuf = append(rbuf[:0], data...)
		}
		// compare final args to original
		if len(plargs) != len(fargs) {
			t.Fatalf("not equal size: %v != %v", len(plargs), len(fargs))
		}
		for i := 0; i < len(plargs); i++ {
			if len(plargs[i]) != len(fargs[i]) {
				t.Fatalf("not equal size for item %v: %v != %v", i, len(plargs[i]), len(fargs[i]))
			}
			for j := 0; j < len(plargs[i]); j++ {
				if !bytes.Equal(plargs[i][j], plargs[i][j]) {
					t.Fatalf("not equal for item %v:%v: %v != %v", i, j, len(plargs[i][j]), len(fargs[i][j]))
				}
			}
		}
	}
}

func TestAppendBulkFloat(t *testing.T) {
	var b []byte
	b = AppendString(b, "HELLO")
	b = AppendBulkFloat(b, 9.123192839)
	b = AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n9.123192839\r\n+HELLO\r\n"
	if string(b) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b)
	}
}

func TestAppendBulkInt(t *testing.T) {
	var b []byte
	b = AppendString(b, "HELLO")
	b = AppendBulkInt(b, -9182739137)
	b = AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n-9182739137\r\n+HELLO\r\n"
	if string(b) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b)
	}
}

func TestAppendBulkUint(t *testing.T) {
	var b []byte
	b = AppendString(b, "HELLO")
	b = AppendBulkInt(b, 91827391370)
	b = AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n91827391370\r\n+HELLO\r\n"
	if string(b) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b)
	}
}

func TestRESP3Parse(t *testing.T) {
	tests := []struct {
		payload string
		typ     Type
		count   int
		data    string
	}{
		{"_\r\n", Null, 0, ""},
		{",1.23\r\n", Double, 0, "1.23"},
		{"#t\r\n", Boolean, 0, "t"},
		{"#f\r\n", Boolean, 0, "f"},
		{"(3492890328409238509324850943850943825024385\r\n", BigNumber, 0,
			"3492890328409238509324850943850943825024385"},
		{"=15\r\ntxt:Some string\r\n", Verbatim, 0, "txt:Some string"},
		{"!21\r\nSYNTAX invalid syntax\r\n", BlobError, 0, "SYNTAX invalid syntax"},
		{"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n", Map, 2,
			"+first\r\n:1\r\n+second\r\n:2\r\n"},
		{"~3\r\n+orange\r\n+apple\r\n:1\r\n", Set, 3,
			"+orange\r\n+apple\r\n:1\r\n"},
		{">3\r\n+message\r\n+somechannel\r\n+hi\r\n", Push, 3,
			"+message\r\n+somechannel\r\n+hi\r\n"},
		{"|1\r\n+ttl\r\n:3600\r\n", Attribute, 1, "+ttl\r\n:3600\r\n"},
		{">2\r\n,1.5\r\n#t\r\n", Push, 2, ",1.5\r\n#t\r\n"},
	}
	for _, tt := range tests {
		n, resp := ReadNextRESP([]byte(tt.payload))
		if n != len(tt.payload) {
			t.Fatalf("payload %q: expected n=%d, got %d", tt.payload, len(tt.payload), n)
		}
		if resp.Type != tt.typ {
			t.Fatalf("payload %q: expected type %d, got %d", tt.payload, tt.typ, resp.Type)
		}
		if resp.Count != tt.count {
			t.Fatalf("payload %q: expected count %d, got %d", tt.payload, tt.count, resp.Count)
		}
		if string(resp.Data) != tt.data {
			t.Fatalf("payload %q: expected data %q, got %q", tt.payload, tt.data, resp.Data)
		}
		if string(resp.Raw) != tt.payload {
			t.Fatalf("payload %q: expected raw %q, got %q", tt.payload, tt.payload, resp.Raw)
		}
	}

	bad := []string{
		"_\r",                 // missing LF
		"_",                   // truncated
		",",                   // truncated
		"=5\r\ntxt:x",         // truncated payload
		"%2\r\n+first\r\n",   // truncated map
		"~x\r\n",              // invalid set count
		"|2\r\n+first\r\n:1\r\n", // attribute declares 2 pairs but has 1
	}
	for _, p := range bad {
		n, resp := ReadNextRESP([]byte(p))
		if n > 0 || resp.Type != 0 {
			t.Fatalf("expected bad for %q, got n=%d resp=%v", p, n, resp)
		}
	}
}

func TestRESP3Helpers(t *testing.T) {
	_, r := ReadNextRESP([]byte(",1.5\r\n"))
	if r.Double() != 1.5 {
		t.Fatalf("expected double 1.5, got %v", r.Double())
	}
	_, r = ReadNextRESP([]byte("#t\r\n"))
	if !r.Bool() {
		t.Fatalf("expected bool true")
	}
	_, r = ReadNextRESP([]byte("#f\r\n"))
	if r.Bool() {
		t.Fatalf("expected bool false")
	}
}

func TestAppendAny3(t *testing.T) {
	tests := []struct {
		name string
		v    interface{}
		exp  string
	}{
		{"Null", nil, "_\r\n"},
		{"BoolTrue", true, "#t\r\n"},
		{"BoolFalse", false, "#f\r\n"},
		{"Float", 1.5, ",1.5\r\n"},
		{"Float32", float32(1.5), ",1.5\r\n"},
		{"Int", 5, "$1\r\n5\r\n"},
		{"String", "hi", "$2\r\nhi\r\n"},
		{"Bytes", []byte("hi"), "$2\r\nhi\r\n"},
		{"SimpleString", SimpleString("OK"), "+OK\r\n"},
		{"Error", fmt.Errorf("ERR oops"), "-ERR oops\r\n"},
		{"Fallback", struct{ A int }{A: 1}, "$3\r\n{1}\r\n"},
		{"StringMap", map[string]int{"b": 2, "a": 1},
			"%2\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"},
		{"FloatMap", map[string]float64{"x": 1.5},
			"%1\r\n$1\r\nx\r\n,1.5\r\n"},
		{"FloatSlice", []float64{1.5, 2.5}, "*2\r\n,1.5\r\n,2.5\r\n"},
		{"MixedSlice", []interface{}{1.5, true}, "*2\r\n,1.5\r\n#t\r\n"},
		{"StringSlice", []string{"a", "b"}, "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(AppendAny3(nil, tt.v))
			if got != tt.exp {
				t.Fatalf("expected %q, got %q", tt.exp, got)
			}
		})
	}
}

func TestAppendRESP3(t *testing.T) {
	tests := []struct {
		name string
		got  []byte
		exp  string
	}{
		{"Null3", AppendNull3(nil), "_\r\n"},
		{"Double", AppendDouble(nil, 9.123192839), ",9.123192839\r\n"},
		{"BoolTrue", AppendBool(nil, true), "#t\r\n"},
		{"BoolFalse", AppendBool(nil, false), "#f\r\n"},
		{"BigNumber", AppendBigNumber(nil,
			"3492890328409238509324850943850943825024385"),
			"(3492890328409238509324850943850943825024385\r\n"},
		{"Verbatim", AppendVerbatim(nil, "txt", "Some string"),
			"=15\r\ntxt:Some string\r\n"},
		{"BlobError", AppendBlobError(nil, "SYNTAX invalid syntax"),
			"!21\r\nSYNTAX invalid syntax\r\n"},
		{"Map", AppendMap(nil, 2), "%2\r\n"},
		{"Set", AppendSet(nil, 3), "~3\r\n"},
		{"Push", AppendPush(nil, 3), ">3\r\n"},
		{"Attribute", AppendAttribute(nil, 1), "|1\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.got) != tt.exp {
				t.Fatalf("expected '%s', got '%s'", tt.exp, tt.got)
			}
		})
	}
}

func TestArrayMap(t *testing.T) {
	var dst []byte
	dst = AppendArray(dst, 4)
	dst = AppendBulkString(dst, "key1")
	dst = AppendBulkString(dst, "val1")
	dst = AppendBulkString(dst, "key2")
	dst = AppendBulkString(dst, "val2")
	n, resp := ReadNextRESP(dst)
	if n != len(dst) {
		t.Fatalf("expected '%d', got '%d'", len(dst), n)
	}
	m := resp.Map()
	if len(m) != 2 {
		t.Fatalf("expected '%d', got '%d'", 2, len(m))
	}
	if m["key1"].String() != "val1" {
		t.Fatalf("expected '%s', got '%s'", "val1", m["key1"].String())
	}
	if m["key2"].String() != "val2" {
		t.Fatalf("expected '%s', got '%s'", "val2", m["key2"].String())
	}
}
