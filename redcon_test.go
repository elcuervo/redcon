package redcon

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestRandomCommands fills a bunch of random commands and test various
// ways that the reader may receive data.
func TestRandomCommands(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// build random commands.
	gcmds := make([][]string, 10000)
	for i := 0; i < len(gcmds); i++ {
		args := make([]string, (rand.Int()%50)+1) // 1-50 args
		for j := 0; j < len(args); j++ {
			n := rand.Int() % 10
			if j == 0 {
				n++
			}
			arg := make([]byte, n)
			for k := 0; k < len(arg); k++ {
				arg[k] = byte(rand.Int() % 0xFF)
			}
			args[j] = string(arg)
		}
		gcmds[i] = args
	}
	// create a list of a buffers
	var bufs []string

	// pipe valid RESP commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid plain commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		var msg string
		for j := 0; j < len(args); j++ {
			quotes := false
			var narg []byte
			arg := args[j]
			if len(arg) == 0 {
				quotes = true
			}
			for k := 0; k < len(arg); k++ {
				switch arg[k] {
				default:
					narg = append(narg, arg[k])
				case ' ':
					quotes = true
					narg = append(narg, arg[k])
				case '\\', '"', '*':
					quotes = true
					narg = append(narg, '\\', arg[k])
				case '\r':
					quotes = true
					narg = append(narg, '\\', 'r')
				case '\n':
					quotes = true
					narg = append(narg, '\\', 'n')
				}
			}
			msg += " "
			if quotes {
				msg += "\""
			}
			msg += string(narg)
			if quotes {
				msg += "\""
			}
		}
		if msg != "" {
			msg = msg[1:]
		}
		msg += "\r\n"
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in broken chunks
	lmsg := ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in large broken chunks
	lmsg = ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		if len(lmsg) < 1500 {
			lmsg += msg
			continue
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// Pipe the buffers in a background routine
	rd, wr := io.Pipe()
	go func() {
		defer wr.Close()
		for _, msg := range bufs {
			io.WriteString(wr, msg)
		}
	}()
	defer rd.Close()
	cnt := 0
	idx := 0
	start := time.Now()
	r := NewReader(rd)
	for {
		cmd, err := r.ReadCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if len(cmd.Args) == 3 && string(cmd.Args[0]) == "RESET" &&
			string(cmd.Args[1]) == "THE" && string(cmd.Args[2]) == "INDEX" {
			if idx != len(gcmds) {
				t.Fatalf("did not process all commands")
			}
			idx = 0
			break
		}
		if len(cmd.Args) != len(gcmds[idx]) {
			t.Fatalf("len not equal for index %d -- %d != %d", idx, len(cmd.Args), len(gcmds[idx]))
		}
		for i := 0; i < len(cmd.Args); i++ {
			if i == 0 {
				if len(cmd.Args[i]) == len(gcmds[idx][i]) {
					ok := true
					for j := 0; j < len(cmd.Args[i]); j++ {
						c1, c2 := cmd.Args[i][j], gcmds[idx][i][j]
						if c1 >= 'A' && c1 <= 'Z' {
							c1 += 32
						}
						if c2 >= 'A' && c2 <= 'Z' {
							c2 += 32
						}
						if c1 != c2 {
							ok = false
							break
						}
					}
					if ok {
						continue
					}
				}
			} else if string(cmd.Args[i]) == string(gcmds[idx][i]) {
				continue
			}
			t.Fatalf("not equal for index %d/%d", idx, i)
		}
		idx++
		cnt++
	}
	if false {
		dur := time.Since(start)
		fmt.Printf("%d commands in %s - %.0f ops/sec\n", cnt, dur, float64(cnt)/(float64(dur)/float64(time.Second)))
	}
}
func testDetached(conn DetachedConn) {
	conn.WriteString("DETACHED")
	if err := conn.Flush(); err != nil {
		panic(err)
	}
}
func TestServerTCP(t *testing.T) {
	testServerNetwork(t, "tcp", ":12345")
}
func TestServerUnix(t *testing.T) {
	os.RemoveAll("/tmp/redcon-unix.sock")
	defer os.RemoveAll("/tmp/redcon-unix.sock")
	testServerNetwork(t, "unix", "/tmp/redcon-unix.sock")
}

func testServerNetwork(t *testing.T, network, laddr string) {
	s := NewServerNetwork(network, laddr,
		func(conn Conn, cmd Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "detach":
				go testDetached(conn.Detach())
			case "int":
				conn.WriteInt(100)
			case "bulk":
				conn.WriteBulkString("bulk")
			case "bulkbytes":
				conn.WriteBulk([]byte("bulkbytes"))
			case "null":
				conn.WriteNull()
			case "err":
				conn.WriteError("ERR error")
			case "array":
				conn.WriteArray(2)
				conn.WriteInt(99)
				conn.WriteString("Hi!")
			}
		},
		func(conn Conn) bool {
			//log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn Conn, err error) {
			//log.Printf("closed: %s [%v]", conn.RemoteAddr(), err)
		},
	)
	if err := s.Close(); err == nil {
		t.Fatalf("expected an error, should not be able to close before serving")
	}
	go func() {
		time.Sleep(time.Second / 4)
		if err := ListenAndServeNetwork(network, laddr, func(conn Conn, cmd Command) {}, nil, nil); err == nil {
			panic("expected an error, should not be able to listen on the same port")
		}
		time.Sleep(time.Second / 4)

		err := s.Close()
		if err != nil {
			panic(err)
		}
		err = s.Close()
		if err == nil {
			panic("expected an error")
		}
	}()
	done := make(chan bool)
	signal := make(chan error)
	go func() {
		defer func() {
			done <- true
		}()
		err := <-signal
		if err != nil {
			panic(err)
		}
		c, err := net.Dial(network, laddr)
		if err != nil {
			panic(err)
		}
		defer c.Close()
		do := func(cmd string) (string, error) {
			io.WriteString(c, cmd)
			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err != nil {
				return "", err
			}
			return string(buf[:n]), nil
		}
		res, err := do("PING\r\n")
		if err != nil {
			panic(err)
		}
		if res != "+PONG\r\n" {
			panic(fmt.Sprintf("expecting '+PONG\r\n', got '%v'", res))
		}
		res, err = do("BULK\r\n")
		if err != nil {
			panic(err)
		}
		if res != "$4\r\nbulk\r\n" {
			panic(fmt.Sprintf("expecting bulk, got '%v'", res))
		}
		res, err = do("BULKBYTES\r\n")
		if err != nil {
			panic(err)
		}
		if res != "$9\r\nbulkbytes\r\n" {
			panic(fmt.Sprintf("expecting bulkbytes, got '%v'", res))
		}
		res, err = do("INT\r\n")
		if err != nil {
			panic(err)
		}
		if res != ":100\r\n" {
			panic(fmt.Sprintf("expecting int, got '%v'", res))
		}
		res, err = do("NULL\r\n")
		if err != nil {
			panic(err)
		}
		if res != "$-1\r\n" {
			panic(fmt.Sprintf("expecting nul, got '%v'", res))
		}
		res, err = do("ARRAY\r\n")
		if err != nil {
			panic(err)
		}
		if res != "*2\r\n:99\r\n+Hi!\r\n" {
			panic(fmt.Sprintf("expecting array, got '%v'", res))
		}
		res, err = do("ERR\r\n")
		if err != nil {
			panic(err)
		}
		if res != "-ERR error\r\n" {
			panic(fmt.Sprintf("expecting array, got '%v'", res))
		}
		res, err = do("DETACH\r\n")
		if err != nil {
			panic(err)
		}
		if res != "+DETACHED\r\n" {
			panic(fmt.Sprintf("expecting string, got '%v'", res))
		}
	}()
	go func() {
		err := s.ListenServeAndSignal(signal)
		if err != nil {
			panic(err)
		}
	}()
	<-done
}

func TestConnImpl(t *testing.T) {
	var i interface{} = &conn{}
	if _, ok := i.(Conn); !ok {
		t.Fatalf("conn does not implement Conn interface")
	}
}

func TestWriteBulkFrom(t *testing.T) {
	wbuf := &bytes.Buffer{}
	wr := NewWriter(wbuf)
	rbuf := &bytes.Buffer{}
	testStr := "hello world"
	rbuf.WriteString(testStr)
	wr.WriteBulkFrom(int64(len(testStr)), rbuf)
	wr.Flush()
	if wbuf.String() != fmt.Sprintf("$%d\r\n%s\r\n", len(testStr), testStr) {
		t.Fatal("failed")
	}
	wbuf.Reset()
	testStr1 := "hi world"
	rbuf.WriteString(testStr1)
	wr.WriteBulkFrom(int64(len(testStr1)), rbuf)
	wr.Flush()
	if wbuf.String() != fmt.Sprintf("$%d\r\n%s\r\n", len(testStr1), testStr1) {
		t.Fatal("failed")
	}
	wbuf.Reset()
}

func TestWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := NewWriter(buf)
	wr.WriteError("ERR bad stuff")
	wr.Flush()
	if buf.String() != "-ERR bad stuff\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteString("HELLO")
	wr.Flush()
	if buf.String() != "+HELLO\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteInt(-1234)
	wr.Flush()
	if buf.String() != ":-1234\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteNull()
	wr.Flush()
	if buf.String() != "$-1\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteBulk([]byte("HELLO\r\nPLANET"))
	wr.Flush()
	if buf.String() != "$13\r\nHELLO\r\nPLANET\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteBulkString("HELLO\r\nPLANET")
	wr.Flush()
	if buf.String() != "$13\r\nHELLO\r\nPLANET\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteArray(3)
	wr.WriteBulkString("THIS")
	wr.WriteBulkString("THAT")
	wr.WriteString("THE OTHER THING")
	wr.Flush()
	if buf.String() != "*3\r\n$4\r\nTHIS\r\n$4\r\nTHAT\r\n+THE OTHER THING\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
}
func TestWriterProtocolVersion(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := NewWriter(buf)
	if wr.ProtocolVersion() != 2 {
		t.Fatalf("expected default protocol version 2, got %d", wr.ProtocolVersion())
	}
	// RESP2 null
	wr.WriteNull()
	wr.Flush()
	if buf.String() != "$-1\r\n" {
		t.Fatalf("expected $-1 null in RESP2, got %q", buf.String())
	}
	// switch to RESP3
	wr.SetProtocolVersion(3)
	if wr.ProtocolVersion() != 3 {
		t.Fatalf("expected protocol version 3, got %d", wr.ProtocolVersion())
	}
	buf.Reset()
	wr.WriteNull()
	wr.Flush()
	if buf.String() != "_\r\n" {
		t.Fatalf("expected _ null in RESP3, got %q", buf.String())
	}
}

func TestWriterRESP3(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := NewWriter(buf)
	wr.SetProtocolVersion(3)

	wr.WriteDouble(1.5)
	wr.WriteBool(true)
	wr.WriteBigNumber("123456789012345678901234567890")
	wr.WriteVerbatim("txt", "hello")
	wr.WriteBlobError("SYNTAX bad")
	wr.WriteMap(1)
	wr.WriteBulkString("k")
	wr.WriteInt(1)
	wr.WriteSet(2)
	wr.WriteString("a")
	wr.WriteString("b")
	wr.WritePush(3)
	wr.WriteString("message")
	wr.WriteString("ch")
	wr.WriteString("hi")
	wr.WriteAttribute(1)
	wr.WriteString("ttl")
	wr.WriteInt(100)
	wr.Flush()

	exp := ",1.5\r\n" +
		"#t\r\n" +
		"(123456789012345678901234567890\r\n" +
		"=9\r\ntxt:hello\r\n" +
		"!10\r\nSYNTAX bad\r\n" +
		"%1\r\n" +
		"$1\r\nk\r\n" +
		":1\r\n" +
		"~2\r\n" +
		"+a\r\n" +
		"+b\r\n" +
		">3\r\n" +
		"+message\r\n" +
		"+ch\r\n" +
		"+hi\r\n" +
		"|1\r\n" +
		"+ttl\r\n" +
		":100\r\n"
	if buf.String() != exp {
		t.Fatalf("expected %q, got %q", exp, buf.String())
	}
}

func TestWriteHello(t *testing.T) {
	// RESP2 shape: flat array of alternating key/value pairs
	buf := &bytes.Buffer{}
	c := &conn{wr: NewWriter(buf)}
	WriteHello(c, "server", "redcon", "proto", "2", "id", "1")
	c.wr.Flush()
	exp2 := "*6\r\n$6\r\nserver\r\n$6\r\nredcon\r\n$5\r\nproto\r\n$1\r\n2\r\n$2\r\nid\r\n$1\r\n1\r\n"
	if buf.String() != exp2 {
		t.Fatalf("RESP2 hello expected %q, got %q", exp2, buf.String())
	}

	// RESP3 shape: map
	buf.Reset()
	c.SetProtocolVersion(3)
	WriteHello(c, "server", "redcon", "proto", "3", "id", "1")
	c.wr.Flush()
	exp3 := "%3\r\n$6\r\nserver\r\n$6\r\nredcon\r\n$5\r\nproto\r\n$1\r\n3\r\n$2\r\nid\r\n$1\r\n1\r\n"
	if buf.String() != exp3 {
		t.Fatalf("RESP3 hello expected %q, got %q", exp3, buf.String())
	}
}

func TestWriteAnyRESP3(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := NewWriter(buf)

	// RESP2: floats/bools as bulk strings, nil as $-1
	wr.WriteAny(1.5)
	wr.WriteAny(true)
	wr.WriteAny(nil)
	wr.WriteAny("hi")
	wr.Flush()
	exp2 := "$3\r\n1.5\r\n$1\r\n1\r\n$-1\r\n$2\r\nhi\r\n"
	if buf.String() != exp2 {
		t.Fatalf("RESP2 WriteAny expected %q, got %q", exp2, buf.String())
	}

	// RESP3: floats as doubles, bools as booleans, nil as _
	buf.Reset()
	wr.SetProtocolVersion(3)
	wr.WriteAny(1.5)
	wr.WriteAny(true)
	wr.WriteAny(nil)
	wr.WriteAny("hi")
	wr.Flush()
	exp3 := ",1.5\r\n#t\r\n_\r\n$2\r\nhi\r\n"
	if buf.String() != exp3 {
		t.Fatalf("RESP3 WriteAny expected %q, got %q", exp3, buf.String())
	}

	// Conn.WriteAny respects the negotiated version
	buf.Reset()
	c := &conn{wr: NewWriter(buf)}
	c.WriteAny(nil)
	c.wr.Flush()
	if buf.String() != "$-1\r\n" {
		t.Fatalf("Conn WriteAny RESP2 null expected $-1, got %q", buf.String())
	}
	// RESP3 arrays of floats with special values serialize to canonical
	// doubles (inf, -inf, nan) like Redis.
	buf.Reset()
	wr.WriteAny([]float64{math.Inf(1), math.Inf(-1), math.NaN(), 1e300})
	wr.Flush()
	expSpecial := "*4\r\n,inf\r\n,-inf\r\n,nan\r\n,1e+300\r\n"
	if buf.String() != expSpecial {
		t.Fatalf("RESP3 special doubles expected %q, got %q", expSpecial, buf.String())
	}
}

func testMakeRawCommands(rawargs [][]string) []string {
	var rawcmds []string
	for i := 0; i < len(rawargs); i++ {
		rawcmd := "*" + strconv.FormatUint(uint64(len(rawargs[i])), 10) + "\r\n"
		for j := 0; j < len(rawargs[i]); j++ {
			rawcmd += "$" + strconv.FormatUint(uint64(len(rawargs[i][j])), 10) + "\r\n"
			rawcmd += rawargs[i][j] + "\r\n"
		}
		rawcmds = append(rawcmds, rawcmd)
	}
	return rawcmds
}

func TestReaderRespRandom(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for h := 0; h < 10000; h++ {
		var rawargs [][]string
		for i := 0; i < 100; i++ {
			// var args []string
			n := int(rand.Int() % 16)
			for j := 0; j < n; j++ {
				arg := make([]byte, rand.Int()%512)
				rand.Read(arg)
				// args = append(args, string(arg))
			}
		}
		rawcmds := testMakeRawCommands(rawargs)
		data := strings.Join(rawcmds, "")
		rd := NewReader(bytes.NewBufferString(data))
		for i := 0; i < len(rawcmds); i++ {
			if len(rawargs[i]) == 0 {
				continue
			}
			cmd, err := rd.ReadCommand()
			if err != nil {
				t.Fatal(err)
			}
			if string(cmd.Raw) != rawcmds[i] {
				t.Fatalf("expected '%v', got '%v'", rawcmds[i], string(cmd.Raw))
			}
			if len(cmd.Args) != len(rawargs[i]) {
				t.Fatalf("expected '%v', got '%v'", len(rawargs[i]), len(cmd.Args))
			}
			for j := 0; j < len(rawargs[i]); j++ {
				if string(cmd.Args[j]) != rawargs[i][j] {
					t.Fatalf("expected '%v', got '%v'", rawargs[i][j], string(cmd.Args[j]))
				}
			}
		}
	}
}

func TestPlainReader(t *testing.T) {
	rawargs := [][]string{
		{"HELLO", "WORLD"},
		{"HELLO", "WORLD"},
		{"HELLO", "PLANET"},
		{"HELLO", "JELLO"},
		{"HELLO ", "JELLO"},
	}
	rawcmds := []string{
		"HELLO WORLD\n",
		"HELLO WORLD\r\n",
		"  HELLO  PLANET \r\n",
		" \"HELLO\" \"JELLO\" \r\n",
		" \"HELLO \" JELLO \n",
	}
	rawres := []string{
		"*2\r\n$5\r\nHELLO\r\n$5\r\nWORLD\r\n",
		"*2\r\n$5\r\nHELLO\r\n$5\r\nWORLD\r\n",
		"*2\r\n$5\r\nHELLO\r\n$6\r\nPLANET\r\n",
		"*2\r\n$5\r\nHELLO\r\n$5\r\nJELLO\r\n",
		"*2\r\n$6\r\nHELLO \r\n$5\r\nJELLO\r\n",
	}
	data := strings.Join(rawcmds, "")
	rd := NewReader(bytes.NewBufferString(data))
	for i := 0; i < len(rawcmds); i++ {
		if len(rawargs[i]) == 0 {
			continue
		}
		cmd, err := rd.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if string(cmd.Raw) != rawres[i] {
			t.Fatalf("expected '%v', got '%v'", rawres[i], string(cmd.Raw))
		}
		if len(cmd.Args) != len(rawargs[i]) {
			t.Fatalf("expected '%v', got '%v'", len(rawargs[i]), len(cmd.Args))
		}
		for j := 0; j < len(rawargs[i]); j++ {
			if string(cmd.Args[j]) != rawargs[i][j] {
				t.Fatalf("expected '%v', got '%v'", rawargs[i][j], string(cmd.Args[j]))
			}
		}
	}
}

func TestReaderMaxBulkSize(t *testing.T) {
	// Within the cap: the command reads normally.
	rd := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"))
	rd.SetMaxBulkSize(5)
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.Args) != 2 || string(cmd.Args[0]) != "GET" || string(cmd.Args[1]) != "hello" {
		t.Fatalf("unexpected command: %q", cmd.Args)
	}

	// Over the cap: rejected from the declared header alone. The payload is
	// deliberately absent, proving it is refused before buffering.
	rd = NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$1000\r\n"))
	rd.SetMaxBulkSize(5)
	if _, err := rd.ReadCommand(); err == nil {
		t.Fatal("expected a protocol error for an oversized declared bulk")
	} else if _, ok := err.(*errProtocol); !ok {
		t.Fatalf("expected *errProtocol, got %T: %v", err, err)
	}

	// Zero disables the guard.
	rd = NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$1000\r\n" + strings.Repeat("x", 1000) + "\r\n"))
	rd.SetMaxBulkSize(0)
	if _, err := rd.ReadCommand(); err != nil {
		t.Fatalf("zero cap should disable the guard: %v", err)
	}
}

func TestReaderMaxCommandSize(t *testing.T) {
	// Within the cap: the command reads normally.
	rd := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"))
	rd.SetMaxCommandSize(64)
	cmd, err := rd.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.Args) != 2 || string(cmd.Args[0]) != "GET" || string(cmd.Args[1]) != "hello" {
		t.Fatalf("unexpected command: %q", cmd.Args)
	}

	// Aggregate overrun across individually permitted bulks: each bulk is well
	// under the cap, but the running total crosses it. The payloads are
	// deliberately absent, proving the guard refuses from the declared headers
	// before buffering rather than only bounding one argument.
	// Declared bytes: 4 (header) + 8 + 8 + 8 = 28 > 20.
	rd = NewReader(strings.NewReader("*3\r\n$2\r\nab\r\n$2\r\ncd\r\n$2\r\n"))
	rd.SetMaxCommandSize(20)
	if _, err := rd.ReadCommand(); err == nil {
		t.Fatal("expected a protocol error for an aggregate command overrun")
	} else if _, ok := err.(*errProtocol); !ok {
		t.Fatalf("expected *errProtocol, got %T: %v", err, err)
	}

	// The same command fits once the cap is raised: the guard is a limit, not
	// a rejection of multi-bulk commands.
	rd = NewReader(strings.NewReader("*3\r\n$2\r\nab\r\n$2\r\ncd\r\n$2\r\nef\r\n"))
	rd.SetMaxCommandSize(28)
	if _, err := rd.ReadCommand(); err != nil {
		t.Fatalf("command at the cap should read: %v", err)
	}

	// An inline command longer than the cap is refused too.
	rd = NewReader(strings.NewReader("GET hello\r\n"))
	rd.SetMaxCommandSize(5)
	if _, err := rd.ReadCommand(); err == nil {
		t.Fatal("expected a protocol error for an oversized inline command")
	} else if _, ok := err.(*errProtocol); !ok {
		t.Fatalf("expected *errProtocol, got %T: %v", err, err)
	}

	// Zero disables the guard.
	rd = NewReader(strings.NewReader("*3\r\n$2\r\nab\r\n$2\r\ncd\r\n$2\r\nef\r\n"))
	rd.SetMaxCommandSize(0)
	if _, err := rd.ReadCommand(); err != nil {
		t.Fatalf("zero cap should disable the guard: %v", err)
	}
}

func TestServerMaxCommandSizePropagates(t *testing.T) {
	s := NewServer("", func(conn Conn, cmd Command) { conn.WriteString("OK") }, nil, nil)
	s.SetMaxCommandSize(20)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(ln) }()
	defer s.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("*3\r\n$2\r\nab\r\n$2\r\ncd\r\n$2\r\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("reading reply: %v", err)
	}
	if !strings.HasPrefix(string(buf[:n]), "-ERR ") || !strings.Contains(string(buf[:n]), "maximum") {
		t.Fatalf("expected an ERR reply mentioning the maximum, got %q", buf[:n])
	}
}

// TestServerMaxCommandSizePropagatesToActiveConn pins that a live limit update
// reaches connections that were already accepted, not only future ones. Before
// the fix, each reader copied the server limit at accept and kept the old cap,
// so a command allowed by the new limit was rejected.
func TestServerMaxCommandSizePropagatesToActiveConn(t *testing.T) {
	s := NewServer("", func(conn Conn, cmd Command) { conn.WriteString("OK") }, nil, nil)
	s.SetMaxCommandSize(20)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(ln) }()
	defer s.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Establish the connection while the cap is 20 and prove it is live.
	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	if n, err := conn.Read(buf); err != nil || !strings.HasPrefix(string(buf[:n]), "+OK") {
		t.Fatalf("PING reply = %q, %v", buf[:n], err)
	}

	// Raise the cap on the running server; the already-accepted connection
	// must pick it up (this 28-byte command was over the old cap).
	s.SetMaxCommandSize(200)
	if _, err := conn.Write([]byte("*3\r\n$2\r\nab\r\n$2\r\ncd\r\n$2\r\nef\r\n")); err != nil {
		t.Fatal(err)
	}
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("reading reply after raising the cap: %v", err)
	}
	if !strings.HasPrefix(string(buf[:n]), "+OK") {
		t.Fatalf("expected +OK after raising the cap on a live connection, got %q", buf[:n])
	}
}

// TestServerMaxBulkSizePropagatesToActiveConn is the same guarantee for the
// per-bulk cap.
func TestServerMaxBulkSizePropagatesToActiveConn(t *testing.T) {
	s := NewServer("", func(conn Conn, cmd Command) { conn.WriteString("OK") }, nil, nil)
	s.SetMaxBulkSize(5)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(ln) }()
	defer s.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	if n, err := conn.Read(buf); err != nil || !strings.HasPrefix(string(buf[:n]), "+OK") {
		t.Fatalf("PING reply = %q, %v", buf[:n], err)
	}

	// Raise the per-bulk cap; a 20-byte bulk that was over the old 5-byte cap
	// must now be accepted on the same connection.
	s.SetMaxBulkSize(100)
	payload := strings.Repeat("x", 20)
	if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$20\r\n" + payload + "\r\n")); err != nil {
		t.Fatal(err)
	}
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("reading reply after raising the bulk cap: %v", err)
	}
	if !strings.HasPrefix(string(buf[:n]), "+OK") {
		t.Fatalf("expected +OK after raising the bulk cap on a live connection, got %q", buf[:n])
	}
}

func TestParse(t *testing.T) {
	_, err := Parse(nil)
	if err != errIncompleteCommand {
		t.Fatalf("expected '%v', got '%v'", errIncompleteCommand, err)
	}
	_, err = Parse([]byte("*1\r\n"))
	if err != errIncompleteCommand {
		t.Fatalf("expected '%v', got '%v'", errIncompleteCommand, err)
	}
	_, err = Parse([]byte("*-1\r\n"))
	if err != errInvalidMultiBulkLength {
		t.Fatalf("expected '%v', got '%v'", errInvalidMultiBulkLength, err)
	}
	_, err = Parse([]byte("*0\r\n"))
	if err != errInvalidMultiBulkLength {
		t.Fatalf("expected '%v', got '%v'", errInvalidMultiBulkLength, err)
	}
	cmd, err := Parse([]byte("*1\r\n$1\r\nA\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(cmd.Raw) != "*1\r\n$1\r\nA\r\n" {
		t.Fatalf("expected '%v', got '%v'", "*1\r\n$1\r\nA\r\n", string(cmd.Raw))
	}
	if len(cmd.Args) != 1 {
		t.Fatalf("expected '%v', got '%v'", 1, len(cmd.Args))
	}
	if string(cmd.Args[0]) != "A" {
		t.Fatalf("expected '%v', got '%v'", "A", string(cmd.Args[0]))
	}
	cmd, err = Parse([]byte("A\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(cmd.Raw) != "*1\r\n$1\r\nA\r\n" {
		t.Fatalf("expected '%v', got '%v'", "*1\r\n$1\r\nA\r\n", string(cmd.Raw))
	}
	if len(cmd.Args) != 1 {
		t.Fatalf("expected '%v', got '%v'", 1, len(cmd.Args))
	}
	if string(cmd.Args[0]) != "A" {
		t.Fatalf("expected '%v', got '%v'", "A", string(cmd.Args[0]))
	}
}

func TestPubSubMessageRESP3(t *testing.T) {
	buf := &bytes.Buffer{}
	c := &conn{wr: NewWriter(buf)}
	sconn := &pubSubConn{dconn: &detachedConn{conn: c}}

	// RESP2 message
	sconn.writeMessage(false, "", "ch", "hi")
	if buf.String() != "*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n" {
		t.Fatalf("RESP2 message mismatch: %q", buf.String())
	}
	// RESP2 pmessage
	buf.Reset()
	sconn.writeMessage(true, "p*", "ch", "hi")
	if buf.String() != "*4\r\n$8\r\npmessage\r\n$2\r\np*\r\n$2\r\nch\r\n$2\r\nhi\r\n" {
		t.Fatalf("RESP2 pmessage mismatch: %q", buf.String())
	}
	// RESP3 message
	buf.Reset()
	c.SetProtocolVersion(3)
	sconn.writeMessage(false, "", "ch", "hi")
	if buf.String() != ">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n" {
		t.Fatalf("RESP3 message mismatch: %q", buf.String())
	}
	// RESP3 pmessage
	buf.Reset()
	sconn.writeMessage(true, "p*", "ch", "hi")
	if buf.String() != ">4\r\n$8\r\npmessage\r\n$2\r\np*\r\n$2\r\nch\r\n$2\r\nhi\r\n" {
		t.Fatalf("RESP3 pmessage mismatch: %q", buf.String())
	}
}

func TestPubSubConfirmationsRESP3(t *testing.T) {
	buf := &bytes.Buffer{}
	c := &conn{wr: NewWriter(buf)}
	sconn := &pubSubConn{dconn: &detachedConn{conn: c}}

	// RESP2 subscribe / psubscribe
	sconn.writeSubscribeConfirmation(false, "ch", 3)
	sconn.dconn.Flush()
	if buf.String() != "*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:3\r\n" {
		t.Fatalf("RESP2 subscribe mismatch: %q", buf.String())
	}
	buf.Reset()
	sconn.writeSubscribeConfirmation(true, "p*", 2)
	sconn.dconn.Flush()
	if buf.String() != "*3\r\n$10\r\npsubscribe\r\n$2\r\np*\r\n:2\r\n" {
		t.Fatalf("RESP2 psubscribe mismatch: %q", buf.String())
	}
	// RESP2 unsubscribe all with null channel
	buf.Reset()
	sconn.writeUnsubscribeConfirmation(false, "", 0)
	sconn.dconn.Flush()
	if buf.String() != "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n" {
		t.Fatalf("RESP2 unsubscribe mismatch: %q", buf.String())
	}

	// RESP3 subscribe / psubscribe
	buf.Reset()
	c.SetProtocolVersion(3)
	sconn.writeSubscribeConfirmation(false, "ch", 3)
	sconn.dconn.Flush()
	if buf.String() != ">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:3\r\n" {
		t.Fatalf("RESP3 subscribe mismatch: %q", buf.String())
	}
	buf.Reset()
	sconn.writeSubscribeConfirmation(true, "p*", 2)
	sconn.dconn.Flush()
	if buf.String() != ">3\r\n$10\r\npsubscribe\r\n$2\r\np*\r\n:2\r\n" {
		t.Fatalf("RESP3 psubscribe mismatch: %q", buf.String())
	}
	// RESP3 unsubscribe all with RESP3 null channel
	buf.Reset()
	sconn.writeUnsubscribeConfirmation(false, "", 0)
	sconn.dconn.Flush()
	if buf.String() != ">3\r\n$11\r\nunsubscribe\r\n_\r\n:0\r\n" {
		t.Fatalf("RESP3 unsubscribe mismatch: %q", buf.String())
	}
	// RESP3 punsubscribe with a channel
	buf.Reset()
	sconn.writeUnsubscribeConfirmation(true, "p*", 1)
	sconn.dconn.Flush()
	if buf.String() != ">3\r\n$12\r\npunsubscribe\r\n$2\r\np*\r\n:1\r\n" {
		t.Fatalf("RESP3 punsubscribe mismatch: %q", buf.String())
	}
}

func TestPubSubIntegrationRESP3(t *testing.T) {
	addr := ":12348"
	var ps PubSub
	go func() {
		err := ListenAndServe(addr, func(conn Conn, cmd Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "hello":
				if len(cmd.Args) == 2 && string(cmd.Args[1]) == "3" {
					conn.SetProtocolVersion(3)
				}
				conn.WriteString("OK")
			case "publish":
				count := ps.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
				conn.WriteInt(count)
			case "subscribe":
				ps.Subscribe(conn, string(cmd.Args[1]))
			}
		}, nil, nil)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second / 8)

	dial := func() net.Conn {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	readRESP := func(rd *bufio.Reader) RESP {
		var buf []byte
		for {
			line, err := rd.ReadBytes('\n')
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			buf = append(buf, line...)
			n, resp := ReadNextRESP(buf)
			if n > 0 {
				return resp
			}
		}
	}

	// client A negotiates RESP3 and subscribes
	a := dial()
	defer a.Close()
	ard := bufio.NewReader(a)
	fmt.Fprint(a, "HELLO 3\r\n")
	if rr := readRESP(ard); rr.Type != String || rr.String() != "OK" {
		t.Fatalf("expected +OK, got %v", rr)
	}
	fmt.Fprint(a, "SUBSCRIBE ch\r\n")
	if rr := readRESP(ard); rr.Type != Push {
		t.Fatalf("expected RESP3 push confirmation, got type %d", rr.Type)
	}

	// client B stays RESP2 and subscribes
	b := dial()
	defer b.Close()
	brd := bufio.NewReader(b)
	fmt.Fprint(b, "SUBSCRIBE ch\r\n")
	if rr := readRESP(brd); rr.Type != Array {
		t.Fatalf("expected RESP2 array confirmation, got type %d", rr.Type)
	}

	// client C publishes
	c3 := dial()
	defer c3.Close()
	crd := bufio.NewReader(c3)
	fmt.Fprint(c3, "PUBLISH ch hello\r\n")
	if rr := readRESP(crd); rr.Type != Integer || rr.Int() != 2 {
		t.Fatalf("expected publish count 2, got %v", rr)
	}

	// A receives a push message, B receives an array message
	if rr := readRESP(ard); rr.Type != Push {
		t.Fatalf("expected RESP3 push message, got type %d", rr.Type)
	}
	if rr := readRESP(brd); rr.Type != Array {
		t.Fatalf("expected RESP2 array message, got type %d", rr.Type)
	}
}

func TestPubSub(t *testing.T) {
	addr := ":12346"
	done := make(chan bool)
	go func() {
		var ps PubSub
		go func() {
			tch := time.NewTicker(time.Millisecond * 5)
			defer tch.Stop()
			channels := []string{"achan1", "bchan2", "cchan3", "dchan4"}
			for i := 0; ; i++ {
				select {
				case <-tch.C:
				case <-done:
					for {
						var empty bool
						ps.mu.Lock()
						if len(ps.conns) == 0 {
							if ps.chans.Len() != 0 {
								panic("chans not empty")
							}
							empty = true
						}
						ps.mu.Unlock()
						if empty {
							break
						}
						time.Sleep(time.Millisecond * 10)
					}
					done <- true
					return
				}
				channel := channels[i%len(channels)]
				message := fmt.Sprintf("message %d", i)
				ps.Publish(channel, message)
			}
		}()
		panic(ListenAndServe(addr, func(conn Conn, cmd Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" +
					string(cmd.Args[0]) + "'")
			case "publish":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" +
						string(cmd.Args[0]) + "' command")
					return
				}
				count := ps.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
				conn.WriteInt(count)
			case "subscribe", "psubscribe":
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" +
						string(cmd.Args[0]) + "' command")
					return
				}
				command := strings.ToLower(string(cmd.Args[0]))
				for i := 1; i < len(cmd.Args); i++ {
					if command == "psubscribe" {
						ps.Psubscribe(conn, string(cmd.Args[i]))
					} else {
						ps.Subscribe(conn, string(cmd.Args[i]))
					}
				}
			}
		}, nil, nil))
	}()

	final := make(chan bool)
	go func() {
		select {
		case <-time.Tick(time.Second * 30):
			panic("timeout")
		case <-final:
			return
		}
	}()

	// create 10 connections
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			var conn net.Conn
			for i := 0; i < 5; i++ {
				var err error
				conn, err = net.Dial("tcp", addr)
				if err != nil {
					time.Sleep(time.Second / 10)
					continue
				}
			}
			if conn == nil {
				panic("could not connect to server")
			}
			defer conn.Close()

			regs := make(map[string]int)
			var maxp int
			var maxs int
			fmt.Fprintf(conn, "subscribe achan1\r\n")
			fmt.Fprintf(conn, "subscribe bchan2 cchan3\r\n")
			fmt.Fprintf(conn, "psubscribe a*1\r\n")
			fmt.Fprintf(conn, "psubscribe b*2 c*3\r\n")

			// collect 50 messages from each channel
			rd := bufio.NewReader(conn)
			var buf []byte
			for {
				line, err := rd.ReadBytes('\n')
				if err != nil {
					panic(err)
				}
				buf = append(buf, line...)
				n, resp := ReadNextRESP(buf)
				if n == 0 {
					continue
				}
				buf = nil
				if resp.Type != Array {
					panic("expected array")
				}
				var vals []RESP
				resp.ForEach(func(item RESP) bool {
					vals = append(vals, item)
					return true
				})

				name := string(vals[0].Data)
				switch name {
				case "subscribe":
					if len(vals) != 3 {
						panic("invalid count")
					}
					ch := string(vals[1].Data)
					regs[ch] = 0
					maxs, _ = strconv.Atoi(string(vals[2].Data))
				case "psubscribe":
					if len(vals) != 3 {
						panic("invalid count")
					}
					ch := string(vals[1].Data)
					regs[ch] = 0
					maxp, _ = strconv.Atoi(string(vals[2].Data))
				case "message":
					if len(vals) != 3 {
						panic("invalid count")
					}
					ch := string(vals[1].Data)
					regs[ch] = regs[ch] + 1
				case "pmessage":
					if len(vals) != 4 {
						panic("invalid count")
					}
					ch := string(vals[1].Data)
					regs[ch] = regs[ch] + 1
				}
				if len(regs) == 6 && maxp == 3 && maxs == 3 {
					ready := true
					for _, count := range regs {
						if count < 50 {
							ready = false
							break
						}
					}
					if ready {
						// all messages have been received
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	// notify sender
	done <- true
	// wait for sender
	<-done
	// stop the timeout
	final <- true
}
