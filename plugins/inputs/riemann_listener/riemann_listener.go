package riemann_listener

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
	tlsint "github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/inputs"

	riemanngo "github.com/riemann/riemann-go-client"
	riemangoProto "github.com/riemann/riemann-go-client/proto"
)

type setReadBufferer interface {
	SetReadBuffer(bytes int) error
}

//SocketListener is a structservice_address = "tcp://:
type RiemannSocketListener struct {
	ServiceAddress  string             `toml:"service_address"`
	MaxConnections  int                `toml:"max_connections"`
	ReadBufferSize  internal.Size      `toml:"read_buffer_size"`
	ReadTimeout     *internal.Duration `toml:"read_timeout"`
	KeepAlivePeriod *internal.Duration `toml:"keep_alive_period"`
	SocketMode      string             `toml:"socket_mode"`
	tlsint.ServerConfig

	wg sync.WaitGroup

	Log telegraf.Logger

	telegraf.Accumulator
	io.Closer
}

type packetSocketListener struct {
	net.PacketConn
	*RiemannSocketListener
}

type riemannListener struct {
	net.Listener
	*RiemannSocketListener

	sockType string

	connections    map[string]net.Conn
	connectionsMtx sync.Mutex
}

func (rsl *riemannListener) listen() {
	rsl.connections = map[string]net.Conn{}

	wg := sync.WaitGroup{}

	for {
		c, err := rsl.Accept()
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				rsl.Log.Error(err.Error())
			}
			break
		}

		if rsl.ReadBufferSize.Size > 0 {
			if srb, ok := c.(setReadBufferer); ok {
				srb.SetReadBuffer(int(rsl.ReadBufferSize.Size))
			} else {
				rsl.Log.Warnf("Unable to set read buffer on a %s socket", rsl.sockType)
			}
		}

		rsl.connectionsMtx.Lock()
		if rsl.MaxConnections > 0 && len(rsl.connections) >= rsl.MaxConnections {
			rsl.connectionsMtx.Unlock()
			c.Close()
			continue
		}
		rsl.connections[c.RemoteAddr().String()] = c
		rsl.connectionsMtx.Unlock()

		if err := rsl.setKeepAlive(c); err != nil {
			rsl.Log.Errorf("Unable to configure keep alive %q: %s", rsl.ServiceAddress, err.Error())
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			rsl.read(c)
		}()
	}

	rsl.connectionsMtx.Lock()
	for _, c := range rsl.connections {
		c.Close()
	}
	rsl.connectionsMtx.Unlock()

	wg.Wait()
}

func (rsl *riemannListener) setKeepAlive(c net.Conn) error {
	if rsl.KeepAlivePeriod == nil {
		return nil
	}
	tcpc, ok := c.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("cannot set keep alive on a %s socket", strings.SplitN(rsl.ServiceAddress, "://", 2)[0])
	}
	if rsl.KeepAlivePeriod.Duration == 0 {
		return tcpc.SetKeepAlive(false)
	}
	if err := tcpc.SetKeepAlive(true); err != nil {
		return err
	}
	return tcpc.SetKeepAlivePeriod(rsl.KeepAlivePeriod.Duration)
}

func (rsl *riemannListener) removeConnection(c net.Conn) {
	rsl.connectionsMtx.Lock()
	delete(rsl.connections, c.RemoteAddr().String())
	rsl.connectionsMtx.Unlock()
}

// readMessages will read Riemann messages from the TCP connection
func readMessages(r io.Reader, p []byte) error {
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			return err
		}
	}
	return nil
}

func checkError(err error) {
	log.Println("The error is")
	if err != nil {
		log.Println(err.Error())
	}
}

func riemanProtobufDecoder(conn net.Conn) ([]telegraf.Metric, error) {

	var err error
	messagePb := &riemangoProto.Msg{}
	var header uint32
	if err = binary.Read(conn, binary.BigEndian, &header); err != nil {

		return nil, err
	}
	data := make([]byte, header)

	log.Println("Reading in Data")
	if err = readMessages(conn, data); err != nil {
		checkError(err)
		return nil, err
	}
	log.Println("Reading in Unmarshal")
	if err = proto.Unmarshal(data, messagePb); err != nil {
		checkError(err)
		return nil, err
	}
	log.Println("Reading Out of Unmarshal")
	riemannEvents := riemanngo.ProtocolBuffersToEvents(messagePb.Events)
	//log.Println(riemannEvents[0])
	x, err := reimanEventsToMetrics(riemannEvents)
	return x, err
	//return riemannEvents, nil

}

func riemannReturnResponse(conn net.Conn) {
	t := true
	message := new(riemangoProto.Msg)
	message.Ok = &t
	returnData, err := proto.Marshal(message)
	checkError(err)
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, uint32(len(returnData))); err != nil {
		checkError(err)
	}
	// send the msg length
	if _, err = conn.Write(b.Bytes()); err != nil {
		checkError(err)
	}
	if _, err = conn.Write(returnData); err != nil {
		log.Println("Somethign")
		checkError(err)
	}
}

func riemannReturnErrorResponse(conn net.Conn) {
	t := false
	errorString := "Riemann processing Error"
	message := new(riemangoProto.Msg)
	message.Ok = &t
	message.Error = &errorString
	returnData, err := proto.Marshal(message)
	checkError(err)
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, uint32(len(returnData))); err != nil {
		checkError(err)
	}
	// send the msg length
	if _, err = conn.Write(b.Bytes()); err != nil {
		checkError(err)
	}
	if _, err = conn.Write(returnData); err != nil {
		log.Println("Somethign")
		checkError(err)
	}
}

func reimanEventsToMetrics(riemannEvents []riemanngo.Event) ([]telegraf.Metric, error) {
	var grouper = metric.NewSeriesGrouper()
	for i := range riemannEvents {
		tags := make(map[string]string)
		fieldValues := map[string]interface{}{}
		for _, tag := range riemannEvents[i].Tags {
			tags[tag] = tag
		}
		tags["Host"] = riemannEvents[i].Host
		tags["Description"] = riemannEvents[i].Description
		tags["State"] = riemannEvents[i].State
		tags["TTL"] = riemannEvents[i].TTL.String()
		fieldValues["Metric"] = riemannEvents[i].Metric

		err := grouper.Add(riemannEvents[i].Service, tags, riemannEvents[i].Time, "metric", riemannEvents[i].Metric)
		if err != nil {
			return nil, err
		}

	}
	return grouper.Metrics(), nil
}

func (rsl *riemannListener) read(c net.Conn) {
	defer rsl.removeConnection(c)
	defer c.Close()

	for {
		if rsl.ReadTimeout != nil && rsl.ReadTimeout.Duration > 0 {
			c.SetDeadline(time.Now().Add(rsl.ReadTimeout.Duration))
		}

		riemannEvents, err := riemanProtobufDecoder(c)
		//log.Println(riemannEvents[0])
		if err != nil {
			log.Println(err)
			rsl.Log.Errorf("Unable to parse incoming line: %s", err.Error())
			// TODO rate limit
			os.Exit(-1)
		}
		//	telegrafMetrics, err := reimanEventsToMetrics(riemannEvents)
		if err != nil {
			checkError(err)
			riemannReturnErrorResponse(c)

		} else {
			log.Println("Hmmmm")
			log.Println(riemannEvents)
			for _, metric := range riemannEvents {
				log.Println(metric)
				rsl.AddMetric(metric)
			}
			riemannReturnResponse(c)
		}

	}

}

func udpListen(network string, address string) (net.PacketConn, error) {
	switch network {
	case "udp", "udp4", "udp6":
		var addr *net.UDPAddr
		var err error
		var ifi *net.Interface
		if spl := strings.SplitN(address, "%", 2); len(spl) == 2 {
			address = spl[0]
			ifi, err = net.InterfaceByName(spl[1])
			if err != nil {
				return nil, err
			}
		}
		addr, err = net.ResolveUDPAddr(network, address)
		if err != nil {
			return nil, err
		}
		if addr.IP.IsMulticast() {
			return net.ListenMulticastUDP(network, ifi, addr)
		}
		return net.ListenUDP(network, addr)
	}
	return net.ListenPacket(network, address)
}

func (sl *RiemannSocketListener) Description() string {
	return "Generic socket listener capable of handling multiple socket types."
}

func (sl *RiemannSocketListener) SampleConfig() string {
	return `
  ## URL to listen on
  # service_address = "tcp://:8094"
  # service_address = "tcp://127.0.0.1:http"
  # service_address = "tcp4://:8094"
  # service_address = "tcp6://:8094"
  # service_address = "tcp6://[2001:db8::1]:8094"
  # service_address = "udp://:8094"
  # service_address = "udp4://:8094"
  # service_address = "udp6://:8094"
  # service_address = "unix:///tmp/telegraf.sock"
  # service_address = "unixgram:///tmp/telegraf.sock"

  ## Change the file mode bits on unix sockets.  These permissions may not be
  ## respected by some platforms, to safely restrict write permissions it is best
  ## to place the socket into a directory that has previously been created
  ## with the desired permissions.
  ##   ex: socket_mode = "777"
  # socket_mode = ""

  ## Maximum number of concurrent connections.
  ## Only applies to stream sockets (e.g. TCP).
  ## 0 (default) is unlimited.
  # max_connections = 1024

  ## Read timeout.
  ## Only applies to stream sockets (e.g. TCP).
  ## 0 (default) is unlimited.
  # read_timeout = "30s"

  ## Optional TLS configuration.
  ## Only applies to stream sockets (e.g. TCP).
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key  = "/etc/telegraf/key.pem"
  ## Enables client authentication if set.
  # tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]

  ## Maximum socket buffer size (in bytes when no unit specified).
  ## For stream sockets, once the buffer fills up, the sender will start backing up.
  ## For datagram sockets, once the buffer fills up, metrics will start dropping.
  ## Defaults to the OS default.
  # read_buffer_size = "64KiB"

  ## Period between keep alive probes.
  ## Only applies to TCP sockets.
  ## 0 disables keep alive probes.
  ## Defaults to the OS configuration.
  # keep_alive_period = "5m"

  ## Data format to consume.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  # data_format = "influx"

  ## Content encoding for message payloads, can be set to "gzip" to or
  ## "identity" to apply no encoding.
  # content_encoding = "identity"
`
}

func (sl *RiemannSocketListener) Gather(_ telegraf.Accumulator) error {
	return nil
}

func (sl *RiemannSocketListener) Start(acc telegraf.Accumulator) error {
	sl.Accumulator = acc
	spl := strings.SplitN(sl.ServiceAddress, "://", 2)
	if len(spl) != 2 {
		return fmt.Errorf("invalid service address: %s", sl.ServiceAddress)
	}

	protocol := spl[0]
	addr := spl[1]

	switch protocol {
	case "tcp", "tcp4", "tcp6":
		tlsCfg, err := sl.ServerConfig.TLSConfig()
		if err != nil {
			return err
		}

		var l net.Listener
		if tlsCfg == nil {
			l, err = net.Listen(protocol, addr)
		} else {
			l, err = tls.Listen(protocol, addr, tlsCfg)
		}
		if err != nil {
			return err
		}

		sl.Log.Infof("Listening on %s://%s", protocol, l.Addr())

		rsl := &riemannListener{
			Listener:              l,
			RiemannSocketListener: sl,
			sockType:              spl[0],
		}

		sl.Closer = rsl
		sl.wg = sync.WaitGroup{}
		sl.wg.Add(1)
		go func() {
			defer sl.wg.Done()
			rsl.listen()
		}()
	default:
		return fmt.Errorf("unknown protocol '%s' in '%s'", protocol, sl.ServiceAddress)
	}

	return nil
}

//Stop is a function
func (sl *RiemannSocketListener) Stop() {
	if sl.Closer != nil {
		sl.Close()
		sl.Closer = nil
	}
	sl.wg.Wait()
}

func newRiemannSocketListener() *RiemannSocketListener {

	return &RiemannSocketListener{}
}

type unixCloser struct {
	path   string
	closer io.Closer
}

func (uc unixCloser) Close() error {
	err := uc.closer.Close()
	os.Remove(uc.path) // ignore error
	return err
}

func init() {
	inputs.Add("riemann_listener", func() telegraf.Input { return newRiemannSocketListener() })
}
