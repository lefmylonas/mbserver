// Package mbserver implments a Modbus server (slave).
package mbserver

import (
	"io"
	"net"
    "math/rand"
    "time"
	"encoding/csv"
	"os"
	"strconv"

	"github.com/goburrow/serial"
)

// Server is a Modbus slave with allocated memory for discrete inputs, coils, etc.
type Server struct {
	// Debug enables more verbose messaging.
	Debug            bool
	listeners        []net.Listener
	ports            []serial.Port
	requestChan      chan *Request
	function         [256](func(*Server, Framer) ([]byte, *Exception))
	DiscreteInputs   []byte
	Coils            []byte
	HoldingRegisters []uint16
	InputRegisters   []uint16
}

// Request contains the connection and Modbus frame.
type Request struct {
	conn  io.ReadWriteCloser
	frame Framer
}

// NewServer creates a new Modbus server (slave).
func NewServer() *Server {
	s := &Server{}

	// Allocate Modbus memory maps.
	s.DiscreteInputs = make([]byte, 65536)
	s.Coils = make([]byte, 65536)
	s.HoldingRegisters = make([]uint16, 65536)
	s.InputRegisters = make([]uint16, 65536)

	// Add default functions.
	s.function[1] = ReadCoils
	s.function[2] = ReadDiscreteInputs
	s.function[3] = ReadHoldingRegisters
	s.function[4] = ReadInputRegisters
	s.function[5] = WriteSingleCoil
	s.function[6] = WriteHoldingRegister
	s.function[15] = WriteMultipleCoils
	s.function[16] = WriteHoldingRegisters

	s.requestChan = make(chan *Request)
	go s.handler()

	rand.Seed(time.Now().UnixNano())
	go s.random_generator()

	return s
}

// RegisterFunctionHandler override the default behavior for a given Modbus function.
func (s *Server) RegisterFunctionHandler(funcCode uint8, function func(*Server, Framer) ([]byte, *Exception)) {
	s.function[funcCode] = function
}

func (s *Server) handle(request *Request) Framer {
	var exception *Exception
	var data []byte

	response := request.frame.Copy()

	function := request.frame.GetFunction()
	if s.function[function] != nil {
		data, exception = s.function[function](s, request.frame)
		response.SetData(data)
	} else {
		exception = &IllegalFunction
	}

	if exception != &Success {
		response.SetException(exception)
	}

	return response
}

func (s *Server) random_generator() {
	file, err := os.Open("realtime_8s_modbus.csv")
	if err != nil {
		// if s.Debug {
		println("Error opening file:", err.Error())
		// }
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Skip the header row
	_, err = reader.Read()
	if err != nil {
		println("Error reading header:", err.Error())
		return
	}

	for {
		record, err := reader.Read()
		if err != nil {
			// if s.Debug {
			println("Error reading file:", err.Error())
			// }
			break
		}

		for i, col := range record[1:5] { // Columns 2-5
			value, err := strconv.ParseInt(col, 10, 16)
			if err != nil {
				// if s.Debug {
				println("Error parsing value:", err.Error())
				// }
				continue
			}
			s.HoldingRegisters[6338+i-1] = uint16(value)
		}
		time.Sleep(time.Second * 8) // Wait for 8 seconds before reading the next row
	}
}

// All requests are handled synchronously to prevent modbus memory corruption.
func (s *Server) handler() {
	for {
		request := <-s.requestChan
		response := s.handle(request)
		request.conn.Write(response.Bytes())
	}
}

// Close stops listening to TCP/IP ports and closes serial ports.
func (s *Server) Close() {
	for _, listen := range s.listeners {
		listen.Close()
	}
	for _, port := range s.ports {
		port.Close()
	}
}
