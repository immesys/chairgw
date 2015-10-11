package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"

	"code.google.com/p/go-uuid/uuid"
)

type Session struct {
	CurrentTime uint32
	HaveTime    bool
	UuidMap     map[string]string
	ReadPtr     int
}

var giles = "http://127.0.0.1:8079/api/query"
var seslock sync.Mutex
var sessions map[uint16]*Session
var streams []string = []string{"humidity", "temperature", "seat_fan", "seat_heat", "back_fan", "back_heat", "battery", "generation"}
var sock *net.UDPConn
var socklock sync.Mutex

type UUReply struct {
	Uuid string `json:"uuid"`
}

func createSession(serial uint16) *Session {
	rv := &Session{HaveTime: false, ReadPtr: -1, UuidMap: make(map[string]string)}
	for _, e := range streams {
		qry := fmt.Sprintf("select uuid where Path like pecs/%04x/%s", serial, e)
		resp, err := http.Post(giles, "text/plain", strings.NewReader(qry))
		if err != nil {
			panic(err)
		}
		rep, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		var uu []map[string]string
		err = json.Unmarshal(rep, &uu)
		if err != nil {
			panic(err)
		}
		if len(uu) == 0 {
			//No existing stream, make it up
			fmt.Printf("No stream found for pecs/%04x/%s, creating new UUID\n", serial, e)
			rv.UuidMap[e] = uuid.NewRandom().String()
		} else {
			rv.UuidMap[e] = uu[0]["uuid"]
		}
	}
	return rv
}

func (ses *Session) Process(serial uint16, ra *net.UDPAddr, msg []byte) {
	var read_ptr = int(uint16(msg[0]) + (uint16(msg[1]) << 8))

	fmt.Printf("Processed 0x%04x::%x\n", serial, read_ptr)

	process := false

	if ses.ReadPtr == -1 {
		//We need to resync. Accept this packet
		ses.ReadPtr = read_ptr
		ses.HaveTime = false
		process = true
	} else {
		expected_ptr := (ses.ReadPtr + 16) % 0x1e0000
		if read_ptr == expected_ptr {
			//This packet is what we expected
			ses.ReadPtr = read_ptr
			process = true
		} else if read_ptr == ses.ReadPtr {
			//Duplicate (must have lost the release packet. Resend it)
		} else {
			fmt.Printf("Lost mote sync. Expected %x, got %x\n", expected_ptr, read_ptr)
			ses.ReadPtr = read_ptr
			ses.HaveTime = false
			process = true
		}
	}
	if process {
		//Magic
	}
	socklock.Lock()
	_, err := sock.WriteToUDP([]byte{uint8(read_ptr), uint8(read_ptr >> 8)}, ra)
	if err != nil {
		panic(err)
	}
	socklock.Unlock()
}
func handlePacket(ra *net.UDPAddr, msg []byte) {
	fmt.Printf("Got packet from %+v\n", ra)
	var serial = (uint16(ra.IP[14]) << 8) + uint16(ra.IP[15])
	seslock.Lock()
	ses, ok := sessions[serial]
	if !ok {
		ses = createSession(serial)
		sessions[serial] = ses
	}
	seslock.Unlock()

	ses.Process(serial, ra, msg)
	/*

	 */
}
func main() {
	sessions = make(map[uint16]*Session)
	addr, err := net.ResolveUDPAddr("udp6", ":4040")
	if err != nil {
		panic(err)
	}
	sock, err = net.ListenUDP("udp6", addr)
	if err != nil {
		panic(err)
	}
	for {
		buf := make([]byte, 2048)
		ln, addr, err := sock.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("Got error: %v\n", err)
			continue
		}
		go handlePacket(addr, buf[:ln])
	}
}
