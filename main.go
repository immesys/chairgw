package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	_ "github.com/go-sql-driver/mysql"
)

type Session struct {
	CurrentTime     uint32
	HaveTime        bool
	HaveInitialTime bool
	UuidMap         map[string]string
	ReadPtr         int
	CorruptRecords  int
}

var giles = "http://127.0.0.1:8079/api/query"
var gilesi = "http://127.0.0.1:8079/add/nokey"
var seslock sync.Mutex
var sessions map[uint16]*Session
var streams []string = []string{"humidity", "temperature", "occupancy", "session_corrupt_records", "resets", "log_ptr", "battery_ok", "fw_version", "offset", "seat_fan", "seat_heat", "back_fan", "back_heat", "battery", "wall_in_remote_time", "remote_in_wall_time"}
var sock *net.UDPConn
var socklock sync.Mutex

var smap_template = `
{
	"%s": {
		"Metadata": {
			"SourceName": "PECS"
		},
		"Properties": {
			"Timezone": "America/Los_Angeles",
			"ReadingType":"double",
			"UnitofMeasure":"%s",
			"UnitofTime":"ms",
			"StreamType":"numeric"
		},
		"Readings": [
			[%d, %f]
		],
		"uuid": "%s"
	}
}
`

type UUReply struct {
	Uuid string `json:"uuid"`
}

func gilesInsert(uuid string, path string, unit string, timestamp uint64, value float64) {
	in := fmt.Sprintf(smap_template, path, unit, timestamp, value, uuid)
	resp, err := http.Post(gilesi, "text/plain", strings.NewReader(in))
	if err != nil {
		panic(err)
	}
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
}
func (ses *Session) GetTime() uint64 {
	//s := uint64(ses.CurrentTime) + 1420070400
	//fmt.Printf("Remote time: %v\n", time.Unix(int64(s), 0))
	return (uint64(ses.CurrentTime) + 1420070400) * 1000
}
func GetWallTime() uint64 {
	return uint64(time.Now().UnixNano() / 1000000)
}
func createSession(serial uint16) *Session {
	rv := &Session{HaveTime: false, HaveInitialTime: false, ReadPtr: -1, UuidMap: make(map[string]string)}
	for _, e := range streams {
		qry := fmt.Sprintf("select uuid where Path like '/%04x/%s'", serial, e)
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
			fmt.Printf("No stream found for %04x/%s, creating new UUID\n", serial, e)
			rv.UuidMap[e] = uuid.NewRandom().String()
		} else {
			rv.UuidMap[e] = uu[0]["uuid"]
		}
	}
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			if rv.HaveInitialTime {
				gilesInsert(rv.UuidMap["remote_in_wall_time"], fmt.Sprintf("/%04x/remote_in_wall_time", serial), "Remote seconds", uint64(time.Now().UnixNano()/1000000), float64(rv.GetTime()/1000))
				offset := (float64(rv.GetTime()) - float64(time.Now().UnixNano()/1000)/1000) / 1000
				gilesInsert(rv.UuidMap["offset"], fmt.Sprintf("/%04x/offset", serial), "Seconds", uint64(time.Now().UnixNano()/1000000), offset)
			}
			gilesInsert(rv.UuidMap["session_corrupt_records"], fmt.Sprintf("/%04x/session_corrupt_records", serial), "Corrupt Records", uint64(time.Now().UnixNano()/1000000), float64(rv.CorruptRecords))
		}
	}()
	return rv
}

func (ses *Session) Process(serial uint16, ra *net.UDPAddr, msg []byte) {
	var read_ptr = int(uint32(msg[0]) + (uint32(msg[1]) << 8) + (uint32(msg[2]) << 16))

	fmt.Printf("Processed 0x%04x::%x\n", serial, read_ptr)
	gilesInsert(ses.UuidMap["log_ptr"], fmt.Sprintf("/%04x/log_ptr", serial), "Record index", GetWallTime(), float64(read_ptr))
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
		for i := 0; i < 16; i++ {
			r := msg[3+i*4 : 3+(i+1)*4]
			typ := r[0]
			switch {
			case (typ & 0xf0) == 0xf0: //BLANK
				fmt.Printf("Skipping blank record: %x %x %x %x\n", r[0], r[1], r[2], r[3])
				ses.CorruptRecords += 1
				continue
			case (typ & 0xf0) == 0xe0: //TIMESTAMP
				ts := (uint32(r[0]) & 0xf << 24) | (uint32(r[1]) << 16) | (uint32(r[2]) << 8) | uint32(r[3])
				ses.CurrentTime = ts
				ses.HaveTime = true
				ses.HaveInitialTime = true
				fmt.Printf(">>> Got ABS TS\n")
				gilesInsert(ses.UuidMap["wall_in_remote_time"], fmt.Sprintf("/%04x/wall_in_remote_time", serial), "Wall seconds", ses.GetTime(), float64(time.Now().UnixNano()/1000000)/1000.)
			case (typ & 0xc0) == 0: //Temp/Hum/Occ
				if !ses.HaveTime {
					fmt.Printf("Dropping THO record, no absolute time\n")
					continue
				}
				rts := uint32(r[0] >> 3 & 7)
				occ := r[0]>>2&1 > 0
				occf := float64(0.0)
				if occ {
					occf = 1.0
				}
				hum := int(r[0]&3)<<10 + int(r[1])<<2 + int(r[2]>>6)
				tmp := int(r[2]&0x3f)<<8 + int(r[3])
				ses.CurrentTime += rts
				fmt.Printf(">>> Got THO, rts was %d\n", rts)
				gilesInsert(ses.UuidMap["wall_in_remote_time"], fmt.Sprintf("/%04x/wall_in_remote_time", serial), "Wall seconds", ses.GetTime(), float64(time.Now().UnixNano()/1000000)/1000.)
				gilesInsert(ses.UuidMap["occupancy"], fmt.Sprintf("/%04x/occupancy", serial), "Binary", ses.GetTime(), occf)
				//fmt.Printf("Inserting occupancy value %f\n", occf)
				if hum != 0 {
					real_hum := -6 + 125*float64(hum<<4)/65536
					//fmt.Printf("Inserting humidity value %f\n", real_hum)
					gilesInsert(ses.UuidMap["humidity"], fmt.Sprintf("/%04x/humidity", serial), "%RH", ses.GetTime(), real_hum)
				} else {
					fmt.Println("Bad humidity record")
				}
				if tmp != 0 {
					fmt.Printf("Got raw temp record: %x\n", tmp)
					real_tmp := (-46.85+175.72*float64(tmp<<2)/65536)*1.8 + 32
					//fmt.Printf("Inserting temperature value %f\n", real_tmp)
					gilesInsert(ses.UuidMap["temperature"], fmt.Sprintf("/%04x/temperature", serial), "Fahrenheit", ses.GetTime(), real_tmp)
				} else {
					fmt.Println("Bad temperature record")
				}
			case (typ & 0xc0) == 0x40: //Settings
				if !ses.HaveTime {
					fmt.Printf("Dropping SET record, no absolute time\n")
					continue
				}
				rts := uint32(r[0] >> 4 & 3)
				seat_heat := ((r[0] & 0xf) << 3) + (r[1] >> 5)
				back_heat := ((r[1] & 0x1f) << 2) + (r[2] >> 6)
				seat_fan := ((r[2] & 0x3f) << 1) + r[3]>>7
				back_fan := r[3] & 0x7f
				ses.CurrentTime += rts
				fmt.Printf(">>> Got SET rts was %d\n", rts)
				gilesInsert(ses.UuidMap["wall_in_remote_time"], fmt.Sprintf("/%04x/wall_in_remote_time", serial), "Wall seconds", ses.GetTime(), float64(time.Now().UnixNano()/1000000)/1000.)
				gilesInsert(ses.UuidMap["seat_heat"], fmt.Sprintf("/%04x/seat_heat", serial), "%", ses.GetTime(), float64(seat_heat))
				gilesInsert(ses.UuidMap["back_heat"], fmt.Sprintf("/%04x/back_heat", serial), "%", ses.GetTime(), float64(back_heat))
				gilesInsert(ses.UuidMap["seat_fan"], fmt.Sprintf("/%04x/seat_fan", serial), "%", ses.GetTime(), float64(seat_fan))
				gilesInsert(ses.UuidMap["back_fan"], fmt.Sprintf("/%04x/back_fan", serial), "%", ses.GetTime(), float64(back_fan))
			case (typ & 0xf0) == 0xc0: //Battery voltage
				if !ses.HaveTime {
					fmt.Printf("Dropping BAT record, no absolute time\n")
					continue
				}
				bat_ok := (r[0] & 8) != 0
				bat_okd := 1.0
				if !bat_ok {
					bat_okd = 0.0
				}
				rts := uint32(r[0]&0x7)<<8 + uint32(r[1])
				vol := int16(uint16(r[2])<<8 + uint16(r[3]))

				volf := (float64(vol) / 32768 * 2.048) / (10000. / (10000. + 68000.))
				ses.CurrentTime += rts
				fmt.Printf(">>> Got BAT\n")
				gilesInsert(ses.UuidMap["wall_in_remote_time"], fmt.Sprintf("/%04x/wall_in_remote_time", serial), "Wall seconds", ses.GetTime(), float64(time.Now().UnixNano()/1000000)/1000.)
				gilesInsert(ses.UuidMap["battery"], fmt.Sprintf("/%04x/battery", serial), "Voltage", ses.GetTime(), volf)
				gilesInsert(ses.UuidMap["battery_ok"], fmt.Sprintf("/%04x/battery_ok", serial), "Boolean", ses.GetTime(), bat_okd)
			case (typ & 0xf0) == 0xd0:
				ver := uint8(r[0] & 0xf)
				rsts := uint8(r[1] >> 2)
				religion := (uint32(r[1]&3) << 16) + uint32(r[2])<<8 + uint32(r[3])
				religionS := fmt.Sprintf("0x%x", religion+0x50000)
				serialS := fmt.Sprintf("0x%04x", serial)
				var nodetime time.Time
				if ses.HaveTime {
					nodetime = time.Unix(0, int64(ses.GetTime())*1000000)
				} else {
					nodetime = time.Unix(0, 0)
				}
				stmt, err := db.Prepare("INSERT INTO bootrecords SET serial=?, resets=?, religion=?, version=?, nodetime=?")
				if err != nil {
					panic(err)
				}
				_, err = stmt.Exec(serialS, rsts, religionS, ver, nodetime)
				if err != nil {
					panic(err)
				}
				fmt.Printf(">>> Got BOOT\n")
				if !ses.HaveTime {
					fmt.Printf("Dropping boot record: no time\n")
					continue
				}
				gilesInsert(ses.UuidMap["wall_in_remote_time"], fmt.Sprintf("/%04x/wall_in_remote_time", serial), "Wall seconds", ses.GetTime(), float64(time.Now().UnixNano()/1000000)/1000.)
				gilesInsert(ses.UuidMap["resets"], fmt.Sprintf("/%04x/resets", serial), "Resets", ses.GetTime(), float64(rsts))
				gilesInsert(ses.UuidMap["fw_version"], fmt.Sprintf("/%04x/fw_version", serial), "Version", ses.GetTime(), float64(ver))
			default:
				fmt.Printf("WHAT KIND OF RECORD IS THIS?? %x %x %x %x\n", r[0], r[1], r[2], r[3])
				ses.CorruptRecords += 1
			}
		}
	}
	socklock.Lock()
	fmt.Printf("Releasing %d / %x\n", read_ptr, read_ptr)
	ts := time.Now().Unix()
	pkt := []byte{uint8(read_ptr), uint8(read_ptr >> 8), uint8(read_ptr >> 16),
		uint8(ts), uint8(ts >> 8), uint8(ts >> 16), uint8(ts >> 24)}
	_, err := sock.WriteToUDP(pkt, ra)
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

var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("mysql", "<fillmein>@/pecs")
	if err != nil {
		panic(err)
	}
	runtime.GOMAXPROCS(16)
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
		fmt.Printf("Got packet ADDR %+v\n", addr)
		if err != nil {
			fmt.Printf("Got error: %v\n", err)
			continue
		}
		go handlePacket(addr, buf[:ln])
	}
}
