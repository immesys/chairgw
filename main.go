package main

import (
	"fmt"
	"net"
)

func handlePacket(ra *net.UDPAddr, msg []byte) {
	fmt.Printf("Got packet from %+v\n", ra)
}
func main() {
	addr, err := net.ResolveUDPAddr("udp6", ":4040")
	if err != nil {
		panic(err)
	}
	sock, err := net.ListenUDP("udp6", addr)
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
