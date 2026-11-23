package comum

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func interfaceMulticast() *net.Interface {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var loopback *net.Interface
	for i := range ifaces {
		ifi := &ifaces[i]
		if ifi.Flags&net.FlagUp == 0 {
			continue
		}
		if ifi.Flags&net.FlagLoopback != 0 {
			loopback = ifi
			continue
		}
		if ifi.Flags&net.FlagMulticast != 0 {
			addrs, _ := ifi.Addrs()
			if len(addrs) > 0 {
				return ifi
			}
		}
	}
	return loopback
}

// Heartbeat: HB_<ID>_<COORDENADOR>, enviado periodicamente para o grupo multicast.
func EnviarHeartbeats(grupo string, intervalo time.Duration, parar <-chan struct{}, mensagem func() string) {
	addr, err := net.ResolveUDPAddr("udp4", grupo)
	if err != nil {
		log.Fatal(err)
	}
	var laddr *net.UDPAddr
	if ifi := interfaceMulticast(); ifi != nil && ifi.Flags&net.FlagLoopback != 0 {
		laddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
	}
	conn, err := net.DialUDP("udp4", laddr, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	raw, err := conn.SyscallConn()
	if err == nil {
		raw.Control(func(fd uintptr) {
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_LOOP, 1)
		})
	}

	avisou := false
	t := time.NewTicker(intervalo)
	defer t.Stop()
	for {
		if _, err := conn.Write([]byte(mensagem())); err != nil && !avisou {
			log.Printf("erro ao enviar heartbeat: %v", err)
			avisou = true
		}
		select {
		case <-parar:
			return
		case <-t.C:
		}
	}
}

func OuvirHeartbeats(grupo string, parar <-chan struct{}, tratar func(id, coord int)) {
	addr, err := net.ResolveUDPAddr("udp4", grupo)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		conn, err = net.ListenMulticastUDP("udp4", interfaceMulticast(), addr)
		if err != nil {
			log.Fatal(err)
		}
	}
	go func() {
		<-parar
		conn.Close()
	}()
	buf := make([]byte, 256)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-parar:
				return
			default:
				continue
			}
		}
		p := strings.Split(string(buf[:n]), "_")
		if len(p) != 3 || p[0] != "HB" {
			continue
		}
		id, err1 := strconv.Atoi(p[1])
		coord, err2 := strconv.Atoi(p[2])
		if err1 == nil && err2 == nil {
			tratar(id, coord)
		}
	}
}

func MensagemHeartbeat(id, coord int) string {
	return fmt.Sprintf("HB_%d_%d", id, coord)
}
