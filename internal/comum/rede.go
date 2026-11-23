package comum

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

func Requisitar(endereco, linha string, timeout time.Duration) (string, error) {
	conn, err := net.DialTimeout("tcp", endereco, timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte(linha + "\n")); err != nil {
		return "", err
	}
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && resp == "" {
		return "", err
	}
	return strings.TrimSpace(resp), nil
}

func Servir(ln net.Listener, tratar func(string) string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				linha, err := r.ReadString('\n')
				linha = strings.TrimSpace(linha)
				if linha != "" {
					if _, errW := c.Write([]byte(tratar(linha) + "\n")); errW != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}(conn)
	}
}

// DescobrirCoordenador procura qual servidor ativo responde pelo coordenador atual.
func DescobrirCoordenador(cfg Config, maxID int) (int, string, bool) {
	for id := 0; id <= maxID; id++ {
		end := cfg.EnderecoServidor(id)
		resp, err := Requisitar(end, "WHOISCOORD", 400*time.Millisecond)
		if err != nil {
			continue
		}
		// Formato: COORD_<ID>_<IP>_<PORT>
		p := strings.Split(resp, "_")
		if len(p) == 4 && p[0] == "COORD" {
			var cid int
			fmt.Sscanf(p[1], "%d", &cid)
			return cid, fmt.Sprintf("%s:%s", p[2], p[3]), true
		}
	}
	return 0, cfg.EnderecoServidor(0), false
}

// EnviarAoCoordenador envia um comando ao coordenador atual, atualizando o endereco se houver eleicao.
func EnviarAoCoordenador(cfg Config, endCoord *string, linha string, timeout time.Duration) (string, error) {
	if *endCoord == "" {
		*endCoord = cfg.EnderecoServidor(0)
	}
	resp, err := Requisitar(*endCoord, linha, timeout)
	if err == nil && strings.HasPrefix(resp, "REDIRECT_") {
		p := strings.Split(resp, "_")
		if len(p) == 3 {
			*endCoord = fmt.Sprintf("%s:%s", p[1], p[2])
			resp, err = Requisitar(*endCoord, linha, timeout)
		}
	}
	if err != nil {
		if _, novoEnd, ok := DescobrirCoordenador(cfg, 20); ok {
			*endCoord = novoEnd
			resp, err = Requisitar(*endCoord, linha, timeout)
		}
	}
	return resp, err
}
