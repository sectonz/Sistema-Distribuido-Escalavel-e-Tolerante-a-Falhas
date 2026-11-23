package comum

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	IP            string
	Hosts         []string
	PortaBase     int
	Encoding      string
	CadastroIP    string
	CadastroPorta int
	Multicast     string
	Heartbeat     time.Duration
	Timeout       time.Duration
	MaxUsuarios   int
	WorkDir       string
}

// Le o arquivo .env procurando no diretorio atual, diretorios pais ou relativo ao executavel. Variaveis de ambiente tem prioridade.
func CarregarConfig() Config {
	candidatos := []string{".env", "../.env", "../../.env", "cadastro/.env", "../cadastro/.env"}
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		candidatos = append(candidatos,
			filepath.Join(dir, ".env"),
			filepath.Join(dir, "..", ".env"),
			filepath.Join(dir, "cadastro", ".env"),
			filepath.Join(dir, "..", "cadastro", ".env"),
		)
	}

	var env map[string]string
	for _, caminho := range candidatos {
		env = lerEnv(caminho)
		if len(env) > 0 {
			break
		}
	}
	if env == nil {
		env = map[string]string{}
	}
	get := func(chave, padrao string) string {
		if v := os.Getenv(chave); v != "" {
			return v
		}
		if v, ok := env[chave]; ok && v != "" {
			return v
		}
		return padrao
	}
	getInt := func(chave, padrao string) int {
		v, err := strconv.Atoi(get(chave, padrao))
		if err != nil {
			log.Fatalf("%s deve ser inteiro", chave)
		}
		return v
	}

	if get("SERVER_IP", "") == "" || get("SERVER_PORT", "") == "" {
		log.Fatal("SERVER_IP e SERVER_PORT sao obrigatorias no .env")
	}
	portaCadastro := get("CADASTRO_PORT", get("CONTROL_PORT", "6001"))
	ipBase := get("SERVER_IP", "")
	var hosts []string
	for _, h := range strings.Split(get("SERVER_HOSTS", ""), ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		hosts = []string{ipBase}
	}
	wd, _ := os.Getwd()
	workDir := get("REMOTE_WORKDIR", wd)

	cfg := Config{
		IP:            ipBase,
		Hosts:         hosts,
		PortaBase:     getInt("SERVER_PORT", ""),
		Encoding:      get("ENCODING", "utf-8"),
		CadastroIP:    get("CADASTRO_IP", ipBase),
		CadastroPorta: getInt("CADASTRO_PORT", portaCadastro),
		Multicast:     get("MULTICAST_ADDR", "239.67.67.67:9999"),
		Heartbeat:     time.Duration(getInt("HEARTBEAT_MS", "500")) * time.Millisecond,
		Timeout:       time.Duration(getInt("TIMEOUT_MS", "2000")) * time.Millisecond,
		MaxUsuarios:   getInt("MAX_USUARIOS", "5"),
		WorkDir:       workDir,
	}
	if strings.ToLower(cfg.Encoding) != "utf-8" {
		log.Printf("aviso: apenas utf-8 e suportado (ENCODING=%s)", cfg.Encoding)
	}
	return cfg
}

// HostServidor retorna a maquina associada ao servidor id em round-robin sobre SERVER_HOSTS.
func (c Config) HostServidor(id int) string {
	if len(c.Hosts) == 0 {
		return c.IP
	}
	idx := id % len(c.Hosts)
	if idx < 0 {
		idx = 0
	}
	return c.Hosts[idx]
}

// Cada servidor escuta em HostServidor(id):(SERVER_PORT + id).
func (c Config) EnderecoServidor(id int) string {
	return fmt.Sprintf("%s:%d", c.HostServidor(id), c.PortaBase+id)
}

// EnderecoEscutaServidor retorna o endereco local de bind TCP para o servidor id.
// Quando ha multiplas maquinas configuradas, faz bind em 0.0.0.0 para aceitar conexoes da rede e local.
func (c Config) EnderecoEscutaServidor(id int) string {
	host := c.HostServidor(id)
	if len(c.Hosts) > 1 && host != "127.0.0.1" && host != "localhost" {
		return fmt.Sprintf("0.0.0.0:%d", c.PortaBase+id)
	}
	return fmt.Sprintf("%s:%d", host, c.PortaBase+id)
}

// EhHostLocal verifica se o host informado corresponde a propria maquina atual.
func (c Config) EhHostLocal(alvo string) bool {
	if alvo == "" || alvo == "127.0.0.1" || alvo == "localhost" {
		return true
	}
	if hn, err := os.Hostname(); err == nil {
		hnCurto := strings.Split(hn, ".")[0]
		alvoCurto := strings.Split(alvo, ".")[0]
		if strings.EqualFold(hn, alvo) || strings.EqualFold(hnCurto, alvoCurto) {
			return true
		}
	}
	return false
}

func (c Config) EnderecoCadastro() string {
	return fmt.Sprintf("%s:%d", c.CadastroIP, c.CadastroPorta)
}

func lerEnv(caminho string) map[string]string {
	env := map[string]string{}
	f, err := os.Open(caminho)
	if err != nil {
		return env
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		linha := strings.TrimSpace(sc.Text())
		if linha == "" || strings.HasPrefix(linha, "#") || !strings.Contains(linha, "=") {
			continue
		}
		p := strings.SplitN(linha, "=", 2)
		env[strings.TrimSpace(p[0])] = strings.Trim(strings.TrimSpace(p[1]), `"'`)
	}
	return env
}
