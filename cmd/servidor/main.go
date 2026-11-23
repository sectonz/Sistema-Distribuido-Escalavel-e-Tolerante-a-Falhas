package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"sd/internal/comum"
)

// Funcao receive_msg(m) definida na secao III.B do enunciado:
// retorna False se m = None; True caso contrario, apos 1 segundo.
func receive_msg(m *string) bool {
	if m == nil {
		return false
	}
	time.Sleep(1 * time.Second)
	return true
}

type Servidor struct {
	id      int
	maxID   int
	cfg     comum.Config
	log     *comum.Logger
	arqEst  string
	parar   chan struct{}

	mu          sync.Mutex
	coord       int
	pronto      bool
	emEleicao   bool
	estado      comum.Estado
	vivos       map[int]time.Time
	tsKill      map[int]time.Time
	emCurso      map[string]chan struct{}
	contExecMsg  map[string]int
	hostsProntos map[string]bool
}

func main() {
	id := flag.Int("id", 0, "ID do servidor")
	worker := flag.Bool("worker", false, "Executa diretamente o processo trabalhador")
	retorno := flag.Bool("retorno", false, "Indica retorno apos falha (aguarda sincronizacao)")
	flag.Parse()

	if !*worker {
		supervisionar(*id)
		return
	}

	cfg := comum.CarregarConfig()
	s := &Servidor{
		id:           *id,
		maxID:        *id,
		cfg:          cfg,
		log:          comum.NovoLogger(fmt.Sprintf("logs/servidor_%d.json", *id), true),
		arqEst:       fmt.Sprintf("dados/estado_servidor_%d.json", *id),
		parar:        make(chan struct{}),
		coord:        0,
		estado:       comum.NovoEstado(0),
		vivos:        map[int]time.Time{*id: time.Now()},
		tsKill:       map[int]time.Time{},
		emCurso:      map[string]chan struct{}{},
		contExecMsg:  map[string]int{},
		hostsProntos: map[string]bool{},
	}

	end := cfg.EnderecoServidor(*id)
	ln, err := net.Listen("tcp", cfg.EnderecoEscutaServidor(*id))
	if err != nil {
		log.Printf("[servidor %d] falha ao abrir %s: %v", *id, end, err)
		os.Exit(2)
	}
	go comum.Servir(ln, s.tratarComando)

	if *retorno {
		s.sincronizarNoRetorno()
	} else if *id == 0 {
		if cid, endCoord, ok := comum.DescobrirCoordenador(cfg, 15); ok && cid != 0 {
			_ = endCoord
			s.sincronizarNoRetorno()
		} else {
			s.mu.Lock()
			s.coord = 0
			s.pronto = true
			s.estado.Coordenador = 0
			s.estado.Servidores[0] = end
			_ = comum.SalvarEstado(s.arqEst, s.estado)
			s.mu.Unlock()
			s.log.Registrar("coordenador", s.id, "inicio_coordenador", 1, "endereco", end)
			s.garantirServidoresParaUsuarios()
		}
	} else {
		s.mu.Lock()
		s.pronto = true
		s.mu.Unlock()
		s.log.Registrar("servidor", s.id, "inicio_servidor", 1, "endereco", end)
	}

	go comum.OuvirHeartbeats(cfg.Multicast, s.parar, s.receberHeartbeat)
	go comum.EnviarHeartbeats(cfg.Multicast, cfg.Heartbeat, s.parar, func() string {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.pronto {
			return ""
		}
		return comum.MensagemHeartbeat(s.id, s.coord)
	})
	go s.monitorarFalhas()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(s.parar)
	ln.Close()
}

// Supervisiona o processo do servidor: se encerrar com erro (ex.: KILL), aguarda 5s e relanca com -retorno.
// Se encerrar com codigo 0 (SHUTDOWN de servidor ocioso ou Ctrl+C), finaliza definitivamente.
func supervisionar(id int) {
	exe, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	retorno := false
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		args := []string{"-id", strconv.Itoa(id), "-worker"}
		if retorno {
			args = append(args, "-retorno")
		}
		cmd := exec.Command(exe, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			log.Fatalf("[supervisor %d] erro ao iniciar: %v", id, err)
		}

		fim := make(chan error, 1)
		go func() { fim <- cmd.Wait() }()

		select {
		case <-sigCh:
			_ = cmd.Process.Signal(syscall.SIGTERM)
			<-fim
			return
		case err := <-fim:
			if err == nil {
				return
			}
			if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 2 {
				// Codigo 2 indica porta ja ocupada por outro processo; encerra supervisor sem loop
				return
			}
			fmt.Printf("[supervisor %d] servidor falhou; aguardando 5s para retornar...\n", id)
			select {
			case <-sigCh:
				return
			case <-time.After(5 * time.Second):
				retorno = true
			}
		}
	}
}

func (s *Servidor) sincronizarNoRetorno() {
	inicio := time.Now()
	cid, endCoord, ok := comum.DescobrirCoordenador(s.cfg, 20)
	if ok && cid != s.id {
		resp, err := comum.Requisitar(endCoord, fmt.Sprintf("SYNC_%d", s.id), s.cfg.Timeout)
		if err == nil && strings.HasPrefix(resp, "SYNC_OK_") {
			var est comum.Estado
			if json.Unmarshal([]byte(strings.TrimPrefix(resp, "SYNC_OK_")), &est) == nil {
				s.mu.Lock()
				s.estado = est.Clonar()
				s.coord = est.Coordenador
				s.pronto = true
				for id := range est.Servidores {
					s.vivos[id] = time.Now()
				}
				_ = comum.SalvarEstado(s.arqEst, s.estado)
				s.mu.Unlock()
				s.log.Registrar("servidor", s.id, "tempo_sincronizacao_retorno", comum.Seg(time.Since(inicio)), "coordenador", cid)
				return
			}
		}
	}

	// Caso nenhum outro coordenador esteja ativo, restaura o estado persistido em disco.
	est, err := comum.CarregarEstado(s.arqEst)
	s.mu.Lock()
	if err == nil {
		s.estado = est
	}
	s.coord = s.id
	s.pronto = true
	s.estado.Coordenador = s.id
	s.estado.Servidores = map[int]string{s.id: s.cfg.EnderecoServidor(s.id)}
	_ = comum.SalvarEstado(s.arqEst, s.estado)
	s.mu.Unlock()
	s.log.Registrar("servidor", s.id, "tempo_sincronizacao_retorno", comum.Seg(time.Since(inicio)), "coordenador", s.id)
	s.notificarNovoCoordenadorCadastro()
	s.garantirServidoresParaUsuarios()
}

func (s *Servidor) receberHeartbeat(id, coord int) {
	s.mu.Lock()
	if !s.pronto {
		s.mu.Unlock()
		return
	}
	_, jaVivo := s.vivos[id]
	s.vivos[id] = time.Now()
	if s.estado.Servidores == nil {
		s.estado.Servidores = map[int]string{}
	}
	s.estado.Servidores[id] = s.cfg.EnderecoServidor(id)

	// Impede multiplos coordenadores simultaneos (desempate pelo maior ID do Bully).
	conflitoCoord := false
	if id != s.id && coord == id && s.coord == s.id {
		if id > s.id {
			s.coord = id
			s.estado.Coordenador = id
		} else {
			conflitoCoord = true
		}
	} else if _, coordVivo := s.vivos[s.coord]; !coordVivo && coord >= 0 {
		s.coord = coord
		s.estado.Coordenador = coord
	}
	souCoord := s.coord == s.id
	if !jaVivo && id != s.id && souCoord {
		if id > s.maxID {
			s.maxID = id
		}
		s.estado.Versao++
		s.estado.TsEnvio = comum.Agora()
		_ = comum.SalvarEstado(s.arqEst, s.estado)
	}
	s.mu.Unlock()

	if conflitoCoord {
		go s.iniciarEleicao(0)
	}
	if !jaVivo && id != s.id && souCoord {
		s.log.Registrar("coordenador", s.id, "servidor_detectado", float64(id))
		go func(novoID int) {
			_, _ = comum.Requisitar(s.cfg.EnderecoCadastro(), fmt.Sprintf("NEWID_%d", novoID), s.cfg.Timeout)
			s.replicarEstado()
			s.emitirMetricasDistribuicao()
		}(id)
	}
}

func (s *Servidor) monitorarFalhas() {
	t := time.NewTicker(s.cfg.Heartbeat / 2)
	defer t.Stop()
	for {
		select {
		case <-s.parar:
			return
		case <-t.C:
			s.verificarTimeouts()
		}
	}
}

func (s *Servidor) verificarTimeouts() {
	s.mu.Lock()
	if !s.pronto {
		s.mu.Unlock()
		return
	}
	agora := time.Now()
	var falhos []int
	for id, ultimo := range s.vivos {
		if id == s.id {
			s.vivos[id] = agora
			continue
		}
		if agora.Sub(ultimo) > s.cfg.Timeout {
			falhos = append(falhos, id)
		}
	}
	if len(falhos) == 0 {
		s.mu.Unlock()
		return
	}

	coordFalhou := false
	souCoord := s.coord == s.id
	temposDeteccao := map[int]time.Duration{}
	for _, id := range falhos {
		inicio := s.vivos[id]
		if tk, ok := s.tsKill[id]; ok {
			inicio = tk
			delete(s.tsKill, id)
		}
		temposDeteccao[id] = agora.Sub(inicio)
		delete(s.vivos, id)
		delete(s.estado.Servidores, id)
		if id == s.coord {
			coordFalhou = true
		}
	}
	s.mu.Unlock()

	if souCoord {
		for _, id := range falhos {
			s.log.Registrar("coordenador", s.id, "tempo_deteccao_falha", comum.Seg(temposDeteccao[id]), "servidor_falho", id)
			s.redistribuirUsuariosDeFalho(id)
		}
	} else if coordFalhou {
		s.iniciarEleicao(temposDeteccao[s.coord])
	}
}

// Algoritmo de eleicao Bully: desafia servidores ativos com ID maior.
func (s *Servidor) iniciarEleicao(tempoDeteccao time.Duration) {
	s.mu.Lock()
	if s.emEleicao || !s.pronto {
		s.mu.Unlock()
		return
	}
	s.emEleicao = true
	var maiores []int
	for id := range s.vivos {
		if id > s.id {
			maiores = append(maiores, id)
		}
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.emEleicao = false
		s.mu.Unlock()
	}()

	alguemRespondeu := false
	for _, idMaior := range maiores {
		resp, err := comum.Requisitar(s.cfg.EnderecoServidor(idMaior), fmt.Sprintf("ELECTION_%d", s.id), 600*time.Millisecond)
		if err == nil && resp == "OK" {
			alguemRespondeu = true
		}
	}

	if alguemRespondeu {
		time.Sleep(800 * time.Millisecond)
		s.mu.Lock()
		_, coordVivo := s.vivos[s.coord]
		s.mu.Unlock()
		if coordVivo {
			return
		}
	}

	s.assumirCoordenacao(tempoDeteccao)
}

func (s *Servidor) assumirCoordenacao(tempoDeteccao time.Duration) {
	s.mu.Lock()
	antigoCoord := s.coord
	s.coord = s.id
	s.estado.Coordenador = s.id
	s.estado.Versao++
	delete(s.vivos, antigoCoord)
	delete(s.estado.Servidores, antigoCoord)
	s.estado.Servidores[s.id] = s.cfg.EnderecoServidor(s.id)

	var pares []int
	for id := range s.vivos {
		if id != s.id {
			pares = append(pares, id)
		}
	}
	_ = comum.SalvarEstado(s.arqEst, s.estado)
	s.mu.Unlock()

	if tempoDeteccao > 0 {
		s.log.Registrar("coordenador", s.id, "tempo_deteccao_falha", comum.Seg(tempoDeteccao), "servidor_falho", antigoCoord)
	}
	s.log.Registrar("coordenador", s.id, "eleito_coordenador", float64(s.id), "anterior", antigoCoord)

	for _, id := range pares {
		_, _ = comum.Requisitar(s.cfg.EnderecoServidor(id), fmt.Sprintf("COORDINATOR_%d", s.id), 600*time.Millisecond)
	}
	s.notificarNovoCoordenadorCadastro()
	if antigoCoord != s.id {
		s.redistribuirUsuariosDeFalho(antigoCoord)
	}
	s.garantirServidoresParaUsuarios()
}

func (s *Servidor) notificarNovoCoordenadorCadastro() {
	porta := s.cfg.PortaBase + s.id
	cmd := fmt.Sprintf("NEWCOORDINATORIP_%s_%d", s.cfg.HostServidor(s.id), porta)
	_, _ = comum.Requisitar(s.cfg.EnderecoCadastro(), cmd, s.cfg.Timeout)
}

func (s *Servidor) redistribuirUsuariosDeFalho(idFalho int) {
	inicio := time.Now()
	s.mu.Lock()
	delete(s.vivos, idFalho)
	delete(s.estado.Servidores, idFalho)

	var orfaos []int
	for uid, srv := range s.estado.Alocacao {
		if srv == idFalho {
			orfaos = append(orfaos, uid)
		} else if _, ok := s.estado.Servidores[srv]; !ok {
			orfaos = append(orfaos, uid)
		}
	}
	sort.Ints(orfaos)
	s.mu.Unlock()

	migrados := 0
	for _, uid := range orfaos {
		s.alocarUsuario(uid, false)
		migrados++
	}

	s.mu.Lock()
	s.estado.Versao++
	_ = comum.SalvarEstado(s.arqEst, s.estado)
	s.mu.Unlock()

	s.replicarEstado()
	s.log.Registrar("coordenador", s.id, "tempo_redistribuicao_usuarios", comum.Seg(time.Since(inicio)), "servidor_falho", idFalho, "migrados", migrados)
	s.log.Registrar("coordenador", s.id, "usuarios_migrados", float64(migrados), "evento", "falha", "servidor_falho", idFalho)
	s.emitirMetricasDistribuicao()
}

func (s *Servidor) alocarUsuario(uid int, jaComLock bool) int {
	if !jaComLock {
		s.mu.Lock()
	}
	contagem := map[int]int{}
	for id := range s.estado.Servidores {
		contagem[id] = 0
	}
	for u, srv := range s.estado.Alocacao {
		if u == uid {
			continue
		}
		if _, ativo := contagem[srv]; ativo {
			contagem[srv]++
		}
	}

	escolhido := -1
	menorCarga := s.cfg.MaxUsuarios + 1
	var ids []int
	for id := range contagem {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		if contagem[id] < s.cfg.MaxUsuarios && contagem[id] < menorCarga {
			menorCarga = contagem[id]
			escolhido = id
		}
	}

	if escolhido == -1 {
		if !jaComLock {
			s.mu.Unlock()
		}
		novoID := s.criarNovoServidor()
		if !jaComLock {
			s.mu.Lock()
		}
		escolhido = novoID
	}

	s.estado.Alocacao[uid] = escolhido
	if !jaComLock {
		s.mu.Unlock()
	}
	return escolhido
}

func (s *Servidor) proximoIDServidor() int {
	max := s.id
	if s.maxID > max {
		max = s.maxID
	}
	for id := range s.estado.Servidores {
		if id > max {
			max = id
		}
	}
	for _, id := range s.estado.Alocacao {
		if id > max {
			max = id
		}
	}
	s.maxID = max + 1
	return s.maxID
}

func (s *Servidor) garantirServidoresParaUsuarios() {
	s.mu.Lock()
	if s.coord != s.id {
		s.mu.Unlock()
		return
	}
	var orfaos []int
	for uid, srv := range s.estado.Alocacao {
		if _, ativo := s.estado.Servidores[srv]; !ativo {
			orfaos = append(orfaos, uid)
		}
	}
	for uid := range s.estado.Usuarios {
		if _, alocado := s.estado.Alocacao[uid]; !alocado {
			orfaos = append(orfaos, uid)
		}
	}
	sort.Ints(orfaos)
	s.mu.Unlock()

	if len(orfaos) == 0 {
		return
	}

	migrados := 0
	for _, uid := range orfaos {
		s.alocarUsuario(uid, false)
		migrados++
	}

	s.mu.Lock()
	s.estado.Versao++
	_ = comum.SalvarEstado(s.arqEst, s.estado)
	s.mu.Unlock()

	s.replicarEstado()
	s.log.Registrar("coordenador", s.id, "usuarios_migrados", float64(migrados), "evento", "recuperacao_geral")
	s.emitirMetricasDistribuicao()
}

func (s *Servidor) criarNovoServidor() int {
	inicio := time.Now()
	s.mu.Lock()
	novoID := s.proximoIDServidor()
	hostAlvo := s.cfg.HostServidor(novoID)
	end := s.cfg.EnderecoServidor(novoID)
	s.estado.Servidores[novoID] = end
	s.vivos[novoID] = time.Now()
	s.mu.Unlock()

	exe, err := os.Executable()
	if err == nil {
		if s.cfg.EhHostLocal(hostAlvo) {
			cmd := exec.Command(exe, "-id", strconv.Itoa(novoID))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			_ = cmd.Start()
		} else {
			s.iniciarServidorRemotoSSH(hostAlvo, exe, novoID)
		}
	}

	prazo := time.Now().Add(5 * time.Second)
	for time.Now().Before(prazo) {
		if resp, err := comum.Requisitar(end, "PING", 200*time.Millisecond); err == nil && strings.HasPrefix(resp, "PONG_") {
			_, _ = comum.Requisitar(end, fmt.Sprintf("COORDINATOR_%d", s.id), 300*time.Millisecond)
			break
		}
		time.Sleep(80 * time.Millisecond)
	}

	s.log.Registrar("coordenador", s.id, "tempo_novo_servidor_operante", comum.Seg(time.Since(inicio)), "novo_servidor", novoID, "host", hostAlvo)
	_, _ = comum.Requisitar(s.cfg.EnderecoCadastro(), fmt.Sprintf("NEWID_%d", novoID), s.cfg.Timeout)
	return novoID
}

// Prepara a maquina remota (copia binario e .env se necessario) e inicia o servidor via SSH.
func (s *Servidor) iniciarServidorRemotoSSH(host, exe string, novoID int) {
	workDir := s.cfg.WorkDir
	if workDir == "" {
		workDir = filepath.Dir(filepath.Dir(exe))
	}
	binRemoto := filepath.Join(workDir, "bin", "servidor")

	s.mu.Lock()
	jaPreparado := s.hostsProntos[host]
	s.mu.Unlock()

	sshOpts := []string{"-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=3"}

	if !jaPreparado {
		mkdirCmd := exec.Command("ssh", append(sshOpts, host, fmt.Sprintf("mkdir -p %q/bin %q/logs %q/dados", workDir, workDir, workDir))...)
		if err := mkdirCmd.Run(); err != nil {
			log.Printf("[coordenador %d] aviso: falha ao criar diretorios em %s via ssh: %v", s.id, host, err)
		}

		scpBin := exec.Command("scp", append(sshOpts, exe, fmt.Sprintf("%s:%s", host, binRemoto))...)
		if err := scpBin.Run(); err != nil {
			log.Printf("[coordenador %d] aviso: falha ao copiar binario para %s via scp: %v", s.id, host, err)
		}

		arqEnv := filepath.Join(workDir, ".env")
		if _, err := os.Stat(arqEnv); err == nil {
			scpEnv := exec.Command("scp", append(sshOpts, arqEnv, fmt.Sprintf("%s:%s", host, arqEnv))...)
			_ = scpEnv.Run()
		}

		s.mu.Lock()
		s.hostsProntos[host] = true
		s.mu.Unlock()
	}

	cmdRemoto := fmt.Sprintf("cd %q && nohup %q -id %d >> %q/logs/ssh_servidor_%d.out 2>&1 < /dev/null &",
		workDir, binRemoto, novoID, workDir, novoID)
	sshRun := exec.Command("ssh", append(sshOpts, host, cmdRemoto)...)
	if err := sshRun.Start(); err != nil {
		log.Printf("[coordenador %d] erro ao iniciar servidor %d em %s via ssh: %v", s.id, novoID, host, err)
	} else {
		go func() { _ = sshRun.Wait() }()
	}
}

func (s *Servidor) verificarServidoresOciosos() {
	time.Sleep(250 * time.Millisecond)
	s.mu.Lock()
	if s.coord != s.id {
		s.mu.Unlock()
		return
	}
	numUsuarios := len(s.estado.Usuarios)
	necessarios := int(math.Ceil(float64(numUsuarios) / float64(s.cfg.MaxUsuarios)))
	if necessarios < 1 {
		necessarios = 1
	}
	if len(s.estado.Servidores) <= necessarios {
		s.mu.Unlock()
		return
	}

	contagem := map[int]int{}
	for id := range s.estado.Servidores {
		contagem[id] = 0
	}
	for _, srv := range s.estado.Alocacao {
		contagem[srv]++
	}

	var ociosos []int
	for id, c := range contagem {
		if id != s.id && c == 0 {
			ociosos = append(ociosos, id)
		}
	}
	sort.Ints(ociosos)
	for _, idOcioso := range ociosos {
		if len(s.estado.Servidores) <= necessarios {
			break
		}
		delete(s.estado.Servidores, idOcioso)
		delete(s.vivos, idOcioso)
		s.estado.Versao++
		_ = comum.SalvarEstado(s.arqEst, s.estado)
		s.mu.Unlock()

		inicio := time.Now()
		_, _ = comum.Requisitar(s.cfg.EnderecoServidor(idOcioso), "SHUTDOWN", s.cfg.Timeout)
		s.log.Registrar("coordenador", s.id, "tempo_desativar_servidor_ocioso", comum.Seg(time.Since(inicio)), "servidor", idOcioso)
		s.replicarEstado()
		s.emitirMetricasDistribuicao()
		s.mu.Lock()
	}
	s.mu.Unlock()
}

func (s *Servidor) replicarEstado() {
	s.mu.Lock()
	s.estado.TsEnvio = comum.Agora()
	copia := s.estado.Clonar()
	var destinos []int
	for id := range s.estado.Servidores {
		if id != s.id {
			destinos = append(destinos, id)
		}
	}
	s.mu.Unlock()

	b, err := json.Marshal(copia)
	if err != nil {
		return
	}
	linha := "REPL_" + string(b)
	var wg sync.WaitGroup
	for _, id := range destinos {
		wg.Add(1)
		go func(dest int) {
			defer wg.Done()
			_, _ = comum.Requisitar(s.cfg.EnderecoServidor(dest), linha, s.cfg.Timeout)
		}(id)
	}
	wg.Wait()
}

func (s *Servidor) emitirMetricasDistribuicao() {
	s.mu.Lock()
	numServ := len(s.estado.Servidores)
	if numServ == 0 {
		s.mu.Unlock()
		return
	}
	contagem := map[int]int{}
	for id := range s.estado.Servidores {
		contagem[id] = 0
	}
	semServidor := 0
	for _, srv := range s.estado.Alocacao {
		if _, ativo := contagem[srv]; ativo {
			contagem[srv]++
		} else {
			semServidor++
		}
	}
	numUsuarios := len(s.estado.Usuarios)
	s.mu.Unlock()

	minC, maxC := math.MaxInt32, 0
	soma := 0.0
	for _, c := range contagem {
		if c < minC {
			minC = c
		}
		if c > maxC {
			maxC = c
		}
		soma += float64(c)
	}
	media := soma / float64(numServ)
	somaQuad := 0.0
	for _, c := range contagem {
		d := float64(c) - media
		somaQuad += d * d
	}
	desvio := math.Round(math.Sqrt(somaQuad/float64(numServ))*1000) / 1000

	necessarios := math.Ceil(float64(numUsuarios) / float64(s.cfg.MaxUsuarios))
	if necessarios < 1 {
		necessarios = 1
	}
	razao := math.Round((float64(numServ)/necessarios)*1000) / 1000

	s.log.Registrar("coordenador", s.id, "desbalanceamento", float64(maxC-minC))
	s.log.Registrar("coordenador", s.id, "desvio_padrao_usuarios", desvio)
	s.log.Registrar("coordenador", s.id, "servidores_ativos_por_necessarios", razao, "ativos", numServ, "usuarios", numUsuarios)
	s.log.Registrar("coordenador", s.id, "usuarios_sem_servidor", float64(semServidor))
}

func (s *Servidor) tratarComando(linha string) string {
	switch {
	case linha == "PING":
		s.mu.Lock()
		defer s.mu.Unlock()
		return fmt.Sprintf("PONG_%d_%d", s.id, s.coord)

	case linha == "WHOISCOORD":
		s.mu.Lock()
		pronto := s.pronto
		cid := s.coord
		s.mu.Unlock()
		if !pronto {
			return "ERROR_NOT_READY"
		}
		return fmt.Sprintf("COORD_%d_%s_%d", cid, s.cfg.HostServidor(cid), s.cfg.PortaBase+cid)

	case linha == "STATUS":
		s.mu.Lock()
		b, _ := json.Marshal(s.estado)
		s.mu.Unlock()
		return string(b)

	case strings.HasPrefix(linha, "ELECTION_"):
		s.mu.Lock()
		pronto := s.pronto
		s.mu.Unlock()
		if !pronto {
			return "ERROR_NOT_READY"
		}
		idOrigem, _ := strconv.Atoi(strings.TrimPrefix(linha, "ELECTION_"))
		if idOrigem < s.id {
			go s.iniciarEleicao(0)
		}
		return "OK"

	case strings.HasPrefix(linha, "COORDINATOR_"):
		novoCoord, err := strconv.Atoi(strings.TrimPrefix(linha, "COORDINATOR_"))
		if err != nil {
			return "ERROR_INVALID_COMMAND"
		}
		s.mu.Lock()
		s.coord = novoCoord
		s.estado.Coordenador = novoCoord
		s.vivos[novoCoord] = time.Now()
		s.estado.Servidores[novoCoord] = s.cfg.EnderecoServidor(novoCoord)
		s.mu.Unlock()
		return "ACK_COORDINATOR"

	case strings.HasPrefix(linha, "REPL_"):
		raw := strings.TrimPrefix(linha, "REPL_")
		var snap comum.Estado
		if err := json.Unmarshal([]byte(raw), &snap); err != nil {
			return "ERROR_INVALID_JSON"
		}
		s.mu.Lock()
		perdidas := 0
		for idMsg := range s.estado.Processadas {
			if !snap.Processadas[idMsg] {
				perdidas++
				snap.Processadas[idMsg] = true
			}
		}
		if snap.Versao >= s.estado.Versao {
			s.estado = snap.Clonar()
			s.coord = snap.Coordenador
			s.pronto = true
			_ = comum.SalvarEstado(s.arqEst, s.estado)
		}
		s.mu.Unlock()

		atraso := comum.Agora() - snap.TsEnvio
		if atraso < 0 {
			atraso = 0
		}
		atraso = math.Round(atraso*1000) / 1000
		s.log.Registrar("servidor", s.id, "atraso_replicacao", atraso, "versao", snap.Versao)
		s.log.Registrar("servidor", s.id, "mensagens_confirmadas_perdidas", float64(perdidas))
		return "ACK_REPL"

	case strings.HasPrefix(linha, "PROCESSED_"):
		msgID := strings.TrimPrefix(linha, "PROCESSED_")
		s.mu.Lock()
		s.estado.Processadas[msgID] = true
		s.estado.Versao++
		_ = comum.SalvarEstado(s.arqEst, s.estado)
		s.mu.Unlock()
		go s.replicarEstado()
		return "ACK_PROCESSED"

	case strings.HasPrefix(linha, "DELIVER_"):
		msg, err := comum.ParseMsg(strings.TrimPrefix(linha, "DELIVER_"))
		if err != nil {
			return "ERROR_INVALID_MESSAGE"
		}
		return s.executarReceiveMsg(msg)

	case linha == "DIE":
		go func() {
			time.Sleep(50 * time.Millisecond)
			os.Exit(1)
		}()
		return "ACK_DIE"

	case linha == "SHUTDOWN":
		go func() {
			time.Sleep(50 * time.Millisecond)
			os.Exit(0)
		}()
		return "ACK_SHUTDOWN"

	case strings.HasPrefix(linha, "SYNC_"):
		if s.coord != s.id {
			return s.redirecionar()
		}
		idRet, err := strconv.Atoi(strings.TrimPrefix(linha, "SYNC_"))
		if err != nil {
			return "ERROR_INVALID_COMMAND"
		}
		s.mu.Lock()
		if idRet > s.maxID {
			s.maxID = idRet
		}
		s.vivos[idRet] = time.Now()
		s.estado.Servidores[idRet] = s.cfg.EnderecoServidor(idRet)
		s.estado.Versao++
		s.estado.TsEnvio = comum.Agora()
		_ = comum.SalvarEstado(s.arqEst, s.estado)
		b, _ := json.Marshal(s.estado)
		s.mu.Unlock()

		go func() {
			_, _ = comum.Requisitar(s.cfg.EnderecoCadastro(), fmt.Sprintf("NEWID_%d", idRet), s.cfg.Timeout)
			s.replicarEstado()
			s.emitirMetricasDistribuicao()
			s.verificarServidoresOciosos()
		}()
		return "SYNC_OK_" + string(b)

	case strings.HasPrefix(linha, "NEWUSER_"):
		if s.coord != s.id {
			return s.redirecionar()
		}
		return s.tratarNewUser(linha)

	case strings.HasPrefix(linha, "DELUSER_"):
		if s.coord != s.id {
			return s.redirecionar()
		}
		return s.tratarDelUser(linha)

	case strings.HasPrefix(linha, "KILL_"):
		if s.coord != s.id {
			return s.redirecionar()
		}
		return s.tratarKill(linha)

	case strings.HasPrefix(linha, "MSG_"):
		if s.coord != s.id {
			return s.redirecionar()
		}
		return s.tratarMensagemUsuario(linha)
	}

	return "ERROR_INVALID_COMMAND"
}

func (s *Servidor) redirecionar() string {
	s.mu.Lock()
	cid := s.coord
	s.mu.Unlock()
	return fmt.Sprintf("REDIRECT_%s_%d", s.cfg.HostServidor(cid), s.cfg.PortaBase+cid)
}

func (s *Servidor) tratarNewUser(linha string) string {
	p := strings.SplitN(linha, "_", 3)
	if len(p) < 2 {
		return "ERROR_INVALID_COMMAND"
	}
	uid, err := strconv.Atoi(p[1])
	if err != nil || uid <= 0 {
		return "ERROR_INVALID_USER_ID"
	}
	nome := fmt.Sprintf("Usuario %d", uid)
	if len(p) == 3 && strings.TrimSpace(p[2]) != "" {
		nome = strings.TrimSpace(p[2])
	}

	s.mu.Lock()
	_, jaExiste := s.estado.Usuarios[uid]
	srvAntigo, temSrv := s.estado.Alocacao[uid]
	_, srvAtivo := s.estado.Servidores[srvAntigo]
	s.mu.Unlock()

	if !jaExiste || !temSrv || !srvAtivo {
		srv := s.alocarUsuario(uid, false)
		s.mu.Lock()
		s.estado.Usuarios[uid] = nome
		s.estado.Versao++
		_ = comum.SalvarEstado(s.arqEst, s.estado)
		s.mu.Unlock()

		s.replicarEstado()
		s.log.Registrar("coordenador", s.id, "usuarios_migrados", 0, "evento", "criacao", "usuario", uid, "servidor", srv)
		s.emitirMetricasDistribuicao()
	}
	return fmt.Sprintf("ACK_NEWUSER_%d", uid)
}

func (s *Servidor) tratarDelUser(linha string) string {
	uid, err := strconv.Atoi(strings.TrimPrefix(linha, "DELUSER_"))
	if err != nil {
		return "ERROR_INVALID_USER_ID"
	}
	s.mu.Lock()
	delete(s.estado.Usuarios, uid)
	delete(s.estado.Alocacao, uid)
	s.estado.Versao++
	_ = comum.SalvarEstado(s.arqEst, s.estado)
	s.mu.Unlock()

	s.replicarEstado()
	s.log.Registrar("coordenador", s.id, "usuarios_migrados", 0, "evento", "remocao", "usuario", uid)
	s.emitirMetricasDistribuicao()
	go s.verificarServidoresOciosos()
	return fmt.Sprintf("ACK_DELUSER_%d", uid)
}

func (s *Servidor) tratarKill(linha string) string {
	idAlvo, err := strconv.Atoi(strings.TrimPrefix(linha, "KILL_"))
	if err != nil {
		return "ERROR_INVALID_SERVER_ID"
	}
	s.mu.Lock()
	s.tsKill[idAlvo] = time.Now()
	s.mu.Unlock()

	if idAlvo == s.id {
		go func() {
			time.Sleep(50 * time.Millisecond)
			os.Exit(1)
		}()
		return fmt.Sprintf("ACK_KILL_%d", idAlvo)
	}

	_, err = comum.Requisitar(s.cfg.EnderecoServidor(idAlvo), "DIE", 500*time.Millisecond)
	if err != nil {
		return "ERROR_SERVER_NOT_FOUND"
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.redistribuirUsuariosDeFalho(idAlvo)
	}()
	return fmt.Sprintf("ACK_KILL_%d", idAlvo)
}

func (s *Servidor) tratarMensagemUsuario(linha string) string {
	msg, err := comum.ParseMsg(linha)
	if err != nil {
		return "ERROR_INVALID_MESSAGE"
	}

	s.mu.Lock()
	if s.estado.Processadas[msg.ID] {
		s.mu.Unlock()
		s.log.Registrar("servidor", s.id, "acks_reenviados", 1, "msg_id", msg.ID)
		s.log.Registrar("servidor", s.id, "execucoes_duplicadas_receive_msg", 0, "msg_id", msg.ID)
		return "ACK_" + msg.ID
	}
	srvDest, existeDest := s.estado.Alocacao[msg.Destino]
	s.mu.Unlock()

	if !existeDest {
		return "ERROR_DEST_NOT_FOUND"
	}

	// Encaminha ao servidor responsavel pelo destinatario; se falhar durante o envio,
	// redistribui os usuarios do servidor falho e reenvia ao novo servidor responsavel.
	for tentativa := 1; tentativa <= 2; tentativa++ {
		if srvDest == s.id {
			return s.executarReceiveMsg(msg)
		}
		resp, err := comum.Requisitar(s.cfg.EnderecoServidor(srvDest), "DELIVER_"+linha, s.cfg.Timeout+1500*time.Millisecond)
		if err == nil && strings.HasPrefix(resp, "ACK_") {
			if tentativa > 1 {
				s.log.Registrar("servidor", s.id, "mensagens_perdidas_falha", 0, "msg_id", msg.ID)
			}
			return resp
		}
		s.log.Registrar("coordenador", s.id, "tempo_deteccao_falha", comum.Seg(s.cfg.Heartbeat), "servidor_falho", srvDest)
		s.redistribuirUsuariosDeFalho(srvDest)
		s.mu.Lock()
		srvDest = s.estado.Alocacao[msg.Destino]
		s.mu.Unlock()
	}

	s.log.Registrar("servidor", s.id, "mensagens_perdidas_falha", 1, "msg_id", msg.ID)
	return "ERROR_DELIVERY_FAILED"
}

func (s *Servidor) executarReceiveMsg(msg *comum.Msg) string {
	s.mu.Lock()
	for {
		if s.estado.Processadas[msg.ID] {
			dups := s.contExecMsg[msg.ID] - 1
			if dups < 0 {
				dups = 0
			}
			s.mu.Unlock()
			s.log.Registrar("servidor", s.id, "acks_reenviados", 1, "msg_id", msg.ID)
			s.log.Registrar("servidor", s.id, "execucoes_duplicadas_receive_msg", float64(dups), "msg_id", msg.ID)
			return "ACK_" + msg.ID
		}
		if ch, emAndamento := s.emCurso[msg.ID]; emAndamento {
			s.mu.Unlock()
			<-ch
			s.mu.Lock()
			continue
		}
		ch := make(chan struct{})
		s.emCurso[msg.ID] = ch
		break
	}
	s.mu.Unlock()

	ok := receive_msg(&msg.Conteudo)

	s.mu.Lock()
	ch := s.emCurso[msg.ID]
	delete(s.emCurso, msg.ID)
	if !ok {
		close(ch)
		s.mu.Unlock()
		return "ERROR_RECEIVE_FAILED"
	}
	s.contExecMsg[msg.ID]++
	dups := s.contExecMsg[msg.ID] - 1
	s.estado.Processadas[msg.ID] = true
	souCoord := s.coord == s.id
	endCoord := s.cfg.EnderecoServidor(s.coord)
	if souCoord {
		s.estado.Versao++
	}
	_ = comum.SalvarEstado(s.arqEst, s.estado)
	close(ch)
	s.mu.Unlock()

	if souCoord {
		s.replicarEstado()
	} else {
		_, _ = comum.Requisitar(endCoord, "PROCESSED_"+msg.ID, s.cfg.Timeout)
	}

	s.log.Registrar("servidor", s.id, "execucoes_duplicadas_receive_msg", float64(dups), "msg_id", msg.ID, "remetente", msg.Remetente, "destino", msg.Destino)
	s.log.Registrar("servidor", s.id, "acks_reenviados", 0, "msg_id", msg.ID)
	s.log.Registrar("servidor", s.id, "mensagens_confirmadas_perdidas", 0, "msg_id", msg.ID)
	return "ACK_" + msg.ID
}
