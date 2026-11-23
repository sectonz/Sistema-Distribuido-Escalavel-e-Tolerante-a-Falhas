package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"sd/internal/comum"
)

type ClienteUsuario struct {
	id         int
	nome       string
	cfg        comum.Config
	log        *comum.Logger

	mu         sync.Mutex
	endCoord   string
	enviadas   int
	reenvios   int
	ultimaMsg  *comum.Msg
}

func NovoClienteUsuario(id int, nome string, cfg comum.Config, ecoLog bool) *ClienteUsuario {
	if nome == "" {
		nome = fmt.Sprintf("Usuario%d", id)
	}
	return &ClienteUsuario{
		id:       id,
		nome:     nome,
		cfg:      cfg,
		log:      comum.NovoLogger(fmt.Sprintf("logs/usuario_%d.json", id), ecoLog),
		endCoord: cfg.EnderecoServidor(0),
	}
}

func (u *ClienteUsuario) Cadastrar() error {
	cmd := fmt.Sprintf("NEWUSER_%d_%s", u.id, u.nome)
	u.mu.Lock()
	end := u.endCoord
	u.mu.Unlock()

	resp, err := comum.EnviarAoCoordenador(u.cfg, &end, cmd, u.cfg.Timeout+1500*time.Millisecond)
	u.mu.Lock()
	u.endCoord = end
	u.mu.Unlock()
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERROR_") {
		return fmt.Errorf("erro do coordenador: %s", resp)
	}
	return nil
}

func (u *ClienteUsuario) ObterEstado() (comum.Estado, error) {
	u.mu.Lock()
	end := u.endCoord
	u.mu.Unlock()

	resp, err := comum.EnviarAoCoordenador(u.cfg, &end, "STATUS", u.cfg.Timeout)
	u.mu.Lock()
	u.endCoord = end
	u.mu.Unlock()
	if err != nil {
		return comum.Estado{}, err
	}
	var est comum.Estado
	if err := json.Unmarshal([]byte(resp), &est); err != nil {
		return comum.Estado{}, err
	}
	return est, nil
}

// Envia uma mensagem ao destinatario via coordenador, realizando reenvios com o mesmo MSG_ID em caso de falha.
func (u *ClienteUsuario) EnviarMensagem(msg *comum.Msg) (string, error) {
	u.mu.Lock()
	u.enviadas++
	u.ultimaMsg = msg
	end := u.endCoord
	u.mu.Unlock()

	timeout := u.cfg.Timeout + 2*time.Second
	var ultimoErro error
	var ultimaResp string

	for tentativa := 1; tentativa <= 4; tentativa++ {
		if tentativa > 1 {
			u.mu.Lock()
			u.reenvios++
			u.mu.Unlock()
			time.Sleep(800 * time.Millisecond)
			if _, novoEnd, ok := comum.DescobrirCoordenador(u.cfg, 20); ok {
				end = novoEnd
			}
		}

		resp, err := comum.EnviarAoCoordenador(u.cfg, &end, msg.String(), timeout)
		u.mu.Lock()
		u.endCoord = end
		razao := math.Round((float64(u.reenvios)/float64(u.enviadas))*1000) / 1000
		u.mu.Unlock()

		if err == nil && resp == "ACK_"+msg.ID {
			u.log.Registrar("usuario", u.id, "reenvios_por_mensagens", razao, "msg_id", msg.ID, "tentativas", tentativa)
			u.log.Registrar("usuario", u.id, "mensagens_perdidas_falha", 0, "msg_id", msg.ID)
			u.log.Registrar("usuario", u.id, "mensagens_confirmadas_perdidas", 0, "msg_id", msg.ID)
			return resp, nil
		}
		ultimoErro = err
		ultimaResp = resp
		if err == nil && strings.HasPrefix(resp, "ERROR_") && resp != "ERROR_DELIVERY_FAILED" {
			break
		}
	}

	u.mu.Lock()
	razao := math.Round((float64(u.reenvios)/float64(u.enviadas))*1000) / 1000
	u.mu.Unlock()
	u.log.Registrar("usuario", u.id, "reenvios_por_mensagens", razao, "msg_id", msg.ID)
	u.log.Registrar("usuario", u.id, "mensagens_perdidas_falha", 1, "msg_id", msg.ID)
	if ultimoErro != nil {
		return "", ultimoErro
	}
	return ultimaResp, fmt.Errorf("falha no envio: %s", ultimaResp)
}

func main() {
	id := flag.Int("id", 1, "ID do usuario (modo individual)")
	nome := flag.String("nome", "", "Nome do usuario (modo individual)")
	auto := flag.Bool("auto", false, "Envia mensagens automaticas em intervalos aleatorios de ate 10s")
	simulador := flag.Bool("simulador", false, "Executa o simulador com multiplos usuarios para testes rapidos")
	numUsuarios := flag.Int("usuarios", 8, "Quantidade de usuarios a criar no modo simulador")
	rodadas := flag.Int("rodadas", 16, "Quantidade total de mensagens a enviar no modo simulador (0 = continuo)")
	maxIntervaloMs := flag.Int("intervalo", 3000, "Intervalo maximo entre envios em ms (padrao enunciado: ate 10000)")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
	cfg := comum.CarregarConfig()

	if *simulador {
		executarSimulador(cfg, *numUsuarios, *rodadas, *maxIntervaloMs)
		return
	}

	executarUsuarioIndividual(cfg, *id, *nome, *auto, *maxIntervaloMs)
}

func executarUsuarioIndividual(cfg comum.Config, id int, nome string, iniciarAuto bool, maxIntervaloMs int) {
	u := NovoClienteUsuario(id, nome, cfg, false)
	if err := u.Cadastrar(); err != nil {
		fmt.Printf("[aviso] nao foi possivel cadastrar usuario %d no inicio (%v)\n", id, err)
	} else {
		fmt.Printf("[ok] usuario %d (%s) conectado ao sistema.\n", id, u.nome)
	}

	var muAuto sync.Mutex
	autoAtivo := iniciarAuto
	parar := make(chan struct{})

	if maxIntervaloMs <= 0 || maxIntervaloMs > 10000 {
		maxIntervaloMs = 10000
	}

	go func() {
		for {
			espera := time.Duration(rand.Intn(maxIntervaloMs)+500) * time.Millisecond
			select {
			case <-parar:
				return
			case <-time.After(espera):
			}
			muAuto.Lock()
			ativo := autoAtivo
			muAuto.Unlock()
			if !ativo {
				continue
			}
			est, err := u.ObterEstado()
			if err != nil || len(est.Usuarios) < 2 {
				continue
			}
			var destinos []int
			for uid := range est.Usuarios {
				if uid != u.id {
					destinos = append(destinos, uid)
				}
			}
			if len(destinos) == 0 {
				continue
			}
			dest := destinos[rand.Intn(len(destinos))]
			msg := &comum.Msg{
				ID:        comum.NovoIDMensagem(u.id),
				Remetente: u.id,
				Destino:   dest,
				Conteudo:  fmt.Sprintf("Mensagem automatica de %d para %d", u.id, dest),
			}
			resp, err := u.EnviarMensagem(msg)
			if err != nil {
				fmt.Printf("\n[auto] erro ao enviar %s: %v\n> ", msg.ID, err)
			} else {
				fmt.Printf("\n[auto] %s -> %s\n> ", msg.String(), resp)
			}
		}
	}()

	imprimirAjuda(u.id)
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			break
		}
		linha := strings.TrimSpace(sc.Text())
		if linha == "" {
			continue
		}
		partes := strings.Fields(linha)
		cmd := strings.ToLower(partes[0])

		switch cmd {
		case "sair", "exit", "quit":
			close(parar)
			return

		case "ajuda", "help":
			imprimirAjuda(u.id)

		case "status", "usuarios":
			est, err := u.ObterEstado()
			if err != nil {
				fmt.Printf("Erro ao consultar coordenador: %v\n", err)
				continue
			}
			imprimirEstado(est)

		case "auto":
			muAuto.Lock()
			autoAtivo = !autoAtivo
			estadoAtual := autoAtivo
			muAuto.Unlock()
			fmt.Printf("Envio automatico: %v\n", estadoAtual)

		case "duplicar":
			u.mu.Lock()
			ult := u.ultimaMsg
			u.mu.Unlock()
			if ult == nil {
				fmt.Println("Nenhuma mensagem anterior para reenviar. Envie uma mensagem primeiro.")
				continue
			}
			fmt.Printf("Reenviando mesmo MSG_ID (%s) para testar idempotencia...\n", ult.ID)
			resp, err := u.EnviarMensagem(ult)
			if err != nil {
				fmt.Printf("Erro: %v\n", err)
			} else {
				fmt.Printf("Resposta: %s (verifique acks_reenviados no log do servidor)\n", resp)
			}

		case "reenviar":
			if len(partes) < 4 {
				fmt.Println("Uso: reenviar <msg_id> <dest_id> <conteudo>")
				continue
			}
			dest, err := strconv.Atoi(partes[2])
			if err != nil {
				fmt.Println("ID de destino invalido.")
				continue
			}
			texto := strings.Join(partes[3:], " ")
			msg := &comum.Msg{ID: partes[1], Remetente: u.id, Destino: dest, Conteudo: texto}
			resp, err := u.EnviarMensagem(msg)
			if err != nil {
				fmt.Printf("Erro: %v\n", err)
			} else {
				fmt.Printf("Resposta: %s\n", resp)
			}

		default:
			if len(partes) < 2 {
				fmt.Println("Comando invalido. Digite '<dest_id> <mensagem>' ou 'ajuda'.")
				continue
			}
			dest, err := strconv.Atoi(partes[0])
			if err != nil {
				fmt.Println("Comando ou ID de destino invalido. Digite 'ajuda'.")
				continue
			}
			texto := strings.Join(partes[1:], " ")
			msg := &comum.Msg{
				ID:        comum.NovoIDMensagem(u.id),
				Remetente: u.id,
				Destino:   dest,
				Conteudo:  texto,
			}
			fmt.Printf("Enviando %s ...\n", msg.String())
			resp, err := u.EnviarMensagem(msg)
			if err != nil {
				fmt.Printf("Falha: %v\n", err)
			} else {
				fmt.Printf("Confirmado: %s\n", resp)
			}
		}
	}
}

func imprimirAjuda(id int) {
	fmt.Printf("\n--- Terminal do Usuario %d ---\n", id)
	fmt.Println("Comandos disponiveis:")
	fmt.Println("  <dest_id> <texto>                  Envia mensagem ao usuario <dest_id> (ex.: 2 Ola tudo bem)")
	fmt.Println("  duplicar                           Reenvia a ultima mensagem com o mesmo MSG_ID (teste de idempotencia)")
	fmt.Println("  reenviar <msg_id> <dest_id> <txt>  Envia mensagem com MSG_ID especifico")
	fmt.Println("  status                             Mostra coordenador, servidores ativos e alocacao dos usuarios")
	fmt.Println("  auto                               Ativa/desativa envio automatico (intervalos aleatorios <= 10s)")
	fmt.Println("  sair                               Encerra o cliente")
}

func imprimirEstado(est comum.Estado) {
	fmt.Printf("Coordenador atual: servidor %d (versao %d)\n", est.Coordenador, est.Versao)
	var srvs []int
	for id := range est.Servidores {
		srvs = append(srvs, id)
	}
	sort.Ints(srvs)
	fmt.Printf("Servidores ativos (%d): %v\n", len(srvs), srvs)

	var uids []int
	for uid := range est.Usuarios {
		uids = append(uids, uid)
	}
	sort.Ints(uids)
	fmt.Printf("Usuarios cadastrados (%d):\n", len(uids))
	for _, uid := range uids {
		fmt.Printf("  - Usuario %d (%s) -> Servidor %d\n", uid, est.Usuarios[uid], est.Alocacao[uid])
	}
}

func executarSimulador(cfg comum.Config, numUsuarios, rodadas, maxIntervaloMs int) {
	if numUsuarios < 2 {
		numUsuarios = 2
	}
	if maxIntervaloMs <= 0 {
		maxIntervaloMs = 2000
	}
	fmt.Printf("[simulador] iniciando com %d usuarios (intervalo ate %dms)...\n", numUsuarios, maxIntervaloMs)

	clientes := make([]*ClienteUsuario, 0, numUsuarios)
	for i := 1; i <= numUsuarios; i++ {
		c := NovoClienteUsuario(i, fmt.Sprintf("SimUser%d", i), cfg, true)
		if err := c.Cadastrar(); err != nil {
			fmt.Printf("[simulador] falha ao cadastrar usuario %d: %v\n", i, err)
		} else {
			fmt.Printf("[simulador] usuario %d cadastrado.\n", i)
		}
		clientes = append(clientes, c)
		time.Sleep(80 * time.Millisecond)
	}

	if est, err := clientes[0].ObterEstado(); err == nil {
		imprimirEstado(est)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	enviadas := 0
	for rodadas == 0 || enviadas < rodadas {
		select {
		case <-sig:
			fmt.Println("\n[simulador] encerrando...")
			return
		default:
		}

		remIdx := rand.Intn(len(clientes))
		destIdx := rand.Intn(len(clientes))
		if destIdx == remIdx {
			destIdx = (remIdx + 1) % len(clientes)
		}
		rem := clientes[remIdx]
		dest := clientes[destIdx]

		msg := &comum.Msg{
			ID:        comum.NovoIDMensagem(rem.id),
			Remetente: rem.id,
			Destino:   dest.id,
			Conteudo:  fmt.Sprintf("Ola de %d para %d (#%d)", rem.id, dest.id, enviadas+1),
		}
		resp, err := rem.EnviarMensagem(msg)
		if err != nil {
			fmt.Printf("[simulador] erro em %s: %v\n", msg.String(), err)
		} else {
			fmt.Printf("[simulador] %s -> %s\n", msg.String(), resp)
			// Na primeira mensagem confirmada, reenvia o mesmo MSG_ID para exercitar a idempotencia.
			if enviadas == 0 {
				fmt.Printf("[simulador] reenviando %s para validar idempotencia...\n", msg.ID)
				respDup, _ := rem.EnviarMensagem(msg)
				fmt.Printf("[simulador] resposta duplicata %s -> %s\n", msg.ID, respDup)
			}
		}
		enviadas++

		espera := time.Duration(rand.Intn(maxIntervaloMs)+200) * time.Millisecond
		select {
		case <-sig:
			return
		case <-time.After(espera):
		}
	}
	fmt.Printf("[simulador] concluido (%d mensagens enviadas).\n", enviadas)
}

