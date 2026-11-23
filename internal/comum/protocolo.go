package comum

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// MSG_<MSG_ID>_<SENDER_ID>_<DEST_ID>_<CONTEUDO>
// O conteudo e sempre o ultimo campo, entao pode conter "_".
// Quebras de linha sao trocadas por espaco, pois cada mensagem ocupa uma linha.
type Msg struct {
	ID        string
	Remetente int
	Destino   int
	Conteudo  string
}

func ParseMsg(linha string) (*Msg, error) {
	p := strings.SplitN(linha, "_", 5)
	if len(p) != 5 || p[0] != "MSG" || p[1] == "" {
		return nil, errors.New("formato invalido")
	}
	rem, err1 := strconv.Atoi(p[2])
	dest, err2 := strconv.Atoi(p[3])
	if err1 != nil || err2 != nil {
		return nil, errors.New("ids invalidos")
	}
	return &Msg{ID: p[1], Remetente: rem, Destino: dest, Conteudo: p[4]}, nil
}

func (m *Msg) String() string {
	return fmt.Sprintf("MSG_%s_%d_%d_%s", m.ID, m.Remetente, m.Destino, LimparTexto(m.Conteudo))
}

func LimparTexto(s string) string {
	s = strings.ReplaceAll(s, "\r", " ")
	return strings.ReplaceAll(s, "\n", " ")
}

func NovoIDMensagem(remetente int) string {
	return fmt.Sprintf("%d-%d", remetente, time.Now().UnixNano())
}

// Estado representa o snapshot replicado entre os servidores e persistido em disco.
type Estado struct {
	Versao      int64           `json:"versao"`
	Coordenador int             `json:"coordenador"`
	TsEnvio     float64         `json:"ts_envio"`
	Usuarios    map[int]string  `json:"usuarios"`
	Alocacao    map[int]int     `json:"alocacao"`
	Processadas map[string]bool `json:"processadas"`
	Servidores  map[int]string  `json:"servidores"`
}

func NovoEstado(coord int) Estado {
	return Estado{
		Coordenador: coord,
		Usuarios:    map[int]string{},
		Alocacao:    map[int]int{},
		Processadas: map[string]bool{},
		Servidores:  map[int]string{},
	}
}

func (e Estado) Clonar() Estado {
	b, _ := json.Marshal(e)
	var copia Estado
	_ = json.Unmarshal(b, &copia)
	if copia.Usuarios == nil {
		copia.Usuarios = map[int]string{}
	}
	if copia.Alocacao == nil {
		copia.Alocacao = map[int]int{}
	}
	if copia.Processadas == nil {
		copia.Processadas = map[string]bool{}
	}
	if copia.Servidores == nil {
		copia.Servidores = map[int]string{}
	}
	return copia
}

func SalvarEstado(caminho string, e Estado) error {
	os.MkdirAll(filepath.Dir(caminho), 0755)
	b, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	tmp := caminho + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, caminho)
}

func CarregarEstado(caminho string) (Estado, error) {
	b, err := os.ReadFile(caminho)
	if err != nil {
		return NovoEstado(0), err
	}
	var e Estado
	if err := json.Unmarshal(b, &e); err != nil {
		return NovoEstado(0), err
	}
	return e.Clonar(), nil
}
