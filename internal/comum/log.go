package comum

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Logger grava metricas em JSON Lines no formato pedido no enunciado.
type Logger struct {
	mu  sync.Mutex
	arq *os.File
	eco bool
}

func NovoLogger(caminho string, eco bool) *Logger {
	os.MkdirAll(filepath.Dir(caminho), 0755)
	f, err := os.OpenFile(caminho, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return &Logger{arq: f, eco: eco}
}

// extras sao pares chave/valor adicionados ao registro.
func (l *Logger) Registrar(componente string, id int, metrica string, valor float64, extras ...interface{}) {
	ts := float64(time.Now().UnixMilli()) / 1000
	v := strconv.FormatFloat(valor, 'f', -1, 64)

	var b, eco strings.Builder
	fmt.Fprintf(&b, `{"ts": %.3f, "componente": "%s", "id": %d, "metrica": "%s", "valor": %s`,
		ts, componente, id, metrica, v)
	for i := 0; i+1 < len(extras); i += 2 {
		k, _ := json.Marshal(fmt.Sprint(extras[i]))
		val, _ := json.Marshal(extras[i+1])
		fmt.Fprintf(&b, ", %s: %s", k, val)
		fmt.Fprintf(&eco, " %v=%v", extras[i], extras[i+1])
	}
	b.WriteString("}\n")

	l.mu.Lock()
	l.arq.WriteString(b.String())
	l.mu.Unlock()
	if l.eco {
		fmt.Printf("%s [%s %d] %s=%s%s\n", time.Now().Format("15:04:05.000"), componente, id, metrica, v, eco.String())
	}
}

// Converte uma duracao para segundos com precisao de milissegundos.
func Seg(d time.Duration) float64 {
	return math.Round(d.Seconds()*1000) / 1000
}

func Agora() float64 {
	return float64(time.Now().UnixMilli()) / 1000
}

