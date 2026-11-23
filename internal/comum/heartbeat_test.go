package comum

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatMulticast(t *testing.T) {
	grupo := "239.1.2.3:19999"
	parar := make(chan struct{})
	defer close(parar)
	var c1, c2 int32

	go OuvirHeartbeats(grupo, parar, func(id, coord int) {
		if id == 7 && coord == 0 {
			atomic.AddInt32(&c1, 1)
		}
	})
	go OuvirHeartbeats(grupo, parar, func(id, coord int) {
		if id == 7 && coord == 0 {
			atomic.AddInt32(&c2, 1)
		}
	})

	time.Sleep(100 * time.Millisecond)
	go EnviarHeartbeats(grupo, 50*time.Millisecond, parar, func() string {
		return MensagemHeartbeat(7, 0)
	})

	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt32(&c1) == 0 || atomic.LoadInt32(&c2) == 0 {
		t.Fatalf("nao recebeu multicast em ambos os ouvintes: c1=%d c2=%d", c1, c2)
	}
}
