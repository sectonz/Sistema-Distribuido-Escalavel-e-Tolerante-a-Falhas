# Sistema Distribuído Escalável e Tolerante a Falhas (TF SDI 2026/2)

## Configuração (`.env`)

```env
SERVER_IP=ens5
SERVER_HOSTS=ens5,ens1,ens2,ens3,ens4
SERVER_PORT=41701
ENCODING=utf-8

CADASTRO_IP=ens5
CADASTRO_PORT=42701

MULTICAST_ADDR=239.67.67.67:43701
HEARTBEAT_MS=500
TIMEOUT_MS=4000
MAX_USUARIOS=5
REMOTE_WORKDIR=/home/g47075843818/sdi/tf
```

Arquivo `cadastro/.env` (usado pelo Cadastro de Usuários):

```env
SERVER_IP=ens5
SERVER_PORT=41701
ENCODING=utf-8

CONTROL_PORT=42701

HTTP_PORT=48701
HTTP_HOST=0.0.0.0
```

## Compilação

```bash
go build -o bin/servidor ./cmd/servidor
go build -o bin/usuario ./cmd/usuario
```

## Execucao

### 1. Limpar logs e processos antigos 

```bash
./limpar_cluster.sh --kill
```

### 2. Iniciar o Cadastro de Usuários

```bash
python3 cadastro/backend/cadastro.py
```

Sobe em `http://localhost:48701`.

### 3. Iniciar o Coordenador inicial

Em outro terminal:

```bash
./bin/servidor -id 0
```

Novos servidores necessários serão criados automaticamente pelo coordenador. Dá pra iniciar réplicas manualmente com `./bin/servidor -id <ID>`.

### 4. Cliente de Usuário

Modo interativo (Cria ou usa um usuario ja existente):

```bash
./bin/usuario -id 1 -nome "Andre"
```

Comandos no terminal do usuário:
- `<dest_id> <mensagem>` — envia mensagem para outro usuário
- `duplicar` — reenvia a última mensagem com o mesmo `MSG_ID` (teste de idempotência)
- `status` — exibe coordenador atual, servidores ativos e alocação dos usuários
- `auto` — liga/desliga envio automático em intervalos aleatórios de até 10s
- `sair` — encerra o cliente

Modo simulador (múltiplos usuários):

```bash
./bin/usuario -simulador -usuarios 8 -rodadas 16 -intervalo 3000
```

## Auxiliares

- **Dashboard pra centralizar metricas** (`http://localhost:48702`):
  ```bash
  python3 dashboard.py
  ```
- **Remover N usuários aleatórios** (pra testar scale down):
  ```bash
  python3 deluser_random.py 15
  ```
- **Limpar logs e estados em todos os hosts**:
  ```bash
  ./limpar_cluster.sh
  ```

## Logs e Estado

- Logs JSON: `logs/servidor_<ID>.json` e `logs/usuario_<ID>.json`
- Snapshots: `dados/estado_servidor_<ID>.json`
