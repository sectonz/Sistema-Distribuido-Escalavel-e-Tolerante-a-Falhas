#!/usr/bin/env bash
# Remove logs e arquivos de estado (dados/) na maquina local e em todos os hosts configurados no .env.
# Uso:
#   ./limpar_cluster.sh          # limpa apenas logs e dados
#   ./limpar_cluster.sh --kill   # tambem encerra processos bin/servidor em todos os hosts

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

SERVER_HOSTS="ens5,ens1,ens2,ens3,ens4"
REMOTE_WORKDIR="$SCRIPT_DIR"

if [[ -f "$ENV_FILE" ]]; then
    while IFS='=' read -r chave valor; do
        chave="$(echo "$chave" | xargs)"
        [[ -z "$chave" || "$chave" == \#* ]] && continue
        valor="$(echo "$valor" | xargs | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")"
        case "$chave" in
            SERVER_HOSTS) SERVER_HOSTS="$valor" ;;
            REMOTE_WORKDIR) REMOTE_WORKDIR="$valor" ;;
        esac
    done < "$ENV_FILE"
fi

MATAR=0
if [[ "${1:-}" == "--kill" ]]; then
    MATAR=1
fi

HOST_LOCAL="$(hostname -s 2>/dev/null || hostname)"
IFS=',' read -ra HOSTS <<< "$SERVER_HOSTS"

for host in "${HOSTS[@]}"; do
    host="$(echo "$host" | xargs)"
    [[ -z "$host" ]] && continue
    host_curto="${host%%.*}"

    if [[ "$host" == "127.0.0.1" || "$host" == "localhost" || "${host_curto,,}" == "${HOST_LOCAL,,}" ]]; then
        echo "[local: $host] limpando logs e dados..."
        if [[ $MATAR -eq 1 ]]; then
            pkill -f "[b]in/servidor" 2>/dev/null || true
        fi
        rm -f "$SCRIPT_DIR"/logs/* "$SCRIPT_DIR"/dados/* "$SCRIPT_DIR"/cadastro/cadastro.json
    else
        echo "[remoto: $host] limpando logs e dados..."
        CMD_REMOTO="rm -f '$REMOTE_WORKDIR'/logs/* '$REMOTE_WORKDIR'/dados/*"
        if [[ $MATAR -eq 1 ]]; then
            CMD_REMOTO="pkill -f '[b]in/servidor' 2>/dev/null || true; $CMD_REMOTO"
        fi
        ssh -o BatchMode=yes "$host" "$CMD_REMOTO" \
            || echo "[aviso] falha ao executar limpeza em $host"
    fi
done

echo "Limpeza concluida."

