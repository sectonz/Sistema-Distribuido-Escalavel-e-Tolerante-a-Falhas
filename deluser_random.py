#!/usr/bin/env python3
"""
Script para excluir aleatoriamente N usuários cadastrados no cluster via DELUSER_<ID>.

Uso:
  python3 deluser_random.py <quantidade>
  python3 deluser_random.py 50
  python3 deluser_random.py 10 --intervalo 0.1
"""

import argparse
import glob
import json
import random
import socket
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def carregar_env():
    env = {}
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        env_file = BASE_DIR / "cadastro" / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env

CFG = carregar_env()
IP = CFG.get("SERVER_IP", CFG.get("IP", "127.0.0.1"))
HOSTS = [h.strip() for h in CFG.get("SERVER_HOSTS", "").split(",") if h.strip()] or [IP]
PORTA_BASE = int(CFG.get("SERVER_PORT", 5001))

def host_servidor(sid):
    return HOSTS[sid % len(HOSTS)]

def obter_coordenador_e_usuarios():
    # 1. Tenta ler o arquivo de estado mais recente em dados/
    arquivos = sorted(glob.glob(str(BASE_DIR / "dados" / "estado_servidor_*.json")), key=lambda p: Path(p).stat().st_mtime, reverse=True)
    for arq in arquivos:
        try:
            with open(arq, "r", encoding="utf-8") as f:
                d = json.load(f)
                coord_id = d.get("coordenador")
                usuarios = [int(u) for u in d.get("usuarios", {}).keys()]
                if coord_id is not None and usuarios:
                    return coord_id, usuarios
        except Exception:
            continue

    # 2. Se não achou em arquivo, consulta diretamente via STATUS nas portas
    for id_tentativa in range(30):
        try:
            porta = PORTA_BASE + id_tentativa
            host = host_servidor(id_tentativa)
            with socket.create_connection((host, porta), timeout=0.3) as s:
                s.sendall(b"STATUS\n")
                resp = s.recv(65536).decode("utf-8").strip()
                if resp and resp.startswith("{"):
                    d = json.loads(resp)
                    coord_id = d.get("coordenador")
                    usuarios = [int(u) for u in d.get("usuarios", {}).keys()]
                    if coord_id is not None:
                        return coord_id, usuarios
        except Exception:
            continue

    return None, []

def enviar_comando(host, porta, comando):
    with socket.create_connection((host, porta), timeout=2.0) as s:
        s.sendall((comando.strip() + "\n").encode("utf-8"))
        resp = s.recv(4096).decode("utf-8").strip()
        return resp

def main():
    parser = argparse.ArgumentParser(description="Exclui aleatoriamente N usuários via DELUSER.")
    parser.add_argument("qtd", type=int, help="Quantidade de usuários a remover")
    parser.add_argument("--intervalo", type=float, default=0.05, help="Intervalo em segundos entre cada DELUSER (padrão: 0.05s)")
    args = parser.parse_args()

    if args.qtd <= 0:
        print("[erro] A quantidade de usuários deve ser maior que 0.")
        sys.exit(1)

    coord_id, usuarios = obter_coordenador_e_usuarios()
    if coord_id is None:
        print("[erro] Não foi possível localizar o coordenador ativo. Verifique se os servidores estão rodando.")
        sys.exit(1)

    if not usuarios:
        print("[aviso] Nenhum usuário cadastrado encontrado no cluster.")
        sys.exit(0)

    host_coord = host_servidor(coord_id)
    porta_coord = PORTA_BASE + coord_id
    print(f"--- Remoção Aleatória de Usuários ---")
    print(f"Coordenador detectado: Servidor {coord_id} em {host_coord}:{porta_coord}")
    print(f"Total de usuários cadastrados no cluster: {len(usuarios)}")

    qtd_remover = min(args.qtd, len(usuarios))
    alvos = random.sample(usuarios, qtd_remover)
    print(f"Removendo {qtd_remover} usuário(s) aleatórios...\n")

    sucessos = 0
    falhas = 0

    for idx, uid in enumerate(alvos, 1):
        cmd = f"DELUSER_{uid}"
        try:
            resp = enviar_comando(host_coord, porta_coord, cmd)
            if resp.startswith("REDIRECT_"):
                # Atualiza endereço do coordenador em caso de redirecionamento
                partes = resp.split("_")
                host_coord = partes[1]
                porta_coord = int(partes[2])
                resp = enviar_comando(host_coord, porta_coord, cmd)

            if resp == f"ACK_DELUSER_{uid}":
                sucessos += 1
                print(f"[{idx}/{qtd_remover}] Usuário {uid} removido -> {resp}")
            else:
                falhas += 1
                print(f"[{idx}/{qtd_remover}] Falha ao remover usuário {uid}: {resp}")
        except Exception as exc:
            falhas += 1
            print(f"[{idx}/{qtd_remover}] Erro de conexão ao remover {uid}: {exc}")

        if args.intervalo > 0:
            time.sleep(args.intervalo)

    print(f"\nFinalizado: {sucessos} removidos com sucesso, {falhas} falhas.")
    print("O coordenador avaliará servidores ociosos para scale down automaticamente.")

if __name__ == "__main__":
    main()
