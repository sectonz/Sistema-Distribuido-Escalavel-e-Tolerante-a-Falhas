#!/usr/bin/env python3
"""
Dashboard web simples para monitoramento e centralização de métricas e estado do cluster.
Executa na porta 8080 (ou PORT especificada).
"""

import concurrent.futures
import datetime
import glob
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PORT = int(os.environ.get("DASHBOARD_PORT", 48702))


def carregar_env():
    env = {}
    for candidato in (BASE_DIR / ".env", BASE_DIR / "cadastro" / ".env"):
        if candidato.exists():
            for line in candidato.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
            break
    return env


CFG = carregar_env()
SERVER_IP = os.environ.get("SERVER_IP", CFG.get("SERVER_IP", "127.0.0.1"))
SERVER_HOSTS = [
    h.strip()
    for h in os.environ.get("SERVER_HOSTS", CFG.get("SERVER_HOSTS", "")).split(",")
    if h.strip()
] or [SERVER_IP]
SERVER_PORT = int(os.environ.get("SERVER_PORT", CFG.get("SERVER_PORT", 5001)))
MAX_USUARIOS = int(os.environ.get("MAX_USUARIOS", CFG.get("MAX_USUARIOS", 5)))
CADASTRO_HTTP_PORT = int(os.environ.get("HTTP_PORT", CFG.get("HTTP_PORT", 8000)))
REMOTE_WORKDIR = os.environ.get("REMOTE_WORKDIR", CFG.get("REMOTE_WORKDIR", str(BASE_DIR)))

_coord_lock = threading.Lock()
_coord_cache = (SERVER_HOSTS[0], SERVER_PORT)

_logs_remotos_lock = threading.Lock()
_logs_remotos_cache = []
_logs_remotos_ts = 0.0

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <title>Dashboard - Métricas do Cluster</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
  <style>
    body { background-color: #f8f9fa; font-family: system-ui, -apple-system, sans-serif; }
    .card-metric { border-radius: 8px; border: none; box-shadow: 0 2px 4px rgba(0,0,0,0.06); }
    .metric-value { font-size: 2rem; font-weight: 700; color: #0d6efd; }
    .table-sm td, .table-sm th { font-size: 0.88rem; }
    .badge-event { font-weight: 500; font-size: 0.8rem; }
    .refresh-badge { font-size: 0.75rem; }
  </style>
</head>
<body class="py-4">
  <div class="container-fluid px-4">
    <div class="d-flex justify-content-between align-items-center mb-4">
      <div>
        <h3 class="mb-0">Painel de Métricas e Estado do Cluster</h3>
        <small class="text-muted">Visualização consolidada dos arquivos de log e estado</small>
      </div>
      <div>
        <span class="badge bg-secondary refresh-badge me-2" id="lastUpdated">Atualizando...</span>
        <button class="btn btn-sm btn-outline-primary" onclick="carregarDados()">Atualizar Agora</button>
      </div>
    </div>

    <!-- Cards Principais -->
    <div class="row g-3 mb-4">
      <div class="col-md-3 col-sm-6">
        <div class="card card-metric p-3">
          <span class="text-muted small">Coordenador Atual</span>
          <div class="metric-value text-primary" id="coordAtual">-</div>
          <small class="text-muted" id="coordVersao">Versão: -</small>
        </div>
      </div>
      <div class="col-md-3 col-sm-6">
        <div class="card card-metric p-3">
          <span class="text-muted small">Servidores Registrados</span>
          <div class="metric-value text-success" id="totalServidores">-</div>
          <small class="text-muted" id="servidoresDet">-</small>
        </div>
      </div>
      <div class="col-md-3 col-sm-6">
        <div class="card card-metric p-3">
          <span class="text-muted small">Total de Usuários</span>
          <div class="metric-value text-info" id="totalUsuarios">-</div>
          <small class="text-muted" id="mediaUsuarios">-</small>
        </div>
      </div>
      <div class="col-md-3 col-sm-6">
        <div class="card card-metric p-3">
          <span class="text-muted small">Servidores Ociosos (0 usuários)</span>
          <div class="metric-value" id="servidoresOciosos">-</div>
          <small class="text-muted" id="necessariosRatio">-</small>
        </div>
      </div>
    </div>

    <!-- Métricas Recentes & Eventos Críticos -->
    <div class="row g-4 mb-4">
      <div class="col-lg-6">
        <div class="card card-metric p-3 h-100">
          <h6 class="card-title fw-bold text-secondary mb-3">Eventos Críticos Recentes (Eleições & Falhas)</h6>
          <div class="table-responsive" style="max-height: 320px; overflow-y: auto;">
            <table class="table table-sm table-hover align-middle mb-0">
              <thead class="table-light sticky-top">
                <tr>
                  <th>Horário</th>
                  <th>Componente</th>
                  <th>Evento</th>
                  <th>Detalhes</th>
                </tr>
              </thead>
              <tbody id="tabelaEventos">
                <tr><td colspan="4" class="text-center text-muted">Carregando eventos...</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="col-lg-6">
        <div class="card card-metric p-3 h-100">
          <h6 class="card-title fw-bold text-secondary mb-3">Métricas de Balanceamento & Replicação</h6>
          <div class="table-responsive" style="max-height: 320px; overflow-y: auto;">
            <table class="table table-sm table-hover align-middle mb-0">
              <thead class="table-light sticky-top">
                <tr>
                  <th>Métrica</th>
                  <th>Último Valor</th>
                  <th>Componente / ID</th>
                  <th>Horário</th>
                </tr>
              </thead>
              <tbody id="tabelaMetricas">
                <tr><td colspan="4" class="text-center text-muted">Carregando métricas...</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <!-- Distribuição de Usuários por Servidor -->
    <div class="card card-metric p-3">
      <h6 class="card-title fw-bold text-secondary mb-3">Distribuição de Carga por Servidor (Alocação)</h6>
      <div class="table-responsive" style="max-height: 360px; overflow-y: auto;">
        <table class="table table-sm table-striped table-hover align-middle mb-0">
          <thead class="table-light sticky-top">
            <tr>
              <th>ID do Servidor</th>
              <th>Endereço</th>
              <th>Papel</th>
              <th>Qtd Usuários</th>
              <th>Carga (%)</th>
            </tr>
          </thead>
          <tbody id="tabelaServidores">
            <tr><td colspan="5" class="text-center text-muted">Carregando servidores...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    async function carregarDados() {
      try {
        const res = await fetch('/api/resumo');
        if (!res.ok) return;
        const d = await res.json();

        // Cards
        document.getElementById('coordAtual').textContent = d.coordenador !== null ? `Servidor ${d.coordenador}` : 'Nenhum';
        document.getElementById('coordVersao').textContent = `Versão do Estado: ${d.versao || 0}`;
        document.getElementById('totalServidores').textContent = d.total_servidores;
        document.getElementById('servidoresDet').textContent = `${d.servidores_com_usuarios} ativos c/ carga`;
        document.getElementById('totalUsuarios').textContent = d.total_usuarios;
        document.getElementById('mediaUsuarios').textContent = `Média: ${d.media_usuarios_por_servidor} usuários/servidor`;
        
        const ociososEl = document.getElementById('servidoresOciosos');
        ociososEl.textContent = d.servidores_ociosos;
        ociososEl.className = d.servidores_ociosos > 0 ? 'metric-value text-warning' : 'metric-value text-secondary';
        document.getElementById('necessariosRatio').textContent = `Necessários: ~${d.servidores_necessarios} nós (teto ${d.max_usuarios_por_servidor} u/srv)`;

        // Eventos Críticos
        const tbEventos = document.getElementById('tabelaEventos');
        if (!d.eventos || d.eventos.length === 0) {
          tbEventos.innerHTML = '<tr><td colspan="4" class="text-center text-muted">Nenhum evento registrado ainda</td></tr>';
        } else {
          tbEventos.innerHTML = d.eventos.map(e => `
            <tr>
              <td><small class="text-muted">${e.hora}</small></td>
              <td><span class="badge ${e.componente === 'coordenador' ? 'bg-primary' : 'bg-secondary'}">${e.componente} ${e.id}</span></td>
              <td><span class="badge ${corEvento(e.metrica)} badge-event">${e.metrica}</span></td>
              <td><small>${formatarDetalhes(e)}</small></td>
            </tr>
          `).join('');
        }

        // Métricas
        const tbMetricas = document.getElementById('tabelaMetricas');
        if (!d.metricas || Object.keys(d.metricas).length === 0) {
          tbMetricas.innerHTML = '<tr><td colspan="4" class="text-center text-muted">Nenhuma métrica recente</td></tr>';
        } else {
          tbMetricas.innerHTML = Object.entries(d.metricas).map(([k, v]) => `
            <tr>
              <td><code>${k}</code></td>
              <td><strong>${v.valor}</strong></td>
              <td><small class="text-muted">${v.componente} ${v.id}</small></td>
              <td><small class="text-muted">${v.hora}</small></td>
            </tr>
          `).join('');
        }

        // Tabela Servidores
        const tbSrvs = document.getElementById('tabelaServidores');
        if (!d.servidores || d.servidores.length === 0) {
          tbSrvs.innerHTML = '<tr><td colspan="5" class="text-center text-muted">Nenhum servidor encontrado no estado</td></tr>';
        } else {
          tbSrvs.innerHTML = d.servidores.map(s => {
            const pct = Math.min(100, Math.round((s.usuarios / (d.max_usuarios_por_servidor || 5)) * 100));
            const barColor = pct >= 100 ? 'bg-danger' : (pct >= 60 ? 'bg-warning' : 'bg-success');
            return `
              <tr>
                <td><strong>#${s.id}</strong></td>
                <td><code>${s.endereco}</code></td>
                <td>
                  ${s.id === d.coordenador ? '<span class="badge bg-primary">Coordenador</span>' : '<span class="badge bg-light text-dark border">Réplica</span>'}
                </td>
                <td>${s.usuarios}</td>
                <td style="width: 200px;">
                  <div class="progress" style="height: 16px;">
                    <div class="progress-bar ${barColor}" style="width: ${pct}%;">${pct}%</div>
                  </div>
                </td>
              </tr>
            `;
          }).join('');
        }

        document.getElementById('lastUpdated').textContent = `Atualizado às ${new Date().toLocaleTimeString()}`;
      } catch (err) {
        console.error('Erro ao carregar dados:', err);
      }
    }

    function corEvento(m) {
      if (m.includes('eleito') || m.includes('inicio')) return 'bg-success';
      if (m.includes('falha') || m.includes('perdidas')) return 'bg-danger';
      if (m.includes('migrados') || m.includes('desativar')) return 'bg-warning text-dark';
      return 'bg-info text-dark';
    }

    function formatarDetalhes(e) {
      const chaves = Object.keys(e).filter(k => !['ts', 'hora', 'componente', 'id', 'metrica'].includes(k));
      return chaves.map(k => `${k}: <b>${e[k]}</b>`).join(', ') || `valor: ${e.valor}`;
    }

    carregarDados();
    setInterval(carregarDados, 3000);
  </script>
</body>
</html>
"""

def host_servidor(sid):
    return SERVER_HOSTS[sid % len(SERVER_HOSTS)]


def eh_host_local(host):
    if host in ("", "127.0.0.1", "localhost"):
        return True
    hn = socket.gethostname().split(".")[0].lower()
    return host.split(".")[0].lower() == hn


def consultar_tcp(host, porta, comando, timeout=0.6):
    with socket.create_connection((host, porta), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall((comando + "\n").encode("utf-8"))
        partes = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            partes.append(chunk)
            if b"\n" in chunk:
                break
        return b"".join(partes).split(b"\n", 1)[0].decode("utf-8", errors="replace").strip()


def obter_coordenador_do_cadastro():
    try:
        url = f"http://127.0.0.1:{CADASTRO_HTTP_PORT}/api/status"
        with urllib.request.urlopen(url, timeout=0.35) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            coord_str = data.get("coordinator", "")
            if ":" in coord_str:
                h, p = coord_str.rsplit(":", 1)
                return h, int(p)
    except Exception:
        pass
    return None


def descobrir_coordenador_rede(max_id=30):
    def sondar(sid):
        h = host_servidor(sid)
        p = SERVER_PORT + sid
        try:
            resp = consultar_tcp(h, p, "WHOISCOORD", timeout=0.35)
            if resp.startswith("COORD_"):
                partes = resp.split("_")
                if len(partes) == 4:
                    return partes[2], int(partes[3])
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futuros = [ex.submit(sondar, sid) for sid in range(max_id + 1)]
        for fut in concurrent.futures.as_completed(futuros):
            res = fut.result()
            if res is not None:
                return res
    return None


def obter_estado_ao_vivo():
    global _coord_cache
    candidatos = []
    with _coord_lock:
        if _coord_cache:
            candidatos.append(_coord_cache)

    do_cadastro = obter_coordenador_do_cadastro()
    if do_cadastro and do_cadastro not in candidatos:
        candidatos.append(do_cadastro)

    for host, porta in candidatos:
        try:
            resp = consultar_tcp(host, porta, "STATUS", timeout=0.8)
            if resp.startswith("{"):
                est = json.loads(resp)
                cid = est.get("coordenador")
                if cid is not None:
                    h_coord = host_servidor(int(cid))
                    p_coord = SERVER_PORT + int(cid)
                    if (h_coord, p_coord) != (host, porta):
                        try:
                            resp2 = consultar_tcp(h_coord, p_coord, "STATUS", timeout=0.8)
                            if resp2.startswith("{"):
                                est = json.loads(resp2)
                                host, porta = h_coord, p_coord
                        except Exception:
                            pass
                with _coord_lock:
                    _coord_cache = (host, porta)
                return est
        except Exception:
            continue

    descoberto = descobrir_coordenador_rede(30)
    if descoberto:
        host, porta = descoberto
        try:
            resp = consultar_tcp(host, porta, "STATUS", timeout=0.8)
            if resp.startswith("{"):
                with _coord_lock:
                    _coord_cache = (host, porta)
                return json.loads(resp)
        except Exception:
            pass

    return None


def coletar_logs_remotos():
    global _logs_remotos_cache, _logs_remotos_ts
    remotos = [h for h in dict.fromkeys(SERVER_HOSTS) if not eh_host_local(h)]
    if not remotos:
        return []

    agora = time.time()
    with _logs_remotos_lock:
        if agora - _logs_remotos_ts < 4.0:
            return list(_logs_remotos_cache)

    def buscar_host(host):
        itens = []
        cmd = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=2",
            host,
            f"tail -q -n 40 {REMOTE_WORKDIR}/logs/servidor_*.json 2>/dev/null || true",
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=3)
            for line in res.stdout.splitlines():
                line = line.strip()
                if line.startswith("{"):
                    try:
                        itens.append(json.loads(line))
                    except Exception:
                        pass
        except Exception:
            pass
        return itens

    coletados = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(remotos)) as ex:
        for res in ex.map(buscar_host, remotos):
            coletados.extend(res)

    with _logs_remotos_lock:
        _logs_remotos_cache = coletados
        _logs_remotos_ts = time.time()
    return coletados


def extrair_resumo():
    # 1. Consulta o estado ao vivo via TCP no coordenador atual (funciona em multi-maquinas).
    #    Caso nenhum servidor responda na rede, usa o snapshot local em dados/ como fallback.
    estado_atual = obter_estado_ao_vivo() or {}
    if not estado_atual:
        arquivos_estado = sorted(
            glob.glob(str(BASE_DIR / "dados" / "estado_servidor_*.json")),
            key=os.path.getmtime,
            reverse=True,
        )
        for p in arquivos_estado:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    d = json.load(f)
                    if d.get("servidores"):
                        estado_atual = d
                        break
            except Exception:
                continue

    srvs = estado_atual.get("servidores", {})
    aloc = estado_atual.get("alocacao", {})
    usuarios = estado_atual.get("usuarios", {})
    coord = estado_atual.get("coordenador", None)
    versao = estado_atual.get("versao", 0)

    contagens = {int(sid): 0 for sid in srvs}
    for u, sid in aloc.items():
        if sid in contagens:
            contagens[sid] += 1

    lista_servidores = []
    for sid_str, end in sorted(srvs.items(), key=lambda x: int(x[0])):
        sid = int(sid_str)
        lista_servidores.append({
            "id": sid,
            "endereco": end,
            "usuarios": contagens.get(sid, 0)
        })

    total_srvs = len(lista_servidores)
    total_u = len(usuarios)
    com_u = sum(1 for s in lista_servidores if s["usuarios"] > 0)
    ociosos = total_srvs - com_u
    media_u = round(total_u / total_srvs, 2) if total_srvs > 0 else 0
    max_u = MAX_USUARIOS
    necessarios = -(-total_u // max_u) if total_u > 0 else 1

    # 2. Coleta eventos críticos e métricas recentes dos logs locais e das máquinas remotas
    eventos = []
    metricas = {}
    eventos_chave = {
        "eleito_coordenador", "tempo_deteccao_falha", "usuarios_migrados",
        "tempo_novo_servidor_operante", "tempo_desativar_servidor_ocioso",
        "tempo_sincronizacao_retorno"
    }

    arquivos_logs = sorted(glob.glob(str(BASE_DIR / "logs" / "servidor_*.json")), key=os.path.getmtime, reverse=True)[:30]
    linhas_recentes = []

    for arq in arquivos_logs:
        try:
            with open(arq, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        linhas_recentes.append(json.loads(line))
        except Exception:
            continue

    linhas_recentes.extend(coletar_logs_remotos())
    linhas_recentes.sort(key=lambda x: x.get("ts", 0), reverse=True)

    for item in linhas_recentes[:300]:
        m = item.get("metrica")
        ts = item.get("ts", 0)
        hora_str = datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")
        item["hora"] = hora_str

        if m in eventos_chave and len(eventos) < 15:
            eventos.append(item)

        if m not in metricas and len(metricas) < 12:
            metricas[m] = item

    return {
        "coordenador": coord,
        "versao": versao,
        "total_servidores": total_srvs,
        "servidores_com_usuarios": com_u,
        "servidores_ociosos": ociosos,
        "total_usuarios": total_u,
        "media_usuarios_por_servidor": media_u,
        "servidores_necessarios": necessarios,
        "max_usuarios_por_servidor": max_u,
        "servidores": lista_servidores,
        "eventos": eventos,
        "metricas": metricas
    }

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode("utf-8"))
        elif path == "/api/resumo":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            resumo = extrair_resumo()
            self.wfile.write(json.dumps(resumo, ensure_ascii=False).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"[dashboard] Servidor de métricas rodando em: http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    main()

