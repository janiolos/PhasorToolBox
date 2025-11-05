import logging
import subprocess
import sys
from typing import Dict, List, Optional

import psutil
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Field, Session, SQLModel, create_engine, select
from fastapi.staticfiles import StaticFiles

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOG = logging.getLogger("PDC_Manager_API")

# --- Configuração do Banco de Dados (SQLite) ---
sqlite_file_name = "flows.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=True)


class FluxoConfig(SQLModel, table=True):
    # O frontend espera esses campos
    id: Optional[int] = Field(default=None, primary_key=True)
    ID_Fluxo: str = Field(unique=True, index=True)  # Este será o 'flow_id'
    Agente: str
    Tipo_conexao: str = "TCP"
    Versao_norma: str = "C37.118-2011"
    Tipo_Frame_configuracao: str = "CFG-2"
    IP: str
    Porta: int
    IDCODE: int  # Adicionamos o IDCODE, que é essencial para o PDC


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


# --- Gerenciamento de Processos ---
# Dicionário para rastrear processos em execução
# Chave: ID_Fluxo, Valor: (subprocess.Popen, psutil.Process)
running_processes: Dict[str, tuple[subprocess.Popen, psutil.Process]] = {}

# --- Configuração do FastAPI ---
app = FastAPI(
    title="SMART Phasor - PDC Management System",
    description="Sistema de gerenciamento de fluxos de PMUs com persistência em BD e monitoramento de processos.",
)

#Montar pasta 'static' (se você tiver uma para CSS/JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Configurar Templates Jinja2
templates = Jinja2Templates(directory="templates")

# --- Conexão Redis (para o Dashboard) ---
redis_client: redis.Redis = None


# --- Dependência de Sessão do Banco de Dados ---
def get_session():
    with Session(engine) as session:
        yield session


# --- Eventos de Startup/Shutdown ---
@app.on_event("startup")
async def startup_event():
    global redis_client
    create_db_and_tables()
    LOG.info("Banco de dados verificado/criado.")

    # Conectar ao Redis (usado apenas pela API do dashboard)
    try:
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        await redis_client.ping()
        LOG.info("Conexão com Redis (para API de dados) bem-sucedida.")
    except Exception as e:
        LOG.error(f"Não foi possível conectar ao Redis na inicialização: {e}")
        redis_client = None


@app.on_event("shutdown")
async def shutdown_event():
    LOG.info("Parando todos os processos de fluxo...")
    for flow_id in list(running_processes.keys()):
        await stop_flow_process(flow_id, Session(engine))  # Passa uma nova sessão

    if redis_client:
        await redis_client.close()
    LOG.info("Servidor desligado.")


# --- Funções Auxiliares de Processo ---

async def start_flow_process(flow: FluxoConfig, session: Session):
    """Lança um fluxo como um subprocesso."""
    flow_id = flow.ID_Fluxo
    if flow_id in running_processes:
        return {"status": "error", "message": "Processo já está em execução."}

    LOG.info(f"Iniciando processo para o fluxo: {flow_id}...")

    # Comando para executar o script 'run_pdc_process.py'
    command = [
        sys.executable,  # O caminho para o interpretador Python atual
        "run_pdc_process.py",
        "--flow_id", flow.ID_Fluxo,
        "--idcode", str(flow.IDCODE),
        "--ip", flow.IP,
        "--port", str(flow.Porta)
    ]

    try:
        # Inicia o processo
        process = subprocess.Popen(command)
        # Cria o wrapper do psutil para monitoramento
        psutil_process = psutil.Process(process.pid)
        # Armazena
        running_processes[flow_id] = (process, psutil_process)
        LOG.info(f"Processo para {flow_id} iniciado com PID: {process.pid}")
        return {"status": "success", "message": "Fluxo iniciado."}

    except Exception as e:
        LOG.error(f"Falha ao iniciar {flow_id}: {e}")
        return {"status": "error", "message": str(e)}


async def stop_flow_process(flow_id: str, session: Session):
    """Para um subprocesso de fluxo."""
    if flow_id not in running_processes:
        return {"status": "error", "message": "Processo não encontrado."}

    LOG.info(f"Parando processo para o fluxo: {flow_id}...")
    process, psutil_proc = running_processes[flow_id]

    try:
        process.terminate()  # Envia SIGTERM
        process.wait(timeout=5)  # Espera o processo terminar
        LOG.info(f"Processo {flow_id} (PID: {process.pid}) terminado.")
    except subprocess.TimeoutExpired:
        LOG.warning(f"Processo {flow_id} não terminou, forçando (SIGKILL)...")
        process.kill()
    except psutil.NoSuchProcess:
        LOG.warning(f"Processo {flow_id} já não existia.")

    del running_processes[flow_id]
    return {"status": "success", "message": "Fluxo parado."}


# --- API Endpoints (para o Frontend AJAX) ---

@app.post("/api/receptor/start")
async def api_start_flow(request: dict, session: Session = Depends(get_session)):
    flow_id = request.get("id_fluxo")
    flow = session.get(FluxoConfig, flow_id)  # O frontend antigo usa 'id' como int

    if not flow:
        # Tenta buscar pelo ID_Fluxo (string)
        statement = select(FluxoConfig).where(FluxoConfig.ID_Fluxo == flow_id)
        flow = session.exec(statement).first()

    if not flow:
        raise HTTPException(status_code=404, detail="Fluxo não encontrado no banco de dados.")

    result = await start_flow_process(flow, session)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result


@app.post("/api/receptor/stop")
async def api_stop_flow(request: dict, session: Session = Depends(get_session)):
    flow_id = request.get("id_fluxo")
    result = await stop_flow_process(flow_id, session)
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@app.get("/api/fluxos/status_completo")
async def get_all_flows_status(session: Session = Depends(get_session)):
    """
    Endpoint principal do 'fluxo.html'.
    Busca todos os fluxos do BD e combina com o status de execução.
    """
    flows_from_db = session.exec(select(FluxoConfig)).all()
    response_list = []

    for flow in flows_from_db:
        status_info = {
            "status": "Parado",
            "pid": None,
            "cpu_percent": 0.0,
            "memory_mb": 0.0
        }

        if flow.ID_Fluxo in running_processes:
            process, psutil_proc = running_processes[flow.ID_Fluxo]

            # Verifica se o processo ainda está vivo
            if process.poll() is None:  # None = Ainda rodando
                try:
                    with psutil_proc.oneshot():
                        status_info = {
                            "status": "Em Execução",
                            "pid": psutil_proc.pid,
                            "cpu_percent": psutil_proc.cpu_percent(interval=0.0),  # Não bloqueante
                            "memory_mb": psutil_proc.memory_info().rss / (1024 * 1024)  # Em MB
                        }
                except psutil.NoSuchProcess:
                    status_info["status"] = "Erro (Processo Morto)"
                    del running_processes[flow.ID_Fluxo]
            else:  # Processo morreu/crashou
                status_info["status"] = f"Erro (Código: {process.poll()})"
                del running_processes[flow.ID_Fluxo]

        # Monta o objeto JSON que o frontend espera
        response_list.append({
            "id": flow.id,
            "ID_Fluxo": flow.ID_Fluxo,
            "Agente": flow.Agente,
            "IP": flow.IP,
            "Porta": flow.Porta,
            "status_info": status_info,
            # Adicione outros campos do BD se o frontend precisar
        })
    print("debug:")
    print(response_list)
    return {"data": response_list}


@app.get("/api/health/overview_stats")
async def get_overview_stats(session: Session = Depends(get_session)):
    """
    Endpoint para o 'index.html'.
    """
    total = len(session.exec(select(FluxoConfig)).all())
    running = 0
    error = 0

    # Atualiza a lista de processos (limpa os que morreram)
    for flow_id in list(running_processes.keys()):
        process, _ = running_processes[flow_id]
        if process.poll() is None:
            running += 1
        else:
            error += 1
            del running_processes[flow_id]  # Limpa o processo morto

    stopped = total - running - error

    return {
        "total": total,
        "running": running,
        "stopped": stopped,
        "error": error
    }


@app.get("/api/pmu_data/{flow_id}")
async def get_pmu_data(flow_id: str):
    """
    Endpoint para o 'dashboard.html'.
    Lê o *último* valor do Hash do Redis.
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Conexão com Redis indisponível.")

    hash_key = f"pmu_data:{flow_id}"
    try:
        data = await redis_client.hgetall(hash_key)
        if not data:
            return {"data": {}}

        # Converte os valores de string para números
        # O dashboard.html espera números
        processed_data = {}
        for k, v in data.items():
            try:
                processed_data[k] = float(v)
            except ValueError:
                processed_data[k] = v

        return {"data": processed_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao ler do Redis: {e}")


# --- Rotas de Página (Renderização de HTML) ---

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "title": "Dashboard"})


@app.get("/fluxo", response_class=HTMLResponse)
async def read_fluxo_page(request: Request):
    return templates.TemplateResponse("fluxo.html", {"request": request, "title": "Gerenciar Fluxos"})


@app.get("/add_fluxo", response_class=HTMLResponse)
async def read_add_fluxo_form(request: Request):
    return templates.TemplateResponse("add_fluxo.html", {"request": request, "title": "Adicionar Novo Fluxo"})


@app.post("/add_fluxo")
async def process_add_fluxo_form(
        session: Session = Depends(get_session),
        Agente: str = Form(...),
        ID_Fluxo: str = Form(...),  # Adicionei este campo
        IDCODE: int = Form(...),  # Adicionei este campo
        IP: str = Form(...),
        Porta: int = Form(...),
        Tipo_conexao: str = Form("TCP"),
        Versao_norma: str = Form("C37.118-2011"),
        Tipo_Frame_configuracao: str = Form("CFG-2")
):
    # O formulário original não tinha ID_Fluxo e IDCODE,
    # Modifique o add_fluxo.html para incluir:
    # <input type="text" class="form-control" id="ID_Fluxo" name="ID_Fluxo" required>
    # <input type="number" class="form-control" id="IDCODE" name="IDCODE" required>

    new_flow = FluxoConfig(
        Agente=Agente,
        ID_Fluxo=ID_Fluxo,
        IDCODE=IDCODE,
        IP=IP,
        Porta=Porta,
        Tipo_conexao=Tipo_conexao,
        Versao_norma=Versao_norma,
        Tipo_Frame_configuracao=Tipo_Frame_configuracao
    )

    session.add(new_flow)
    session.commit()

    return RedirectResponse(url="/fluxo", status_code=303)  # Redireciona para a lista


@app.get("/edit_fluxo/{flow_id_str}", response_class=HTMLResponse)
async def read_edit_fluxo_form(request: Request, flow_id_str: str, session: Session = Depends(get_session)):
    statement = select(FluxoConfig).where(FluxoConfig.ID_Fluxo == flow_id_str)
    flow = session.exec(statement).first()
    if not flow:
        raise HTTPException(status_code=404, detail="Fluxo não encontrado")
    return templates.TemplateResponse("edit_fluxo.html", {"request": request, "title": "Editar Fluxo", "fluxo": flow})


@app.post("/edit_fluxo/{flow_id_str}")
async def process_edit_fluxo_form(
        flow_id_str: str,
        session: Session = Depends(get_session),
        Agente: str = Form(...),
        IDCODE: int = Form(...),
        IP: str = Form(...),
        Porta: int = Form(...),
        Tipo_conexao: str = Form(...),
        Versao_norma: str = Form(...),
        Tipo_Frame_configuracao: str = Form(...)
):
    statement = select(FluxoConfig).where(FluxoConfig.ID_Fluxo == flow_id_str)
    flow = session.exec(statement).first()
    if not flow:
        raise HTTPException(status_code=404, detail="Fluxo não encontrado")

    # Atualiza os campos
    flow.Agente = Agente
    flow.IDCODE = IDCODE
    flow.IP = IP
    flow.Porta = Porta
    flow.Tipo_conexao = Tipo_conexao
    flow.Versao_norma = Versao_norma
    flow.Tipo_Frame_configuracao = Tipo_Frame_configuracao

    session.add(flow)
    session.commit()

    return RedirectResponse(url="/fluxo", status_code=303)


@app.get("/dashboard/{pmu_id}", response_class=HTMLResponse)
async def read_dashboard(request: Request, pmu_id: str):
    # O pmu_id aqui é o ID_Fluxo
    return templates.TemplateResponse("dashboard.html",
                                      {"request": request, "title": f"Dashboard {pmu_id}", "pmu_id": pmu_id})
