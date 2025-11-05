import asyncio
import json
import logging
import time
from typing import Dict, List

try:
    import redis.asyncio as redis
except ModuleNotFoundError:
    redis = None
    
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Importar classes da biblioteca PhasorToolBox
# O import direto pode falhar se o ambiente não estiver configurado corretamente,
# mas como instalamos com `pip install -e`, deve funcionar.
from phasortoolbox import Client, PDC, Synchrophasor

# Configuração
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_STREAM_KEY = "phasor_data_stream"

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOG = logging.getLogger("PDC_System")

# --- Modelos Pydantic para a API ---

class PmuConfig(BaseModel):
    idcode: int
    remote_ip: str
    remote_port: int
    mode: str = 'TCP'

class FlowConfig(BaseModel):
    flow_id: str
    pmu_configs: List[PmuConfig]
    time_out: float = 0.1
    history: int = 1

class FlowStatus(BaseModel):
    flow_id: str
    status: str
    pmu_count: int
    last_sync_time: float = 0.0
    synchrophasors_created: int = 0

# --- Gerenciamento de Fluxos (PDC Flow Manager) ---

class PDCFlowManager:
    def __init__(self, redis_client: redis.Redis):
        self.flows: Dict[str, PDC] = {}
        self.flow_tasks: Dict[str, asyncio.Task] = {}
        self.redis_client = redis_client
        self.flow_status: Dict[str, FlowStatus] = {}

    async def _redis_callback(self, synchrophasors: List[Synchrophasor], flow_id: str):
        """
        Callback chamado pelo PDC quando um synchrophasor é criado.
        Responsável por formatar e enviar os dados para o Redis Stream.
        """
        if not synchrophasors:
            return

        # Pegamos o mais recente
        latest_synchrophasor = synchrophasors[-1]

        # --- Sugestão de Expansão ---
        # 1. Extrair os dados de fasores.
        #    (Assumindo que `latest_synchrophasor.phasors` é uma lista de objetos `Phasor`)

        phasor_data_list = []
        try:
            # Iterar sobre os fasores dentro do objeto Synchrophasor
            # O objeto Phasor da biblioteca geralmente tem .magnitude e .angle
            for phasor in latest_synchrophasor.phasors:
                phasor_data_list.append({
                    "magnitude": phasor.magnitude,
                    "angle": phasor.angle  # (Em radianos, geralmente)
                })
        except AttributeError as e:
            # Logar se a estrutura do objeto Synchrophasor não for a esperada
            LOG.warning(f"Flow {flow_id}: Could not parse phasor attributes: {e}")

        # 2. Construir o payload completo para o Redis
        data_to_send = {
            "timestamp": latest_synchrophasor.time,
            "flow_id": flow_id,

            # Dados do próprio Synchrophasor (ex: frequência, df/dt)
            "frequency": getattr(latest_synchrophasor, 'frequency', 0.0),
            "df_dt": getattr(latest_synchrophasor, 'df_dt', 0.0),

            # Serializar a lista de fasores como uma string JSON
            # É mais eficiente armazenar dados estruturados como JSON em um único campo do Redis Stream
            "phasors_json": json.dumps(phasor_data_list),

            # Você também pode querer enviar os dados analógicos ou digitais
             "analogs_json": json.dumps(latest_synchrophasor.analogs),
             "digitals_json": json.dumps(latest_synchrophasor.digitals),
        }

        # --- Fim da Sugestão ---

        # Atualizar status
        status = self.flow_status.get(flow_id)  # Obter status de forma segura
        if status:
            status.last_sync_time = latest_synchrophasor.time
            status.synchrophasors_created += 1

        # Enviar para o Redis Stream
        try:
            # O redis-py lida com a serialização dos valores do dicionário para strings
            await self.redis_client.xadd(
                REDIS_STREAM_KEY,
                data_to_send,
                maxlen=10000
            )
            LOG.debug(f"Flow {flow_id}: Data sent to Redis Stream.")
        except Exception as e:
            LOG.error(f"Flow {flow_id}: Error sending to Redis: {e}")

    async def start_flow(self, config: FlowConfig) -> FlowStatus:
        flow_id = config.flow_id
        if flow_id in self.flows:
            raise HTTPException(status_code=400, detail=f"Flow {flow_id} already exists.")

        LOG.info(f"Starting flow {flow_id}...")

        # 1. Criar os clientes PMU
        clients = []
        for pmu_config in config.pmu_configs:
            client = Client(
                idcode=pmu_config.idcode,
                remote_ip=pmu_config.remote_ip,
                remote_port=pmu_config.remote_port,
                mode=pmu_config.mode
            )
            clients.append(client)

        # 2. Criar a instância do PDC
        # O callback precisa ser uma função que aceite 'synchrophasors' e 'flow_id'
        # Usamos uma lambda para injetar o flow_id
        pdc_instance = PDC(
            callback=lambda synchrophasors: asyncio.create_task(self._redis_callback(synchrophasors, flow_id)),
            clients=clients,
            time_out=config.time_out,
            history=config.history
        )
        self.flows[flow_id] = pdc_instance
        
        # 3. Inicializar o status
        self.flow_status[flow_id] = FlowStatus(
            flow_id=flow_id,
            status="Starting",
            pmu_count=len(config.pmu_configs)
        )

        # 4. Criar a tarefa assíncrona para rodar o PDC
        # O PDC.run() original é bloqueante. Usamos o coro_run() para rodar no loop de eventos do FastAPI.
        async def run_pdc():
            try:
                # O PDC.set_loop() é chamado internamente pelo coro_run()
                await pdc_instance.coro_run()
                self.flow_status[flow_id].status = "Stopped (Finished)"
            except asyncio.CancelledError:
                LOG.info(f"Flow {flow_id} task cancelled.")
                self.flow_status[flow_id].status = "Stopped (Cancelled)"
            except Exception as e:
                LOG.error(f"Flow {flow_id} crashed: {e}")
                self.flow_status[flow_id].status = f"Crashed: {e.__class__.__name__}"
            finally:
                await pdc_instance.coro_close()
                LOG.info(f"Flow {flow_id} closed.")

        task = asyncio.create_task(run_pdc())
        self.flow_tasks[flow_id] = task
        self.flow_status[flow_id].status = "Running"
        
        return self.flow_status[flow_id]

    async def stop_flow(self, flow_id: str) -> FlowStatus:
        if flow_id not in self.flows:
            raise HTTPException(status_code=404, detail=f"Flow {flow_id} not found.")

        LOG.info(f"Stopping flow {flow_id}...")
        
        task = self.flow_tasks.get(flow_id)
        if task:
            task.cancel()
            # Aguardar a tarefa ser cancelada e o coro_close ser chamado no run_pdc
            try:
                await task
            except asyncio.CancelledError:
                pass # Esperado

        # Limpar o estado
        del self.flows[flow_id]
        del self.flow_tasks[flow_id]
        
        self.flow_status[flow_id].status = "Stopped"
        return self.flow_status[flow_id]

    def get_status(self, flow_id: str) -> FlowStatus:
        if flow_id not in self.flow_status:
            raise HTTPException(status_code=404, detail=f"Flow {flow_id} not found.")
        return self.flow_status[flow_id]

    def list_flows(self) -> List[FlowStatus]:
        return list(self.flow_status.values())

# --- Inicialização do FastAPI e Redis ---

app = FastAPI(
    title="PDC Redis Stream System",
    description="Sistema de Concentração de Dados de Fasores (PDC) com gerenciamento dinâmico de fluxos e envio para Redis Stream.",
    version="1.0.0"
)

# Configuração de CORS para permitir o frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Permitir qualquer origem para o desenvolvimento
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client: redis.Redis = None
flow_manager: PDCFlowManager = None

@app.on_event("startup")
async def startup_event():
    global redis_client, flow_manager
    LOG.info("Connecting to Redis...")
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        await redis_client.ping()
        LOG.info("Redis connection successful.")
    except Exception as e:
        LOG.error(f"Could not connect to Redis: {e}")
        # Em um ambiente real, isso seria um erro fatal. Aqui, continuamos para permitir o desenvolvimento.
        # Mas as funcionalidades de Redis não funcionarão.
        redis_client = None 

    flow_manager = PDCFlowManager(redis_client)
    LOG.info("PDC Flow Manager initialized.")

@app.on_event("shutdown")
async def shutdown_event():
    LOG.info("Stopping all active flows...")
    for flow_id in list(flow_manager.flows.keys()):
        await flow_manager.stop_flow(flow_id)
    
    if redis_client:
        await redis_client.close()
        LOG.info("Redis connection closed.")

# --- Rotas da API ---

@app.get("/")
async def root():
    return {"message": "PDC Redis Stream System API is running."}

@app.post("/flows/start", response_model=FlowStatus)
async def start_flow_route(config: FlowConfig):
    return await flow_manager.start_flow(config)

@app.post("/flows/{flow_id}/stop", response_model=FlowStatus)
async def stop_flow_route(flow_id: str):
    return await flow_manager.stop_flow(flow_id)

@app.get("/flows", response_model=List[FlowStatus])
async def list_flows_route():
    return flow_manager.list_flows()

@app.get("/flows/{flow_id}", response_model=FlowStatus)
async def get_flow_status_route(flow_id: str):
    return flow_manager.get_status(flow_id)

@app.get("/redis/stream_key")
async def get_stream_key():
    return {"stream_key": REDIS_STREAM_KEY}