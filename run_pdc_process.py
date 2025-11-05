import asyncio
import json
import logging
import sys
import argparse  # Para ler argumentos de linha de comando
from typing import List

try:
    import redis.asyncio as redis
except ModuleNotFoundError:
    redis = None

from phasortoolbox import Client, PDC, Synchrophasor

# Configuração
REDIS_HOST = "localhost"
REDIS_PORT = 6379

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOG = logging.getLogger("PDC_Runner")

# Conexão Redis (será criada dentro do loop asyncio)
redis_client: redis.Redis = None


async def redis_callback(synchrophasors: List[Synchrophasor], flow_id: str):
    """
    Callback chamado pelo PDC. Envia dados para um Stream e um Hash no Redis.
    """
    print(f"Debug: Iniciou o REDIS com {flow_id}")
    global redis_client
    if not synchrophasors:
        return

    # Conectar ao Redis se ainda não estiver conectado
    if redis_client is None:
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            await redis_client.ping()
            LOG.info(f"Flow {flow_id}: Redis connection successful.")
        except Exception as e:
            LOG.error(f"Flow {flow_id}: Could not connect to Redis: {e}")
            return  # Não pode continuar sem Redis

    latest_synchrophasor = synchrophasors[-1]

    # --- Montagem do Payload para o Dashboard ---
    # O frontend (dashboard.html) espera chaves específicas.
    # Vamos tentar popular algumas delas.

    data_to_send = {
        "timestamp": latest_synchrophasor.time,
        "flow_id": flow_id,
        "frequency": getattr(latest_synchrophasor, 'frequency', 0.0),
        "df_dt": getattr(latest_synchrophasor, 'df_dt', 0.0),
    }

    # Tentativa de extrair fasores e nomeá-los como o frontend espera
    # (Ex: 'VA_mag', 'VA_ang'). Isso é uma suposição.
    try:
        if len(latest_synchrophasor.phasors) > 0:
            data_to_send["VA_mag"] = latest_synchrophasor.phasors[0].magnitude
            data_to_send["VA_ang"] = latest_synchrophasor.phasors[0].angle * 180 / 3.14159  # Convertendo para Graus
        if len(latest_synchrophasor.phasors) > 1:
            data_to_send["VB_mag"] = latest_synchrophasor.phasors[1].magnitude
            data_to_send["VB_ang"] = latest_synchrophasor.phasors[1].angle * 180 / 3.14159
        # Adicione mais fasores conforme necessário...
    except Exception:
        pass  # Ignora erros se os fasores não existirem

    # --- Envio para o Redis ---
    # Usamos dois métodos para suportar os dois frontends:

    # 1. Para o 'dashboard.html': Salvar o *último* valor em um Hash
    #    Isso permite que a API /api/pmu_data/ leia o valor mais recente.
    #    Chave: "pmu_data:<flow_id>"
    hash_key = f"pmu_data:{flow_id}"

    # 2. Para um histórico: Adicionar a um Stream (como no seu código original)
    #    Chave: "stream:<flow_id>"
    stream_key = f"stream:{flow_id}"

    try:
        # Usamos um Pipelina para eficiência
        pipe = redis_client.pipeline()

        # 1. Hash (sobrescreve o último valor)
        # Convertemos todos os valores para string para o HSET
        data_for_hash = {k: str(v) for k, v in data_to_send.items()}
        pipe.hset(hash_key, mapping=data_for_hash)
        pipe.expire(hash_key, 60)  # Expira o dado se a PMU parar

        # 2. Stream (adiciona ao histórico)
        pipe.xadd(stream_key, data_to_send, maxlen=10000)

        await pipe.execute()

        LOG.debug(f"Flow {flow_id}: Data sent to Redis Hash and Stream.")

    except Exception as e:
        LOG.error(f"Flow {flow_id}: Error sending to Redis: {e}")


async def main(config: dict):
    """
    Configura e executa o PDC.
    """
    flow_id = config['flow_id']
    LOG.info(f"Starting flow {flow_id}...")
    print(f"Starting flow {flow_id}...")
    clients = []
    #pmu_client1 = Client(remote_ip='10.0.0.160', remote_port=4712, idcode=1, mode='TCP')
    for pmu_config in config['pmu_configs']:
        client = Client(
            idcode=pmu_config['idcode'],
            remote_ip=pmu_config['remote_ip'],
            remote_port=pmu_config['remote_port'],
            mode=pmu_config.get('mode', 'TCP')
        )
        print(f"debug: Client {client}")

        clients.append(client)

    pdc_instance = PDC(
        callback=lambda synchrophasors: asyncio.create_task(redis_callback(synchrophasors, flow_id)),
        clients=clients,
        time_out=config.get('time_out', 0.1),
        history=config.get('history', 1)
    )

    try:
        pdc_instance.loop = asyncio.get_running_loop()
        pdc_instance.executor = None
        await pdc_instance.coro_run()
    except asyncio.CancelledError:
        LOG.info(f"Flow {flow_id} task cancelled.")
    except Exception as e:
        LOG.error(f"Flow {flow_id} crashed: {e}")
    finally:
        await pdc_instance.coro_close()
        if redis_client:
            await redis_client.close()
        LOG.info(f"Flow {flow_id} closed.")


if __name__ == "__main__":
    # Este script é feito para ser chamado com argumentos
    parser = argparse.ArgumentParser(description="PDC Runner Process")
    parser.add_argument("--flow_id", required=True)
    parser.add_argument("--idcode", required=True, type=int)
    parser.add_argument("--ip", required=True)
    parser.add_argument("--port", required=True, type=int)

    args = parser.parse_args()

    # Monta a configuração do PDC a partir dos argumentos
    pdc_config = {
        "flow_id": args.flow_id,
        "pmu_configs": [
            {
                "idcode": args.idcode,
                "remote_ip": args.ip,
                "remote_port": args.port
            }
        ]
    }
    print(f"debug: pmu_configs: {pdc_config}")
    try:
        asyncio.run(main(pdc_config))
    except KeyboardInterrupt:
        LOG.info("PDC Runner process shutting down.")