from flask_apscheduler import APScheduler
from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from time import sleep
import random
import faker
import json

PROCESSO = "confirmacao"
PROCESSO_PRECO = "preco_passagem"


def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()


def confirmacao(dados_compra):
    sleep(random.randint(1, 6))

    pagamento = ["Dinheiro", "PIX", "Cartao de Credito"]
    forma_pagamento = random.choice(pagamento)

    valido, mensagem = True, "Pagamento confirmado. Forma de pagamento: " + forma_pagamento
    return valido, mensagem


def executar():
    global deslocamento

    consumir_preco = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000
    )
    topico = TopicPartition(PROCESSO_PRECO, 0)
    consumir_preco.assign([topico])
    consumir_preco.seek(topico, deslocamento)

    for pedido in consumir_preco:
        deslocamento = pedido.offset + 1

        dados_compra = pedido.value
        dados_compra = json.loads(dados_compra)

        valido, mensagem = confirmacao(dados_compra)
        if valido:
            dados_compra["sucesso"] = 1
        else:
            dados_compra["sucesso"] = 0
        dados_compra["Mensagem"] = mensagem

        try:
            produtor = KafkaProducer(
                bootstrap_servers=["kafka:29092"],
                api_version=(0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value=json.dumps(dados_compra).encode("utf-8"))
        except KafkaError as erro:

            print(f"Ocorreu erro: {erro}")


if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar,trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
