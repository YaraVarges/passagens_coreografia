from flask_apscheduler import APScheduler
from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from time import sleep
import random
import faker
import json

PROCESSO = "bilhete"
PROCESSO_CONFIRMACAO = "confirmacao"

def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

def bilhete(dados_compra):
    sleep(random.randint(1, 6))

    dados_falsos = faker.Faker(locale="pt_BR")
    cliente = dados_falsos.name()
    bilhete = dados_falsos.numerify(text='####')

    valido, mensagem = True, "Bilhete N°" + bilhete + " da passagem comprada  para o(a) cliente: " + cliente
    return valido, mensagem

def executar():
    global deslocamento

    consumir_confirmacao = KafkaConsumer(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1),
        auto_offset_reset = "earliest",
        consumer_timeout_ms = 1000
    )
    topico = TopicPartition(PROCESSO_CONFIRMACAO, 0)
    consumir_confirmacao.assign([topico])
    consumir_confirmacao.seek(topico, deslocamento)

    for pedido in consumir_confirmacao:
        deslocamento = pedido.offset + 1

        dados_compra = pedido.value
        dados_compra = json.loads(dados_compra)

        valido, mensagem = bilhete(dados_compra)
        if valido:
            dados_compra["sucesso"] = 1
        else:
            dados_compra["sucesso"] = 0
        dados_compra["mensagem"] = mensagem

        try:
            produtor = KafkaProducer(
                bootstrap_servers = [ "kafka:29092" ],
                api_version = (0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value= json.dumps(
                dados_compra
            ).encode("utf-8"))
        except KafkaError as erro:
             print(f"Ocorreu erro: {erro}")

if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar, trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
