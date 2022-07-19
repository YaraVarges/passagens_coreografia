from sys import api_version
from flask_apscheduler import APScheduler
from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from time import sleep
import random
import json

PROCESSO = "preco_passagem"
PROCESSO_COMPRA = "venda_de_passagem"
BANCO = "/workdir/banco.json"

def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()


def preco(dados_compra):
    sleep(random.randint(1, 4))

    valido, destino, mensagem, total_compra, precopassagem = False, "", "", 0.0, 0.0

    with open(BANCO, "r") as arquivo_banco:
        estoque = json.load(arquivo_banco)
        passagens = estoque["passagens"]
        for passagem in passagens:
            if passagem["id"] == dados_compra["cod_passagem"]:
                destino = passagem["destino"]
                valido = True
                precopassagem = passagem["preco"]
                total_compra = dados_compra["quantidade"] * float(passagem["preco"])
                mensagem = "Passagem comprada com Sucesso."

                break

        arquivo_banco.close()

    return valido, destino, mensagem, total_compra, precopassagem


def executar():
    global deslocamento

    consumir_compra = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000
    )
    topico = TopicPartition(PROCESSO_COMPRA, 0)
    consumir_compra.assign([topico])
    consumir_compra.seek(topico, deslocamento)

    for pedido in consumir_compra:
        deslocamento = pedido.offset + 1

        dados_compra = json.loads(pedido.value)
        valido, destino, mensagem, total_compra, precopassagem = preco(
            dados_compra)
        if valido:
            dados_compra["sucesso"] = 1
            dados_compra["PASSAGEM"] = destino
            dados_compra["MENSAGEM"] = mensagem
            dados_compra["PREÇO_PASSAGEM(UN)"] = precopassagem
            dados_compra["TOTAL_PASSAGENS"] = total_compra
        else:
            dados_compra["sucesso"] = 0

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
    agendador.add_job(id=PROCESSO, func=executar, trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
