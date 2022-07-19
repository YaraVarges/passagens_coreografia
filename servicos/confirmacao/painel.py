from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_confirmacao = KafkaConsumer(
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1000)

topico = TopicPartition("confirmacao", 0)
painel_confirmacao.assign([topico])

painel_confirmacao.seek_to_beginning(topico)
while True:
    print("Aguardando confirmação de compra da passagem.")

    for pedido in painel_confirmacao:
        dados_do_pedido = json.loads(pedido.value)
        print(f"Dados da compra: {dados_do_pedido}")

    sleep(4)
