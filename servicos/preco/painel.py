from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_preco = KafkaConsumer(
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1000)

topico = TopicPartition("preco_passagem", 0)
painel_preco.assign([topico])

painel_preco.seek_to_beginning(topico)
while True:
    print("Aguardando verificação do preço da passagem")

    for pedido in painel_preco:
        dados_do_pedido = json.loads(pedido.value)
        print(f"Dados da compra: {dados_do_pedido}")

    sleep(4)
