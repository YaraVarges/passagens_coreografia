from kafka import TopicPartition, KafkaConsumer
from time import sleep
import json

painel_compra = KafkaConsumer(
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1000)

topico = TopicPartition("venda_de_passagem", 0)
painel_compra.assign([topico])

painel_compra.seek_to_beginning(topico)
while True:
    print("Aguardando compra de passagens.")

    for pedido in painel_compra:
        dados_do_pedido = json.loads(pedido.value)
        print(f"Dados da compra: {dados_do_pedido}")

    sleep(4)