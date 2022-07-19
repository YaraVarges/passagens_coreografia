from kafka import TopicPartition, KafkaConsumer
from time import sleep
import json

painel_bilhete = KafkaConsumer(
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1000)

topico = TopicPartition("bilhete", 0)
painel_bilhete.assign([topico])

painel_bilhete.seek_to_beginning(topico)
while True:
    print("Aguardando número do bilhete da passagem.")

    for pedido in painel_bilhete:
        dados_do_pedido = json.loads(pedido.value)
        print(f"Dados da compra: {dados_do_pedido}")

    sleep(4)
