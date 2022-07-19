from flask import Flask, jsonify
from kafka import KafkaClient, KafkaProducer
from kafka.errors import KafkaError
import hashlib
import random
import string
import json

servico = Flask(__name__)

PROCESSO = "venda_de_passagem"
DESCRICAO = "servico para venda de passagens de onibus"
VERSAO = "0.0.1"

def iniciar():
    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()


@servico.route("/info", methods=["GET"])
def get_info():
    return jsonify(descricao=DESCRICAO, versao=VERSAO)


@servico.route("/compra/<string:cpf_cliente>/<int:id_passagem>/<int:quantidade>", methods=["GET", "POST"])
def compra_passagem(cpf_cliente, id_passagem, quantidade):
    resultado = {
        "resultado": "falha",
        "id_transacao": ""
    }

    ID = "".join(random.choice(string.ascii_letters + string.punctuation)
                 for _ in range(12))
    ID = hashlib.md5(ID.encode("utf-8")).hexdigest()

    produtor = KafkaProducer(
        bootstrap_servers = ["kafka:29092"],
        api_version = (0, 10, 1)
    )

    try:
        dados_compra = {
            "identificacao": ID,
            "sucesso": 1, 
            "mensagem": "Compra de passagem iniciada.",
            "cpf_cliente": cpf_cliente,
            "cod_passagem": id_passagem,
            "quantidade": quantidade
        }
        produtor.send(topic=PROCESSO, value=json.dumps(dados_compra).encode("utf-8"))
        produtor.flush()

        resultado["resultado"] = "sucesso"
        resultado["id_transacao"] = ID
    except KafkaError as erro:
        pass

    produtor.close()

    return json.dumps(resultado).encode("utf-8")


if __name__ == "__main__":
    iniciar()

    servico.run(
        host="0.0.0.0",
        debug=True
    )
