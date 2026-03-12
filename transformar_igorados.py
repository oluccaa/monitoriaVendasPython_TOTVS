import json
import re
from pathlib import Path


def normalizar_id(raw_id, largura=6):
    """
    Normaliza IDs de pedido para comparação confiável.
    """
    if raw_id is None:
        return ""

    valor = str(raw_id).strip()

    if not valor:
        return ""

    # caso venha "281.0"
    if valor.endswith(".0"):
        valor = valor[:-2]

    # extrai apenas números
    valor = re.sub(r"\D", "", valor)

    if not valor:
        return ""

    return valor.zfill(largura)


def organizar_blacklist(caminho):
    caminho = Path(caminho)

    if not caminho.exists():
        print(f"Arquivo não encontrado: {caminho}")
        return

    with caminho.open("r", encoding="utf-8-sig") as f:
        dados = json.load(f)

    if not isinstance(dados, list):
        print("O JSON precisa ser uma lista de pedidos.")
        return

    total_original = len(dados)

    # normaliza e remove vazios
    normalizados = [
        normalizar_id(p) for p in dados if normalizar_id(p)
    ]

    # remove duplicados
    unicos = sorted(set(normalizados), key=lambda x: int(x))

    duplicados = len(normalizados) - len(unicos)

    # salva novamente
    with caminho.open("w", encoding="utf-8") as f:
        json.dump(unicos, f, indent=2, ensure_ascii=False)

    print("Arquivo organizado com sucesso.")
    print(f"Total original: {total_original}")
    print(f"Total único: {len(unicos)}")
    print(f"Duplicados removidos: {duplicados}")


if __name__ == "__main__":
    organizar_blacklist("ignorar_pedidos.json")