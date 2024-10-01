from pymongo import MongoClient
from datetime import datetime, timezone

# Conectar ao MongoDB
client = MongoClient("mongodb+srv://devsamuelmuniz:OX7ttUHpNVmZnE97@db.lt2n6.mongodb.net/?retryWrites=true&w=majority&appName=db")
db = client['clash']

# 1. Inserir um jogador
def inserir_jogador(nickname, tempo_jogo, trofeus, nivel):
    player_data = {
        "nickname": nickname,
        "tempo_jogo": tempo_jogo,
        "trofeus": trofeus,
        "nivel": nivel,
        "decks": []
    }
    try:
        db.jogadores.insert_one(player_data)
    except Exception as e:
        print(f"Erro ao inserir jogador: {e}")

# 2. Inserir uma batalha
def inserir_batalha(tempo_batalha, torres_derrubadas, vencedor, deck_jogador_1, deck_jogador_2, trofeus_jogador_1, trofeus_jogador_2):
    battle_data = {
        "tempo_batalha": tempo_batalha,
        "torres_derrubadas": torres_derrubadas,
        "vencedor": vencedor,
        "deck_jogador_1": deck_jogador_1,
        "deck_jogador_2": deck_jogador_2,
        "trofeus_jogador_1": trofeus_jogador_1,
        "trofeus_jogador_2": trofeus_jogador_2,
        "data": datetime.now(timezone.utc)
    }
    try:
        db.batalhas.insert_one(battle_data)
    except Exception as e:
        print(f"Erro ao inserir batalha: {e}")

# 3. Inserir várias batalhas (exemplo)
def inserir_varias_batalhas(battles):
    for battle in battles:
        try:
            battle_entry = {
                "tempo_batalha": battle['duration'],
                "torres_derrubadas": {
                    "jogador_1": battle['team'][0]['towerDamage'],
                    "jogador_2": battle['team'][1]['towerDamage']
                },
                "vencedor": battle['winner'],
                "deck_jogador_1": [card['name'] for card in battle['team'][0]['cards']],
                "deck_jogador_2": [card['name'] for card in battle['team'][1]['cards']],
                "trofeus_jogador_1": battle['team'][0]['trophies'],
                "trofeus_jogador_2": battle['team'][1]['trophies'],
                "data": battle['battleTime']  # Ou qualquer outra data que você tenha
            }
            db.batalhas.insert_one(battle_entry)
        except Exception as e:
            print(f"Erro ao inserir batalha: {e}")

# 4. Consulta 1 - Calcular porcentagem de vitórias e derrotas
def calcular_porcentagem_vitorias_derrotas(carta_x, inicio_timestamp, fim_timestamp):
    pipeline = [
        {
            "$match": {
                "data": {"$gte": inicio_timestamp, "$lte": fim_timestamp},
                "$or": [
                    {"deck_jogador_1": carta_x},
                    {"deck_jogador_2": carta_x}
                ]
            }
        },
        {
            "$group": {
                "_id": "$vencedor",
                "vitorias": {"$sum": 1},
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$project": {
                "porcentagem_vitorias": {"$multiply": [{"$divide": ["$vitorias", "$total_batalhas"]}, 100]},
                "porcentagem_derrotas": {"$subtract": [100, {"$multiply": [{"$divide": ["$vitorias", "$total_batalhas"]}, 100]}]}
            }
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados

# 5. Consulta 2 - Listar decks por vitórias
def listar_decks_por_vitorias(min_vitorias_percent, inicio_timestamp, fim_timestamp):
    pipeline = [
        {
            "$match": {
                "data": {"$gte": inicio_timestamp, "$lte": fim_timestamp}
            }
        },
        {
            "$group": {
                "_id": {
                    "deck_jogador_1": "$deck_jogador_1",
                    "deck_jogador_2": "$deck_jogador_2"
                },
                "vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", "$deck_jogador_1"]}, 1, 0]}
                },
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$gt": [{"$multiply": [{"$divide": ["$vitorias", "$total_batalhas"]}, 100]}, min_vitorias_percent]
                }
            }
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados

# 6. Consulta 3 - Calcular derrotas de combo
def calcular_derrotas_combo(cartas_combo, inicio_timestamp, fim_timestamp):
    pipeline = [
        {
            "$match": {
                "data": {"$gte": inicio_timestamp, "$lte": fim_timestamp},
                "deck_jogador_2": {"$all": cartas_combo}
            }
        },
        {
            "$match": {
                "$expr": {"$ne": ["$vencedor", "$deck_jogador_2"]}
            }
        },
        {
            "$count": "quantidade_derrotas"
        }
    ]
    resultado = list(db.batalhas.aggregate(pipeline))
    return resultado[0]['quantidade_derrotas'] if resultado else 0

# 7. Consulta 4 - Calcular vitórias condicionais
def calcular_vitorias_condicionais(carta_x, trofeus_perc, inicio_timestamp, fim_timestamp):
    pipeline = [
        {
            "$match": {
                "data": {"$gte": inicio_timestamp, "$lte": fim_timestamp},
                "$or": [
                    {"deck_jogador_1": carta_x},
                    {"deck_jogador_2": carta_x}
                ]
            }
        },
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$lt": ["$trofeus_jogador_1", {"$multiply": ["$trofeus_jogador_2", (100 - trofeus_perc) / 100]}]},
                        {"$lt": ["$tempo_batalha", 120]},
                        {"$gte": ["$torres_derrubadas.jogador_2", 2]}
                    ]
                }
            }
        },
        {
            "$count": "quantidade_vitorias"
        }
    ]
    resultado = list(db.batalhas.aggregate(pipeline))
    return resultado[0]['quantidade_vitorias'] if resultado else 0

# 8. Consulta 5 - Listar combo de cartas
def listar_combo_cartas(min_vitorias_percent, tamanho_n, inicio_timestamp, fim_timestamp):
    pipeline = [
        {
            "$match": {
                "data": {"$gte": inicio_timestamp, "$lte": fim_timestamp}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$eq": [{"$size": "$deck_jogador_1"}, tamanho_n]
                }
            }
        },
        {
            "$group": {
                "_id": "$deck_jogador_1",
                "vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", "$deck_jogador_1"]}, 1, 0]}
                },
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$gt": [{"$multiply": [{"$divide": ["$vitorias", "$total_batalhas"]}, 100]}, min_vitorias_percent]
                }
            }
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados

# Exemplo de uso
if __name__ == "__main__":
    # Insira jogadores e batalhas conforme necessário
    inserir_jogador("Jogador1", 120, 1500, 10)
    inserir_batalha(300, {"jogador_1": 1, "jogador_2": 0}, "Jogador1", ["Carta1", "Carta2"], ["Carta3", "Carta4"], 1500, 1450)

    # Execute consultas conforme necessário
    print(calcular_porcentagem_vitorias_derrotas("Carta1", datetime(2023, 1, 1), datetime(2023, 12, 31)))

def calcular_porcentagem_vitorias_derrotas(carta, data_inicial, data_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": data_inicial,
                    "$lte": data_final
                },
                "$or": [
                    {"deck_jogador_1": carta},
                    {"deck_jogador_2": carta}
                ]
            }
        },
        {
            "$group": {
                "_id": None,
                "total_vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", carta]}, 1, 0]}
                },
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$project": {
                "porcentagem_vitorias": {
                    "$multiply": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, 100]
                },
                "porcentagem_derrotas": {
                    "$multiply": [{"$divide": [{"$subtract": ["$total_batalhas", "$total_vitorias"]}, "$total_batalhas"]}, 100]
                }
            }
        }
    ]
    resultado = list(db.batalhas.aggregate(pipeline))
    return resultado

def listar_decks_com_vitorias_porcentagem(min_porcentagem, data_inicial, data_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": data_inicial,
                    "$lte": data_final
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "deck_jogador_1": "$deck_jogador_1",
                    "deck_jogador_2": "$deck_jogador_2"
                },
                "total_vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", "$_id.deck_jogador_1"]}, 1, 0]}
                },
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$gt": [
                        {"$multiply": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, 100]},
                        min_porcentagem
                    ]
                }
            }
        },
        {
            "$project": {
                "deck": "$_id",
                "porcentagem_vitorias": {
                    "$multiply": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, 100]
                }
            }
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados

def calcular_derrotas_combo(cartas, data_inicial, data_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": data_inicial,
                    "$lte": data_final
                },
                "deck_jogador_1": {"$all": cartas}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$ne": ["$vencedor", "$deck_jogador_1"]
                }
            }
        },
        {
            "$count": "total_derrotas"
        }
    ]
    resultado = list(db.batalhas.aggregate(pipeline))
    return resultado

def calcular_vitorias_com_carta_especifica(carta, porcentagem_trofeus, data_inicial, data_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": data_inicial,
                    "$lte": data_final
                },
                "$or": [
                    {"deck_jogador_1": carta},
                    {"deck_jogador_2": carta}
                ]
            }
        },
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$lt": [{"$subtract": ["$trofeus_jogador_1", "$trofeus_jogador_2"]}, 0]},
                        {"$lt": ["$tempo_batalha", 120]},
                        {"$gte": ["$torres_derrubadas_jogador_2", 2]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$vencedor",
                "total_vitorias": {"$sum": 1}
            }
        }
    ]
    resultado = list(db.batalhas.aggregate(pipeline))
    return resultado

def listar_combos_cartas(min_porcentagem, tamanho_combo, data_inicial, data_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": data_inicial,
                    "$lte": data_final
                }
            }
        },
        {
            "$group": {
                "_id": "$deck_jogador_1",
                "total_vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", "$deck_jogador_1"]}, 1, 0]}
                },
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gt": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, min_porcentagem]},
                        {"$eq": [{"$size": "$_id"}, tamanho_combo]}
                    ]
                }
            }
        },
        {
            "$project": {
                "combo": "$_id",
                "porcentagem_vitorias": {
                    "$multiply": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, 100]
                }
            }
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados

def cartas_mais_utilizadas(data_inicial, data_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": data_inicial,
                    "$lte": data_final
                }
            }
        },
        {
            "$unwind": "$deck_jogador_1"
        },
        {
            "$group": {
                "_id": "$deck_jogador_1",
                "total_utilizacoes": {"$sum": 1},
                "total_vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", "$deck_jogador_1"]}, 1, 0]}
                }
            }
        },
        {
            "$project": {
                "taxa_vitoria": {
                    "$multiply": [{"$divide": ["$total_vitorias", "$total_utilizacoes"]}, 100]
                }
            }
        },
        {
            "$sort": {"total_utilizacoes": -1}
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados

def combos_desbalanceados(media_vitoria, data_inicial, data_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": data_inicial,
                    "$lte": data_final
                }
            }
        },
        {
            "$group": {
                "_id": "$deck_jogador_1",
                "total_vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", "$deck_jogador_1"]}, 1, 0]}
                },
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$or": [
                        {"$gt": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, media_vitoria]},
                        {"$lt": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, media_vitoria]}
                    ]
                }
            }
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados

def analise_tempo(cartas, periodo_inicial, periodo_final):
    pipeline = [
        {
            "$match": {
                "data": {
                    "$gte": periodo_inicial,
                    "$lte": periodo_final
                },
                "$or": [
                    {"deck_jogador_1": {"$in": cartas}},
                    {"deck_jogador_2": {"$in": cartas}}
                ]
            }
        },
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$data"}},
                "total_vitorias": {
                    "$sum": {"$cond": [{"$eq": ["$vencedor", "$deck_jogador_1"]}, 1, 0]}
                },
                "total_batalhas": {"$sum": 1}
            }
        },
        {
            "$project": {
                "taxa_vitoria": {
                    "$multiply": [{"$divide": ["$total_vitorias", "$total_batalhas"]}, 100]
                }
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
    resultados = list(db.batalhas.aggregate(pipeline))
    return resultados
