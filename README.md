# Запуск и проверка

Требуются Docker с Compose v2, Python 3 и свободные порты 2181, 8080, 8501, 9095.

Файлы `train.csv` и `test.csv` из [соревнования Kaggle](https://www.kaggle.com/competitions/teta-ml-1-2025/data) должны находиться в `data/`. Файл модели `fraud_detector/models/my_catboost.cbm` включён в репозиторий.

## Запуск контейнеров

```bash
test -s data/train.csv && test -s data/test.csv && test -s fraud_detector/models/my_catboost.cbm
docker compose up -d --build
docker compose ps --all
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list
```

## Проверка скоринга

Одна строка `test.csv` отправляется в топик `transactions` с уникальным идентификатором:

```bash
CHECK_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
python3 -c 'import csv,json,sys; print(json.dumps({"transaction_id":sys.argv[1],"data":next(csv.DictReader(open("data/test.csv")))}))' "$CHECK_ID" | docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:9092 --topic transactions
```

Результат для того же идентификатора проверяется в топике `scores`:

```bash
docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic scores --from-beginning --timeout-ms 120000 | grep -m1 "$CHECK_ID"
```

JSON с тремя полями: `transaction_id`, `score`, `fraud_flag`. В логах `fraud_detector` после обработки сообщения `Prediction complete`:

```bash
docker compose logs --tail=30 fraud_detector
```

## Проверка PostgreSQL

Результат с тем же идентификатором проверяется в таблице `transaction_scores`:

```bash
docker compose exec -T postgres psql -U fraud -d fraud_scores -c "SELECT transaction_id, score, fraud_flag, scored_at FROM transaction_scores WHERE transaction_id = '$CHECK_ID';"
```


## Проверка интерфейса

<http://localhost:8501>

```bash
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group ml-scorer
docker compose logs --tail=100 fraud_detector scores_writer
```
