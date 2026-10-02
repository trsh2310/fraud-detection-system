import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import altair as alt
import psycopg2

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@postgres:5432/fraud_scores")


def load_results():
    """Возвращает последние мошеннические транзакции и последние 100 скоров."""
    with psycopg2.connect(DATABASE_URL, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT transaction_id, score, fraud_flag, scored_at
                FROM transaction_scores
                WHERE fraud_flag = 1
                ORDER BY scored_at DESC, transaction_id DESC
                LIMIT 10
            """)
            fraud_rows = cursor.fetchall()
            cursor.execute("""
                SELECT score
                FROM transaction_scores
                ORDER BY scored_at DESC, transaction_id DESC
                LIMIT 100
            """)
            scores = [row[0] for row in cursor.fetchall()]
    return fraud_rows, scores

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
upload_tab, results_tab = st.tabs(["Отправка транзакций", "Результаты скоринга"])

with upload_tab:
    st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"]
    )

    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        # Добавляем файл в состояние
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file)
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
    if st.session_state.uploaded_files:
        st.subheader("🗂 Список загруженных файлов")
    
        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([4, 2, 2])
        
            with cols[0]:
                st.markdown(f"**Файл:** `{file_name}`")
                st.markdown(f"**Статус:** `{file_data['status']}`")
        
            with cols[2]:
                if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Отправка..."):
                            success = send_to_kafka(
                                file_data["df"],
                                KAFKA_CONFIG["topic"],
                                KAFKA_CONFIG["bootstrap_servers"]
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                                st.rerun()
                    else:
                        st.error("Файл не содержит данных")

with results_tab:
    st.title("Результаты скоринга")
    if st.button("Посмотреть результаты"):
        try:
            fraud_rows, scores = load_results()
        except psycopg2.Error as exc:
            st.error(f"Не удалось получить результаты из PostgreSQL: {exc}")
        else:
            st.subheader("Последние 10 транзакций с fraud_flag = 1")
            if fraud_rows:
                st.dataframe(
                    pd.DataFrame(fraud_rows, columns=["transaction_id", "score", "fraud_flag", "scored_at"]),
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.info("Транзакций с флагом мошенничества пока нет.")

            st.subheader("Распределение скоров последних 100 транзакций")
            if scores:
                chart = alt.Chart(pd.DataFrame({"score": scores})).mark_bar().encode(
                    x=alt.X("score:Q", bin=alt.Bin(maxbins=20), scale=alt.Scale(domain=[0, 1]), title="Скор"),
                    y=alt.Y("count():Q", title="Количество транзакций"),
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("Скоров в базе пока нет.")
