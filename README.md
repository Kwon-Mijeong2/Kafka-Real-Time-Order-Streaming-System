# Kafka Real-Time Order Streaming System

실시간 주문 이벤트를 생성하고 스트리밍 처리하는 데이터 엔지니어링 프로젝트

## 프로젝트 개요

이 프로젝트는 Kafka 기반 실시간 주문 이벤트 처리 파이프라인입니다.

주문 데이터를 실시간 생성하여 Kafka로 전송하고,  
Spark Structured Streaming으로 처리한 뒤 PostgreSQL에 저장하며  
Streamlit Dashboard로 시각화합니다.

---

## 아키텍처

```plaintext
Order Producer
   ↓
Kafka Topics
   ↓
Spark Structured Streaming
   ↓
PostgreSQL
   ↓
Streamlit Dashboard
```

---

## 사용 기술

- Python
- Apache Kafka
- Apache Spark
- PostgreSQL
- Docker
- Streamlit

---

## 주요 기능

### 실시간 주문 이벤트 생성
- 랜덤 주문 데이터 생성
- Kafka orders topic 전송

### 실시간 스트리밍 처리
- Spark Structured Streaming 소비
- JSON 파싱 및 변환

### 데이터 저장
- PostgreSQL 적재

### 실시간 모니터링
- 주문 수
- 총 매출
- 평균 주문 금액
- 상품별 주문 현황

---

## 실행 방법

### 1. Docker 실행

```bash
docker-compose up -d
```

### 2. Producer 실행

```bash
python producer/order_producer.py
```

### 3. Spark Consumer 실행

```bash
python consumer/spark_consumer.py
```

### 4. Dashboard 실행

```bash
streamlit run dashboard/app.py
```

---

## 성과

- Kafka 기반 이벤트 스트리밍 파이프라인 구축
- Spark Structured Streaming 실시간 처리 구현
- PostgreSQL 실시간 적재
- 실시간 대시보드 구축
