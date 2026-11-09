# Smart Logistics Data & ML Platform

Решение кандидата на позицию Middle Data Engineer в **«Умную Логистику»**: сквозной прототип аналитики и прогнозирования тарифов грузоперевозок поверх снимка `deals_bigdata` (`as_of = 2026-10-26`).

Платформа считает **рыночный коридор ставок** (медиана и квартили в `руб./рейс` и `руб./км`), **доказательную базу** по маршруту и **ML-прогноз** справедливого тарифа перевозчика с записью факт vs прогноз в БД.

```mermaid
flowchart LR
    SRC["Источник<br/>PostgreSQL 17<br/>sql/schema.sql + synthetic.sql"] --> ETL["ETL & DQ<br/>валидация, FX→RUB,<br/>идемпотентная загрузка"]
    ETL --> CORE["Core DWH<br/>core.fact_prices"]
    CORE --> MART["Mart<br/>mart.route_card"]
    CORE --> ML["ML Feature + CatBoost<br/>time split + batch scoring"]
    ML --> PRED["mart.ml_price_predictions"]
    MART --> CH["★ ClickHouse Mart"]
    PRED --> CH
    CH --> BI["★ Superset dashboard"]
    MART --> BI
```

---

## Архитектура

Слои в **PostgreSQL 17**:

| Слой | Назначение |
| :--- | :--- |
| `public` | Операционный источник (`sql/schema.sql` + `sql/synthetic.sql`) |
| `core.fact_prices` | Очищенные сделки с `price_rub`, категории веса/объёма, каноническая группа кузова |
| `dq.quarantine_prices` | Карантин невалидных строк с кодом причины |
| `mart.route_card` | Карточка маршрута и рыночный коридор |
| `mart.ml_price_predictions` | Batch-скоринг: факт vs прогноз |
| `etl_meta.runs` / `etl_meta.ml_runs` | Аудит прогонов ETL и метрик ML |

**Поток данных**

1. `etl.bootstrap_db` — DDL источника, синтетика при пустой `prices`, DDL `sql/dwh/*.sql`.
2. `etl.run_etl` — валидация / FX / staging → `DELETE+INSERT` в Core и DQ → точечный refresh `mart.route_card`.
3. `ml.train_predict` — Feature Pipeline, хронологический train/test, обучение CatBoost, batch inference в `mart.ml_price_predictions`.
4. ★ `etl.sync_clickhouse` — full refresh копий витрин в ClickHouse (`ReplacingMergeTree` + `EXCHANGE TABLES`).
5. ★ Airflow DAGs в `dags/` и дашборд Superset поверх Postgres/ClickHouse.

---

## Обоснование стека

| Компонент | Выбор | Почему |
| :--- | :--- | :--- |
| СУБД | **PostgreSQL 17** | Один контейнер на обязательном контуре: транзакции, индексы, оконные функции и перцентили для Core/Mart без отдельного OLAP. |
| Оркестратор (база) | **SQL ELT + Python** (`python -m etl.run_etl`) | Идемпотентный батч поднимается через `docker compose up`; отдельный планировщик на обязательном этапе не нужен. |
| Оркестратор (★) | **Apache Airflow** | Ежедневный инкремент и ручной backfill с ML/ClickHouse без дублирования бизнес-логики. |
| ML | **CatBoostRegressor** | Табличные признаки со смешанными категориями и числами; фиксированный seed и один поток для воспроизводимости; границы окон от `AS_OF_DATE`. |
| ★ OLAP | **ClickHouse** | Копия витрин на `ReplacingMergeTree` для аналитических чтений. |
| ★ BI | **Apache Superset** | Демо-дашборд «Smart Logistics — Market Corridor». |

Конфигурация — через [`.env.example`](.env.example). В Compose `DATABASE_URL` собирается из `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`.

---

## Структура репозитория

```text
.
├── README.md
├── docker-compose.yml          # db + app; профиль bonus → Airflow / ClickHouse / Superset
├── Dockerfile                  # обязательный сервис app
├── .env.example
├── requirements.txt
├── etl/                        # Вопрос 1: валидация, FX, идемпотентная загрузка
│   ├── bootstrap_db.py
│   ├── run_etl.py
│   ├── sync_clickhouse.py      # ★
│   └── sql/                    # SQL-трансформации Core / route_card
├── sql/
│   ├── schema.sql              # DDL операционного источника
│   ├── synthetic.sql           # детерминированный снимок (~300k сделок)
│   ├── dwh/                    # DDL Core / Mart / DQ / meta
│   └── clickhouse/             # ★ DDL витрин
├── ml/                         # Вопрос 3: features, train, batch scoring
│   └── train_predict.py
├── dags/                       # ★ Airflow
│   └── ul_etl_dag.py           # ul_etl_daily + ul_etl_ml_clickhouse
├── docker/                     # entrypoint, Airflow/Superset образы
├── tests/                      # pytest (unit + integration)
└── attachments/                # продуктовый контекст и ML-референс
```

---

## Быстрый старт

### 1. Обязательный контур (Postgres + ETL + ML)

```bash
cp .env.example .env   # при необходимости поправьте пароль/порт
docker compose up --build
```

Сервис `app` после готовности Postgres выполнит:

1. `python -m etl.bootstrap_db`
2. `python -m etl.run_etl`
3. `python -m ml.train_predict`

Логи метрик ML — в stdout контейнера `ul-de-app`. После строки `Done` контейнер завершается с кодом `0`, сервис `db` продолжает работать.

На полном снимке 300 000 сделок (когда Postgres уже принимает подключения) сам пайплайн занимает около минуты: bootstrap ~35 с, ETL ~14 с, ML ~14 с. Первый `docker compose up --build` дольше из‑за сборки образа. Повторный запуск пересчитывает тот же интервал идемпотентно.

### 2. Backfill ETL за выбранный период

Окно `[start, end)` (полуинтервал) и снимок `as_of`:

```bash
ETL_START_DATE=2026-10-01 ETL_END_DATE=2026-11-01 \
AS_OF_DATE=2026-10-26 python -m etl.run_etl
```

В Docker:

```bash
docker compose run --rm \
  -e ETL_START_DATE=2026-10-01 \
  -e ETL_END_DATE=2026-11-01 \
  -e AS_OF_DATE=2026-10-26 \
  app python -m etl.run_etl
```

Если backfill меняет состав `core.fact_prices` в train/test/future окнах, перезапустите скоринг:

```bash
AS_OF_DATE=2026-10-26 python -m ml.train_predict
```

### 3. Бонус ★: Airflow / ClickHouse / Superset

```bash
# базовый пайплайн (если ещё не поднимали)
docker compose up --build

# ClickHouse + Airflow + Superset
# на Linux: export AIRFLOW_UID=$(id -u)
docker compose --profile bonus up --build
```

После healthy ClickHouse синхронизируйте витрины (или включите `CLICKHOUSE_SYNC_ENABLED=1` и перезапустите `app`):

```bash
docker compose exec app python -m etl.sync_clickhouse
```

| Сервис | URL | Доступ |
| :--- | :--- | :--- |
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Superset | http://localhost:8088 | `admin` / `admin` |
| ClickHouse HTTP | http://localhost:8123 | `default` / `clickhouse` |

**DAGs**

- `ul_etl_daily` — ежедневный инкремент своего data interval + sync ClickHouse, **без** переобучения модели.
- `ul_etl_ml_clickhouse` — ручной backfill: `run_etl` → `train_predict` → `sync_clickhouse`. Trigger with config:

```json
{"start_date": "2026-10-01", "end_date": "2026-11-01", "as_of": "2026-10-26"}
```

**ClickHouse.** DDL: [`sql/clickhouse/01_mart_route_card.sql`](sql/clickhouse/01_mart_route_card.sql). Синхронизация пишет в staging и делает `EXCHANGE TABLES`, поэтому обрыв пачки не опустошает опубликованную витрину.

```sql
SELECT count() FROM mart.route_card FINAL;
SELECT count() FROM mart.ml_price_predictions FINAL;
```

**Superset.** Bootstrap создаёт источники PostgreSQL DWH и ClickHouse Mart, датасеты витрин и дашборд **Smart Logistics — Market Corridor** (`/superset/dashboard/smart-logistics-corridor/`).

### 4. Локальный запуск без Docker

Нужны PostgreSQL 17, Python 3.11+, клиент `psql`.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/deals_bigdata
python -m etl.bootstrap_db
python -m etl.run_etl
python -m ml.train_predict
pytest -q
```

Интеграционные тесты (`integration`) создают временную БД `ul_pytest_<pid>` и удаляют её в конце сессии. Если Postgres недоступен, они пропускаются; в CI (`CI=1` / `PYTEST_REQUIRE_DB=1`) пропуск считается ошибкой.

---

## ETL (Вопрос 1)

Код: [`etl/run_etl.py`](etl/run_etl.py) + SQL в [`etl/sql/`](etl/sql/).

**Валидация и карантин**

- Отсев: `price <= 0`, для RUB также `price <= 1000`, `distance <= 0`, отрицательные вес/объём, город вне справочника.
- Отбракованные строки сохраняются в `dq.quarantine_prices` с кодом причины.
- `outlier_mark = true` исключается из рыночной витрины; флаг из источника не пересчитывается.

**Конвертация валют**

- USD/EUR → RUB по курсу на `nearest_rate_date` (`exchange_rates.setting_date = nearest_rate_date`).
- Без точного курса строка уходит в карантин (без fallback по `loading_date`).

**Идемпотентность**

- Параметры: `ETL_START_DATE`, `ETL_END_DATE`, `AS_OF_DATE`.
- Повторный прогон удаляет строки и по сохранённой, и по текущей дате источника, затем вставляет заново в одной транзакции с refresh затронутых ячеек `route_card`.
- Одна сделка попадает в каждую связанную бизнес-группу кузова без дублей внутри группы; повторные `price_dates` внутри месяца не увеличивают вес сделки.

Инструкция повторного запуска — в разделе [Backfill](#2-backfill-etl-за-выбранный-период).

---

## Аналитический слой (Вопрос 2)

DDL: [`sql/dwh/`](sql/dwh/) (`01_schemas.sql` … `09_ml_runs.sql`).

Эталонный расчёт карточки маршрута:

- подготовка: [`etl/sql/refresh_route_card_prepare.sql`](etl/sql/refresh_route_card_prepare.sql)
- вставка: [`etl/sql/refresh_route_card_insert.sql`](etl/sql/refresh_route_card_insert.sql)

Grain витрины `mart.route_card`: **месяц × город погрузки × город выгрузки × группа кузова (`std_id`) × весовая категория**.

Правила выборки: факты (`price_type IN (1,2,3,4,5,9)`), `complex = false`, валидный `price_rub`, без `outlier_mark`, без будущих `price_dates` после `as_of`. Метрики: `p25/p50/p75` за рейс и за км, `trips_count`, `carriers_count`, `cv_rate`, `mom_pct` / `yoy_pct`, `reliability_level`.

### Примеры SQL для проверки витрин

Коридор по маршруту:

```sql
SELECT month, loading_city_gcid, unloading_city_gcid, body_group_std_id, weight_category,
       median_trip, p25_trip, p75_trip, median_km, trips_count, carriers_count,
       reliability_level, mom_pct, yoy_pct
FROM mart.route_card
WHERE loading_city_gcid = 1001 AND unloading_city_gcid = 1003
ORDER BY month DESC
LIMIT 12;
```

Сводка скоринга:

```sql
SELECT
  COUNT(*) FILTER (WHERE actual_price_rub IS NOT NULL) AS scored_with_actual,
  COUNT(*) FILTER (WHERE actual_price_rub IS NULL) AS future_only,
  AVG(ape_pct) FILTER (WHERE actual_price_rub IS NOT NULL) AS avg_ape_pct
FROM mart.ml_price_predictions;
```

Прогноз × карточка маршрута:

```sql
SELECT rc.month, rc.median_trip, rc.p25_trip, rc.p75_trip, p.predicted_price_rub
FROM mart.route_card rc
JOIN mart.ml_price_predictions p
  ON p.loading_city_gcid = rc.loading_city_gcid
 AND p.unloading_city_gcid = rc.unloading_city_gcid
 AND p.body_group_std_id = rc.body_group_std_id
 AND p.weight_category = rc.weight_category
 AND rc.month = date_trunc('month', p.loading_date)::date;
```

---

## ML (Вопрос 3)

Код: [`ml/train_predict.py`](ml/train_predict.py).

**Feature Pipeline**

- Гео из дедуплицированного `cities` (`MIN(id)` на `gc_id`): регионы из `full_name_local`, координаты.
- Параметры груза/маршрута: `distance`, вес, объём, каноническая группа кузова (`MIN(std_id)`), категории.
- Циклические признаки даты (`sin`/`cos` для месяца, дня недели, дня года).
- Лаг `lag_median_trip` — медиана той же ячейки за **предыдущий** календарный месяц (без текущего).
- Без утечек: не используются `price_with_vat`, `price/distance`, маржа, `price_group_stats`, `outlier_mark` как фича.

**Обучение**

- Таргет — ставка перевозчика: `price_type IN (3, 9)`.
- Time split от `AS_OF_DATE=2026-10-26`:
  - train: `[2026-07-26, 2026-10-05)`
  - test: `[2026-10-05, 2026-10-26]`
- По умолчанию `ML_TUNE=0`: `depth=8`, `learning_rate=0.08`. `ML_TUNE=1` — поиск по внутреннему chronological val (последние 2 недели train); holdout test в `eval_set` не передаётся.

**Batch Inference**

- Скорятся тестовое окно перевозчиков и **все будущие заявки** (`loading_date > as_of`), включая `price_type = 6`, сложные маршруты и `outlier_mark = true`.
- Для будущих заявок `actual_price_rub`, `abs_error_rub`, `ape_pct` = `NULL`.
- Полная замена прогнозов — в одной транзакции; метрики пишутся в лог и в `etl_meta.ml_runs`.

### Сводка метрик ML

Полный синтетический набор, 300 000 исходных сделок, `as_of = 2026-10-26`, модель `catboost_regressor_v3`, `ML_TUNE=0`.

| Метрика | Значение |
| :--- | ---: |
| R² | 0.9297 |
| MAPE | 15.22% |
| RMSE | 47 503.67 руб. |
| MAE | 25 986.18 руб. |
| MAPE rare body_group (n=890) | 16.06% |
| MAPE common body_group (n=3 523) | 15.00% |
| Train / test | 8 479 / 4 413 |
| Fit / val (внутри train) | 6 610 / 1 869 |
| Batch scoring | 10 411 (5 998 будущих) |

---

## Переменные окружения

См. [`.env.example`](.env.example). Ключевые:

| Переменная | Назначение |
| :--- | :--- |
| `POSTGRES_*` / `DATABASE_URL` | Подключение к БД |
| `N_PRICES` | Объём синтетики при первом init (по умолчанию `300000`) |
| `AS_OF_DATE` | Бизнес-снимок, от него считаются ML-окна |
| `ETL_START_DATE` / `ETL_END_DATE` | Окно backfill `[start, end)` |
| `ML_TUNE` / `ML_RANDOM_SEED` | Режим подбора гиперпараметров и seed |
| `CLICKHOUSE_*` / `CLICKHOUSE_SYNC_ENABLED` | ★ синхронизация витрин |

---

## Ограничения и допущения

- Сделки за `2026-10-26` ещё не прошли ночной джоб разметки аномалий и имеют `outlier_mark = false` по умолчанию — среди них могут остаться неразмеченные выбросы.
- Будущие сложные маршруты и строки с `outlier_mark = true` скорятся для покрытия, но не участвуют в обучении и исторической рыночной витрине.
- `reliability_level = HIGH` учитывает календарную свежесть последней погрузки в ячейке относительно `as_of`.
- При пустом тестовом сплите в лог пишется предупреждение, метрики на holdout не считаются.

---

## Чеклист артефактов сдачи

- [x] `README.md` — архитектура, обоснование стека, запуск, SQL-примеры, метрики ML (R², MAPE, RMSE, MAE)
- [x] `docker-compose.yml`, `Dockerfile`(ы), `.env.example`
- [x] `etl/` — валидация, очистка, FX, идемпотентная загрузка + инструкция backfill
- [x] `dags/` — ★ `ul_etl_daily` и `ul_etl_ml_clickhouse` (Compose-профиль `bonus`)
- [x] `sql/` — DDL Core/Mart и эталонный SQL карточки маршрута
- [x] `ml/` — Feature Pipeline, хронологическое обучение, batch scoring в БД
