# CDP Platform

> **Customer Data Platform (CDP)** – Hệ thống thu thập, xử lý, quản lý dữ liệu khách hàng đa kênh, phục vụ phân khúc, hành trình, chiến dịch và phân tích hành vi.

Monorepo này bao gồm toàn bộ **microservices**, **tài liệu**, **hạ tầng**, **SDK**, **frontend**, **CI/CD**, **tooling** và **runbook** cho hệ thống CDP.

---

## 📌 Mục tiêu của dự án

- Xây dựng một **CDP hiện đại**, đa tenant, có khả năng scale lớn.
- Chuẩn hóa luồng **ingestion → processing → enrichment → indexing → segmentation → activation**.
- Cung cấp bộ API chuẩn hóa cho:
  - **Tracking**
  - **Profiles / Devices**
  - **Segments**
  - **Journeys**
  - **Campaigns**
  - **Admin portal**
- Đảm bảo **tính toàn vẹn dữ liệu**, **hiệu suất cao**, **độ trễ thấp**, **khả năng mở rộng**.
- Định nghĩa đầy đủ tài liệu: kiến trúc, domain, sequence diagram, API, ops, ADR.

---

## 🏗️ Kiến trúc tổng quan

Sơ đồ kiến trúc tổng quan (chi tiết xem tại [ARCHITECTURE.md](docs/overview/ARCHITECTURE.md)):

```mermaid

%%{init: {'flowchart': { 'rankSpacing': 200, 'nodeSpacing': 140 }}}%%

flowchart LR

classDef nodeStyle padding:25px,margin:40px,stroke-width:1.2px;
class * nodeStyle;

  %% Internet
  subgraph INTERNET["Internet"]
    USER["Admin User (Browser)"]
    SDK["Client SDK (Web/App)"]
  end

  %% Public Zone (DMZ)
  subgraph DMZ["Public Zone / DMZ"]
    DNS["DNS"]
    WAF["WAF / Firewall + Load Balancer"]
    AP_PORTAL_FE["Admin Portal Frontend\n(SPA / Static Web)"]
    APIGW["Public API Gateway\n(/track, /identify, /admin/api)"]
  end

  %% Private App Zone
  subgraph APP["Private App Zone - CDP Core (K8s / App Servers)"]
    MS_ING["Ingestion Service\n(Internal API)"]
    MS_INB["Inbound Processor\n(Validation + Mapping + IR)"]
    MS_EVT["Event Service (API)"]
    MS_PROF["Profile Service (API)"]
    MS_SEG["Segmentation Service (API)"]
    MS_WF["Journey / Workflow Orchestrator"]
    MS_CAMP["Campaign Manager"]
    MS_CAMP_TRACK["Campaign Tracking"]
    MS_CONS["Consent & Privacy"]
    MS_CFG["Config / Metadata Service"]
    MS_AUTH["Auth / SSO / RBAC Backend"]
    MS_ADMIN_BE["Admin Portal Backend API\n(optional)"]
  end

  %% Private Data Zone
  subgraph DATA["Private Data Zone - Stateful"]
    subgraph DBSQL["Relational DB Cluster"]
      RDB["MySQL (Config, Segments, Campaign, WF State)"]
    end

    subgraph ES["Elasticsearch Cluster"]
      ES_DATA["ES Data Nodes\n(Events & Profiles)"]
      ES_MASTER["ES Master Nodes"]
    end

    KAFKA["Kafka Cluster\n(events_raw, events_enriched, segment_events, ...)"]
    REDIS["Redis Cluster"]
    OBJ["Object Storage\n(S3 / MinIO)"]
    WH["Data Warehouse / Lake\n(Impala / Trino / ClickHouse)"]
  end

  %% Observability
  subgraph OBS["Observability & Ops"]
    PROM["Prometheus"]
    GRAF["Grafana"]
    LOGC["Log Collector"]
  end

  %% Outbound Channels (External services)
  subgraph OUT["External Channels / Partners"]
    CH_EMAIL["Email / SMS Provider"]
    CH_WEBHOOK["Webhook / CRM / Ads"]
    CH_INAPP["In-App / Onsite"]
  end

  %% Flows: Internet -> DMZ
  USER --> DNS --> WAF
  SDK --> DNS

  WAF --> AP_PORTAL_FE
  WAF --> APIGW

  %% Admin Portal Frontend -> API Gateway
  AP_PORTAL_FE --> APIGW

  %% API Gateway -> Private App Zone (internal network)
  APIGW --> MS_ING
  APIGW --> MS_ADMIN_BE
  APIGW --> MS_AUTH
  APIGW --> MS_CFG
  APIGW --> MS_SEG
  APIGW --> MS_CAMP
  APIGW --> MS_CAMP_TRACK
  APIGW --> MS_WF
  APIGW --> MS_PROF
  APIGW --> MS_EVT
  APIGW --> MS_CONS

  %% App services internal communication
  MS_ING --> MS_INB
  MS_INB --> MS_EVT
  MS_INB --> MS_PROF
  MS_SEG --> MS_WF
  MS_WF --> MS_CAMP
  

  %% App -> Kafka / Redis
  MS_ING --> KAFKA
  MS_INB --> KAFKA
  MS_EVT --> KAFKA
  MS_PROF --> KAFKA
  MS_SEG --> KAFKA
  MS_WF --> KAFKA
  MS_CAMP --> KAFKA
e
  MS_ING --> REDIS
  MS_INB --> REDIS
  MS_SEG --> REDIS
  MS_WF --> REDIS

  %% App -> DB & ES & Object Storage
  MS_CFG --> RDB
  MS_AUTH --> RDB
  MS_SEG --> RDB
  MS_CAMP --> RDB
  MS_WF --> RDB
  MS_PROF --> RDB
  MS_CAMP_TRACK <--> KAFKA
  MS_CAMP_TRACK --> ES_DATA

  MS_EVT --> ES_DATA
  MS_PROF --> ES_DATA
  MS_SEG --> ES_DATA

  MS_EVT --> OBJ

  %% Analytics / DWH
  ES_DATA --> WH
  RDB --> WH
  OBJ --> WH

  %% Outbound
  MS_CAMP --> CH_EMAIL
  MS_CAMP --> CH_WEBHOOK
  MS_CAMP --> CH_INAPP

  %% Observability wiring
  APP --> LOGC
  DATA --> LOGC

  APP --> PROM
  DATA --> PROM
  KAFKA --> PROM

  PROM --> GRAF