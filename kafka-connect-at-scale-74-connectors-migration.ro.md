---
title: "Kafka Connect la Scară Largă: Cazul Migrării a 74 de Conectori"
author: Adrian Balaban
layout: docs
---

# Kafka Connect la Scară Largă: Cazul Migrării a 74 de Conectori

**Autor:** Adrian Balaban  
**Data:** 2026-06-26

În această sesiune vom explora Kafka Connect prin experiența reală a unui client, concentrându-ne pe un proof of concept și o migrare propusă a 74 de conectori din Confluent Kafka Cloud către Flink. Vom parcurge provocările actuale, îmbunătățirile pe care le urmărește noua abordare și compromisurile implicate. Prezentarea evidențiază, de asemenea, alternativele luate în considerare și raționamentul din spatele soluției propuse.

---

## Slide 0 — De ce este util acest talk (ce vei putea face după el)

> Nu doar o relatare a migrării unui client — un **playbook reutilizabil**. După
> acest talk poți reproduce CDC-ul cu Kafka Connect sau cu Flink la alt client.

Cinci lucruri pe care le iei de aici:

1. **Clientul** — cum s-a plecat acum câțiva ani de la o arhitectură **centrată pe DB** și s-a ajuns la una **event-driven** adăugând doar Kafka și un număr de conectori Kafka Connect. Contextul care face rezultatul relevant.
2. **Problemele reale de producție cu KC** — 'rebalancing storms', 'lag' fără posibilitate de interventie per echipă, 'blast radius' partajată pe un singur cluster, licențiere Confluent Cloud.
3. **Flink, conectorii Flink și Debezium pe scurt** — ce sunt, unde se suprapun, unde diferă; Debezium ca parser de binlog reutilizat intern de Flink CDC (si de conector KC).
4. **Flink este complet event-driven** — nu doar un conector, ci un motor de stream processing cu stare, event-time și checkpoint-uri exactly-once, fiecare job ca deployment K8s izolat.
5. **Informațiile + codul POC pentru a face CDC la un alt client** — 5 variante rulând simultan, cod aproape de versiune de producție, infrastructură Podman Compose și K8s reproductibilă, component-tests care validează output-ul Kafka.

> Ținta: la final poți alege între KC și Flink cu argumente — și ai codul de la care pornești, nu de la zero.

---

## Slide 1 — Problema Într-o Propoziție

> Client real. Scară reală. 95 de conectori, 26 de echipe, un cluster partajat — și întrebarea dacă Flink este calea de ieșire corectă.

Migrarea propusă a **74 de conectori MySQL** de la Confluent Kafka Cloud la Flink, cu un proof of concept acoperind toate cele 5 variante.

Prezentarea este pregătită pentru **Comunitatea Java Cognizant România**.

---

## Slide 1b — Agendă (45 de minute)

<!-- notes: Cadru de deschidere: Slide-urile 0–1e ~6 min; blocul de 45 de minute începe la agendă. -->

1. **Unde suntem** (2 min) — Contextul clientului + scopul migrării: 95 de conectori pe un singur cluster din care 74 conectori MySQL, 21 rămân pe KC → *Slide-urile 2–3*
2. **De ce este 'painful' și ce cerem** (5 min) — Provocări + cele 3 cerințe pe care orice soluție trebuie să le indeplinească → *Slide 4*
3. **Ce este Flink și de ce este remedierea structurală** (4 min) — Flink descris pe scurt; izolare per-job → *Slide 5*
4. **POC-ul + dovezi** (10 min) — 5 variante Flink rulând simultan; un snippet de cod; tabelul cu dovezi POC → *Slide-urile 6–8*
5. **Soluția propusă + îmbunătățiri** (5 min) — Modelul shared-job; îmbunătățiri concrete față de provocările de azi → *Slide-urile 10–11*
6. **Arhitectură și evitarea coliziunilor** (8 min) — deployment K8s, intervale server-ID, monitorizare → *Slide-urile 12–13*
7. **Compromisurile** (5 min) — Ce se schimbă, ce rămâne → *Slide 14*
8. **Costul schimbării** (2 min) — TCO: ce se reduce, ce se adaugă → *Slide 15*
9. **Întrebări deschise** (4 min) — 9 spike-uri → *Slide 16*

**Q&A: 15 minute**

<!-- notes: Buget de timp: 45 min agendă + 15 min Q&A. Deschidere 0–1e ~6 min (1c ~75 s, 1d ~3 min, 1e ~1 min). Capturile live (Slide 9) doar dacă timpul permite — nu sunt incluse în cele 45 min. -->

---

## Slide 1c — Context în ~75 de secunde (pentru cei care nu au lucrat cu Kafka Connect & CDC)

```
MySQL binlog  →  Debezium  →  Kafka  →  consumatori
               (capturează     topicuri        (alte sisteme,
                schimbări)                       DB-uri)
```

| Termen | Ce este (o propoziție) |
|--------|------------------------|
| **MySQL binlog** | Jurnalul intern MySQL cu toate INSERT/UPDATE/DELETE — Debezium îl citește și îl replică pe un Kafka topic |
| **Debezium** | Platformă CDC open-source care citește binlog-ul MySQL și emite fiecare INSERT/UPDATE/DELETE ca eveniment JSON structurat |
| **Kafka Connect** | Platforma care rulează Debezium (și alți conectori) ca workere gestionate |
| **SMT** | Single Message Transformer — un plugin KC care modifică fiecare înregistrare în zbor (enrichment, routing) |
| **Confluent Cloud** | Kafka + Kafka Connect ca serviciu managed ([confluent.io/confluent-cloud](https://www.confluent.io/confluent-cloud/) — nu îl administrezi tu, îl plătești) |
| **Apache Flink** | Motor de procesare a stream-urilor; poate face același lucru ca Debezium + KC, dar ca job izolat pe K8s |
| **StatementSet** | Construct Flink Table API care compilează mai multe oper.sql pe mai multe tabele într-un singur JobGraph (ce are un singur checkpoint) |
| **Tabelă outbox** | O tabelă DB scrisă în aceeași tranzacție cu înregistrarea de business; CDC o citește și rutează evenimentul la topicul Kafka potrivit — decuplează publicarea evenimentelor de tabela principală |

> **De reținut:** toate variantele (cu excepția celei de tip outbox) folosesc același lucru — binlog-ul MySQL — și scriu în Kafka.
> Diferența este *cum* și *unde* rulează procesul de citire.

---
## Slide 1c-bis — Tabela Outbox: Ce Este? (deep-dive opțional)

*Slide de rezervă / deep-dive — nu face parte din cadrul de deschidere de ~6 min (Slide-urile 0–1e); arată-l doar dacă timpul permite.*

O tabelă outbox este o tabelă de bază de date folosită în *outbox pattern*, în care sunt stocate mesajele ce urmează a fi trimise către alte servicii sau sisteme. Această abordare asigură că publicarea mesajului face parte din aceeași tranzacție cu actualizarea bazei de date, menținând consistența și fiabilitatea datelor.

*Surse: milanjovanovic.tech · microservices.io*

### Prezentare generală

O tabelă outbox este o componentă esențială în outbox pattern, folosită cu precădere în arhitecturile de microservicii. Servește drept zonă de stocare temporară pentru mesajele care trebuie trimise către alte servicii sau sisteme.

### Scopul tabelei outbox

- **Consistența datelor:** tabela outbox garantează că publicarea mesajului are loc în aceeași tranzacție cu actualizarea bazei de date. Această atomicitate previne inconsistența datelor.
- **Mesagerie fiabilă:** prin stocarea mesajelor în tabela outbox, sistemul poate garanta că mesajele sunt trimise cel puțin o dată, chiar dacă încercarea inițială eșuează.

### Structura tabelei outbox

Tabela outbox include de obicei următoarele coloane:

| Coloană | Tip de date | Descriere |
|---------|-------------|-----------|
| `id` | UUID | Identificator unic pentru fiecare mesaj |
| `type` | VARCHAR(255) | Tipul mesajului (ex. tipul evenimentului) |
| `content` | JSONB | Conținutul efectiv al mesajului |
| `occurred_on_utc` | TIMESTAMP WITH TIME ZONE | Momentul creării mesajului |
| `processed_on_utc` | TIMESTAMP WITH TIME ZONE | Momentul procesării mesajului |
| `error` | TEXT | Mesajul de eroare în caz de eșec al procesării |

### Beneficiile folosirii unei tabele outbox

- **Operații atomice:** garantează că atât actualizarea bazei de date, cât și publicarea mesajului sunt tratate ca o singură operație, prevenind eșecurile parțiale.
- **Decuplare:** permite ca procesul de trimitere a mesajelor să fie gestionat de un serviciu separat, care poate reîncerca trimiterea fără a afecta fluxul principal al aplicației.
- **Consistență eventuală:** deși outbox pattern oferă livrare *at-least-once*, permite consistență eventuală — adică, deși mesajele pot fi trimise de mai multe ori, sistemul le poate gestiona elegant.

---

## Slide 1d — Două Tipare de Conectori: CDC vs Outbox

Două moduri fundamental diferite prin care un conector citește din MySQL și scrie în Kafka.

### Tiparul 1 — CDC: un topic per tabelă de business

```
┌──────────────────────── MySQL ────────────────────────────┐
│                                                           │
│  ┌────────────────┐       ┌──────────────────────────┐   │
│  │    orders      │       │       customers          │   │
│  ├────────────────┤       ├──────────────────────────┤   │
│  │ id │ amount│...│       │ id │ name │ email │  ... │   │
│  └────────────────┘       └──────────────────────────┘   │
│                                                           │
│         binlog — fiecare INSERT / UPDATE / DELETE         │
└──────────────────────┬────────────────────────────────────┘
                       │ conectorul urmărește binlog-ul
                       ▼
              ┌─────────────────┐
              │  Conector CDC   │
              │  (Flink / KC)   │
              └────────┬────────┘
                       │ un topic per tabelă capturată
          ┌────────────┴────────────┐
          ▼                         ▼
   ┌──────────────┐         ┌────────────────┐
   │ poc.flink    │         │ poc.flink      │
   │   .orders    │         │   .customers   │
   └──────────────┘         └────────────────┘
```

Conectorul capturează modificările din fiecare tabelă. 

### Tiparul 2 — Outbox: aplicația scrie intenția; conectorul rutează după destinație

```
┌────────────────────────────── MySQL ──────────────────────────────────────┐
│                                                                           │
│  ┌────────────────┐  aceeași TX  ┌───────────────────────────────────┐   │
│  │    orders      │  ──COMMIT──▶ │         outbox_events             │   │
│  ├────────────────┤              ├───────────────────────────────────┤   │
│  │ id │ amount│...│              │ id │ destination │ payload │  ... │   │
│  └────────────────┘              │    │ "payments"  │ { ... } │       │   │
│                                  │    │ "fraud"     │ { ... } │       │   │
│                                  └───────────────────────────────────┘   │
│                                                                           │
│           binlog — conectorul urmărește doar outbox_events                │
└───────────────────────────────────┬───────────────────────────────────────┘
                                    │ rutează după câmpul destination
                                    ▼
                          ┌──────────────────┐
                          │ Conector Outbox  │
                          │  (Flink / KC)    │
                          └────────┬─────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
       ┌────────────┐      ┌─────────────┐      ┌─────────────┐
       │  payments  │      │    fraud    │      │  analytics  │
       │   .events  │      │   .alerts   │      │    .feed    │
       └────────────┘      └─────────────┘      └─────────────┘
```

Aplicația controlează forma evenimentului și destinația. Schema tabelei de business nu ajunge niciodată la consumatori — doar payload-ul cizelat din rândul outbox ajunge în Kafka.

> **Diferența cheie:** CDC expune modificările fiecărei tabele ca atare (un topic per tabelă; deriva schemei se propagă).
> Outbox oferă aplicației control complet asupra formei evenimentului și topicului destinatar —> o singură tabelă outbox → mai multe topicuri, rutate după câmpul `destination` scris la INSERT.

---

## Slide 1e — Ce Conține POC-ul (Privire de ansamblu)

**POC-ul demonstrează 5 variante Flink CDC + 5 conectori Kafka Connect echivalenți, rulând simultan pe același stack local (podman-compose și K8s).**

| Flink Dashboard — 5/5 joburi RUNNING | Kafka Connect REST — 5 conectori |
|---|---|
| ![Flink Dashboard](images/slides/flink-dashboard.png) | ![Kafka Connect](images/slides/kafka-connect.png) |

```
                ┌──────────────────────── MySQL (poc_db) ─────────────────────────┐
                │   orders · customers · outbox_events   —   binlog ROW / FULL     │
                └─────────────────────────────┬───────────────────────────────────┘
                                              │  ACELAȘI binlog, citit de ambele motoare
                  ┌───────────────────────────┴───────────────────────────┐
                  ▼                                                         ▼
        ╔═══════════════════════════╗                       ╔═══════════════════════════╗
        ║       KAFKA CONNECT       ║                       ║       APACHE FLINK        ║
        ║   Debezium · workere      ║                       ║   Flink CDC · 1 job =     ║
        ║   pe 1 cluster partajat   ║                       ║   1 deployment K8s izolat ║
        ║   topicuri:  poc.kc.<x>   ║                       ║ topicuri: poc.flink.<x>.. ║
        ╚═════════════╤═════════════╝                       ╚═════════════╤═════════════╝
                      │            aceleași 5 tipare, două implementări    │
                      └───────────────────────────┬───────────────────────┘
                                                  ▼
┌───┬──────────────────┬──────────────────────┬──────────────────────────────────┐
│ # │ Variantă (tipar) │ Kafka Connect        │ Apache Flink                     │
├───┼──────────────────┼──────────────────────┼──────────────────────────────────┤
│ 1 │ DataStream API   │ 5510 · datastream    │ 5900–5999 · datastream.orders    │
│ 2 │ Table API        │ 5520 · table-api     │ 6000–6099 · table-api.orders     │
│ 3 │ SQL API          │ 5530 · sql-api       │ 5800–5899 · sql-api.orders       │
│ 4 │ Outbox           │ 5550 · outbox        │ 5600–5699 · outbox.outbox-events │
│ 5 │ YAML Pipeline    │ 5540 · yaml-pipeline │ 5700–5709 · yaml-pipeline.orders │
└───┴──────────────────┴──────────────────────┴──────────────────────────────────┘

  Coloanele motoarelor arată:  server-ID  ·  sufixul topicului Kafka
  KC    → poc.kc.<sufix>                 (un singur topic per conector)
  Flink → poc.flink.<sufix>              (job izolat, checkpoint exactly-once)
  Total: 5 tipare × 2 motoare = 10 implementări CDC rulând simultan pe același MySQL.
```

> Ambele 'motoare' (KC și Flink) rulează în paralel pentru comparație (input și output). Capturile de ecran complete și detaliile in Slide 9.

---

## Slide 2 — Contextul Clientului (Unde Suntem Azi)

```
26 de echipe  ─►  ┌───────────────────────────────┐
                  │  1 cluster KC partajat        │
                  │  95 conectori (74 MySQL + 21) │  blast radius = 1
                  │  1 grup de rebalancing        │
                  └───────────────────────────────┘
```

**Experiență reală cu un client: Confluent Kafka Cloud la scară**

- **95 de conectori** pe **un singur cluster Kafka Connect partajat** pentru **26 de echipe**
- Două familii de conectori astăzi:
  - **Debezium (Kafka Connect)** — citește binlog-ul MySQL via KC gestionat de Confluent, un eveniment per modificare per topic
  - Conectori sink/source SFTP + SingleStore
- Totul partajează un singur cluster: o configurație, un singur grup de rebalancing, o singură rază de impact

> Clusterul partajat era convenabil la 5 conectori. La 95 pe 26 de echipe — și
> în creștere — este cea mai mare sursă de incidente inter-echipe. Această
> presiune de scalare este motivul pentru care investigăm acum.

---

## Slide 3 — Scopul Migrării

**Ce migrăm:** 74 de conectori MySQL → Apache Flink MySQL CDC Connector

**Ce rămâne pe Kafka Connect:** 21 de conectori SFTP + SingleStore (Flink nu are echivalent)

![Pattern Migrare: Înainte și După](images/migration-before-after.svg)

---

## Slide 4 — Ce este 'painful' astăzi și cerem de la orice soluție


**Ce este 'painful' azi — și care sunt costurile:**

| Problemă | Cine | Cât de des | Impact business |
|----------|------|-----------|-----------------|
| 'Rebalancing storms' — un conector defect destabilizează tot | Toate cele 26 de echipe | De mai multe ori/trimestru | Incidente inter-echipe; downtime consumatori |
| 'Blast radius' partajată — 95 de conectori, un cluster | Toate cele 26 de echipe | La fiecare incident | Fără izolare între echipe |
| 'Lag' recurent — fără instrumente 'per team' | Team + consumatori | Continuu | Risc SLA pe consumatorii downstream |
| Erori care se manifestă doar la deploy noi conectori și doar în producție | Când se adaugă conectori noi | Rar în ultima vreme | Erori ce ajung în prod. nedetectate |
| Licențiere Confluent Kafka Cloud | Organizația | Lunar | **Cost lunar de licențiere semnificativ** (este pe numar de noduri, vezi Slide 15 TCO) |
| Patch-uri de securitate Confluent doar la 3 luni | Echipa de mentenanță KC | La fiecare ciclu de release și la fiecare vulnerabilitate fixată | Risc de erori la patch-urile făcute de Echipa de mentenanță KC |

**Ce e un „rebalancing" în Kafka Connect**
Un cluster Kafka Connect e un grup de workeri care își împart între ei conectorii și task-urile lor. Distribuția asta o coordonează protocolul de group membership al Kafka. De câte ori se schimbă ceva — un worker intră/iese, adaugi/modifici/ștergi un conector, un task pică — clusterul declanșează un rebalance: recalculează „cine ce task rulează" și redistribuie sarcinile peste toți workerii.

De ce e „1 grup":
Pentru că toți cei 95 de conectori ai celor 26 de echipe stau pe un singur cluster partajat, ei fac parte din același grup de rebalancing — un singur grup de coordonare. Nu există izolare: nu poți „rebalansa" doar conectorii echipei A fără să atingi restul.

De ce contează (asta e poanta slide-ului):
Cu protocolul clasic de rebalance („eager" / stop-the-world), un rebalance oprește temporar TOATE task-urile din tot clusterul și le reasignează. Deci o singură schimbare/un singur conector defect al unei echipe → declanșează un rebalance care pune pe pauză conectorii tuturor celor 26 de echipe în același timp.


**Ce cerem de la orice soluție** *(agnostic față de soluție — etalonul pentru opțiunile de pe Decision Matrix de mai jos):*

1. Imaginea de bază + patch-urile de securitate rămân **centralizate** — echipele nu dețin runtime-ul.
2. **Să ne îndepărtăm de Confluent Platform** — licențiere și lock-in.
3. **Împărțirea în clustere per echipă, cu aceeași soluție, nu rezolvă problemele**.


**Decision Matrix: Ce Variantă folosim pentru Ce Tip De Conector?**

![Arbore de Decizie Conector: Ce Variantă pentru Ce Conector?](images/connector-decision-tree.svg)

| Tip Conector | Varianta Recomandată | De Ce |
|-------------------|--------------------|----|
| Outbox (tranzacțional, rutare per-rând) | DataStream | Table/SQL API nu pot face rutare per-rând |
| CDC cu îmbogățire/transformare personalizată | DataStream CDC | Acces Java la `CdcEventRouter` + `MapFunction` personalizat |
| CDC simplu (tabel → topic, fără transformare) | YAML Pipeline/SQL API | Zero Java |
| CDC cu join-uri/agregări SQL viitoare | Table API | Simplifică sistemul |

---

## Slide 5 — Ce Este Flink și De Ce Este Remedierea Propusă Structurală

**Apache Flink** este un motor de procesare a stream-urilor cu stare (stateful): un job continuu care citește evenimente, menține stare și scrie rezultate — cu **checkpoint-uri exactly-once** (durabile, recuperabile). Fiecare job rulează ca **propriul deployment K8s izolat** (JobManager + TaskManager proprii) sub Flink Operator.

**Flink + MySQL Connector** cât și **Flink CDC** fac același lucru ca Debezium-on-Kafka-Connect — citește binlog-ul MySQL și emite evenimente CDC către Kafka.

Argumentul structural într-un singur cadru — aceasta este puntea de la "ce e 'painful'" la "de ce Flink remediază":

> **"CDC" înseamnă două lucruri — nu le confunda:** (1) **Flink CDC `MySqlSource`** — conectorul care citește binlog-ul MySQL prin algoritmul propriu de snapshot incremental Flink CDC (variantele 1–4: DataStream / Table API / SQL API / Outbox; reutilizează intern parserul de binlog Debezium, dar **nu** rulează pe conectorul Debezium Kafka Connect). (2) **Flink CDC YAML pipeline** — *framework-ul declarativ de pipeline YAML* deasupra aceleiași surse, fără Java (Varianta 5). Această prezentare acoperă ambele sensuri.

| | Kafka Connect azi | Flink (propus) |
|--|--|--|
| Deployment | 1 cluster partajat de workere | N joburi K8s izolate (prin Flink Operator) grupate pe team-uri |
| Blast radius | 1 — toate cele 95 de conectori | 1 per team — limitată |
| Rebalancing storms | un grup → cascadă pe 26 de echipe | niciuna |
| Licențiere | Confluent Cloud (cu plată) | Apache 2.0 (gratuit) |

---

## Slide 6 — POC-ul: Cinci Variante Flink

Deoarece Flink are 4 API-uri plus Flink CDC (pipeline configurat cu yaml), am construit **5 variante** care sunt rulate **simultan**.

| # | Variantă | Dimensiune Clasă Principală | Format Output | Java este Necesar ? |
|---|---------|-----------|---------------|---------------|
| 1 | CDC cu Flink DataStream API | 63 de linii | envelope Debezium + îmbogățire | Da |
| 2 | CDC cu Flink Table API | 99 linii | Rând aplatizat (upsert-kafka) | Da |
| 3 | CDC cu Flink SQL API | 156 linii | Rând aplatizat (upsert-kafka) | Minim |
| 4 | CDC cu Flink CDC (YAML Pipeline) | 52 linii YAML | envelope Debezium nativ | **Nu** |
| 5 | Outbox cu Flink DataStream API | 56 linii | envelope Debezium al rândului outbox (topic unic; routing per destinație este producție, nu în POC) | Da |

> Toate cele patru variante cu API partajează în plus ~412 linii de infrastructură `common/`(`JobConfig`, `CheckpointConfigurer`, deserializator, routere KafkaSinkFactory`) — clasele de intrare conțin doar codul specific variantei.

---

## Slide 7 — Perspectiva Programatorului Java: Comparație de Cod

### CDC cu DataStream API (clasă de intrare 63 de linii, control maxim)

```java
MySqlSource<String> source = MySqlSource.<String>builder()
    .hostname(config.mysqlHost).port(config.mysqlPort)
    .databaseList(config.mysqlDatabase).tableList(config.mysqlTables)
    .username(config.mysqlUser).password(config.mysqlPassword)
    .serverTimeZone("UTC")
    .serverId(config.serverId)
    .deserializer(new PocJsonDeserializationSchema())
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
   .process(new CdcEventRouter(config))
   .sinkTo(KafkaSinkFactory.create(config, "datastream"));
```

> Toate detaliile de conexiune vin din `JobConfig.fromEnv()` — nimic nu este hardcodat.

### YAML Pipeline (52 linii, zero Java)

```yaml
source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${MYSQL_USER}
  password: ${MYSQL_PASSWORD}
  tables: ${MYSQL_DATABASE}.orders
  server-id: 5700-5709
sink:
  type: kafka
  properties.bootstrap.servers: ${KAFKA_BOOTSTRAP}
  topic: ${KAFKA_TOPIC_PREFIX}.yaml-pipeline.orders
pipeline:
  name: Flink CDC YAML Pipeline CDC Job
```

---

## Slide 8 — Dovezi POC
| Verificare | Rezultat |
|-------------|--------|
| Teste unitare | 58/58 trecute |
| Toate cele 8 module compilează | Curat |
| Formatare (Spotless — Google Java Format) | Conformă |
| Flink CDC 3.6.0 pe Flink 2.2 | Verificat |
| Unit Tests pe fiecare variantă | Passed 100% coverage JaCoCo 100% mutations |
| Local integration test | Trecute (5 variante Flink + 5 KC) |
| Component Tests pe fiecare variantă | Passed (5 variante Flink + 5 KC) |
| StatementSet → 1 JobGraph | Verificat (doar SQL API; Table API folosește un singur INSERT, nu StatementSet) |
| Toate cele 5 variante rulează în paralel | Rulează la scară POC (localhost:8081; stare RocksDB incremental) |
| Teste de integrare locale (Flink MiniCluster) | Trecute (DataStreamCdc, OutboxRouter) |
| Checkpoint-uri persistate în S3/MinIO (ca în producție) | Verificat — bucket `flink-checkpoints`, aceeași configurație cod (interval 30 s, EXACTLY_ONCE) |

---

## Slide 9 — Dovezi POC: Capturi de Ecran Live

**Toate cele 5 variante Flink rulând simultan pe localhost — capturate în timpul POC-ului live.**
<!-- notes: URL-urile de acces pentru fiecare captură sunt în kafka-connect-at-scale-appendix.ro.md → secțiunea „Endpoint-uri de Monitorizare Locale”. -->

### Flink Dashboard — 5/5 Joburi RUNNING

![Flink Dashboard — 5 variante rulând simultan](images/slides/flink-dashboard.png)

> Toate cele cinci variante CDC (DataStream, Table API, SQL API, Outbox, YAML Pipeline) active într-un singur
> cluster Flink. Fiecare are propriul interval server-ID MySQL; zero coliziuni.

### Kafka UI — Cluster poc (32 topicuri, 109 partiții)

![Kafka UI — prezentare generală cluster poc](images/slides/kafka-ui.png)

> Topicurile sunt create automat de conectorii CDC. 32 topicuri = câte un topic per tabel pentru toate cele 5 variante
> plus topicuri de schema-history. Topicurile de semnal (`private.debezium.signal.*`) sunt specifice KC;
> Flink CDC nu le folosește.

### Kafka Connect REST API — 5 Conectori KC (comparație alăturată)

![Kafka Connect — lista a 5 conectori](images/slides/kafka-connect.png)

> Conectorii KC rulează în paralel doar pentru compararea output-ului. Server-ID-uri în intervalul rezervat
> `5500–5599` pentru a evita coliziunea cu variantele Flink.

### Grafana Dashboard — Monitorizare Flink CDC POC

![Grafana — dashboard Monitorizare Flink CDC POC](images/slides/grafana-dashboard.png)

> 3 monitoare livrate (mirroring Datadog): Restart Loop, Durata Checkpoint, Eșecuri Checkpoint — toate cele 5 variante verzi (green = OK). Monitoarele #4–#7 (lag conector, stare snapshot, poziție binlog) în așteptarea Spike S1.

---

## Slide 10 — Arhitectura Recomandată (Modelul de Deployment K8s)

**O imagine de bază per variantă. 74 de conectori MySQL. Fără cod Java folosit de echipe.**

Flink Platform Team 'owns' and 'publishes' Docker images parametrizabile pentru cele 5 variante, actualizate la zi pentru vulnerabilități.
Fiecare echipă 'owns' și configurează conectoarele ei, în repo-ul ei, doar prin suprascrierea valorilor Helm.

![Topologie Deployment K8s: Modelul Shared-Job](images/k8s-deployment-topology.svg)

```yaml
# Tot ce are nevoie o echipă
applicationJobs:
  my-tribe-cdc1:
    image: flink-stream-api-base-image:1.0.0
    extraEnvs:
      MYSQL_HOST: my-db.internal
      MYSQL_DATABASE: my_schema
      KAFKA_TOPIC_PREFIX: my-tribe.cdc
```

---
## Slide 11 — Îmbunătățiri

| Provocare (Slide 4) | Îmbunătățire |
|---------------------|--------------|
| Rebalancing storms — un conector defect destabilizează totul | **blast radius izolată** — jobul Flink al fiecărei echipe este izolat; eșecul rămâne per-echipă |
| blast radius partajată — 95 conectori, un singur cluster | **Proprietate clară** — echipa deține repo-ul și cadența de deploy a conectorului lor |
| Lag recurent — fără pârghie per echipă | **Lag-ul este gestionat pe echipă și per-job** |
| Eșecuri doar în producție | **Ciclu de viață Kubernetes nativ** — Flink Operator; testele component locale prind problemele înainte de deploy |
| Licențiere Confluent | **Economii parțiale de licențiere** — 74 conectori mutați de pe clusterul KC, deci poate fi redus la mai puține noduri de cluster facturabile; 21 conectori SFTP/SingleStore rămân pe KC |
| Upgrade-uri coordonate la nivel de flotă | **Upgrade-uri independente** — versionare per job; fără coordonare la nivel de flotă |

> **Notă:** Sink-ul exactly-once necesită tranzacții Kafka (`DeliveryGuarantee.EXACTLY_ONCE` + prefix ID tranzacțional în `KafkaSinkFactory`); broker-ul Kafka trebuie să aibă tranzacțiile activate.

---

## Slide 12 — Evitarea coliziunilor

Fiecare variantă POC primește alocări dedicate, fără suprapunere, astfel încât toate cele 5 job-uri Flink **și** 5 conectori Kafka Connect pot rula simultan pe același MySQL + Kafka fără coliziuni:

- **Interval MySQL server-ID** — fără suprapunere per variantă (plus un interval separat pentru conectorii KC), pentru ca cititorii paraleli Flink CDC să nu își fure reciproc lease-ul de binlog
- **Schema MySQL** — partajată `poc_db`; toate variantele citesc `poc_db.orders`, `poc_db.customers`, `poc_db.outbox_events`. Izolarea vine de la server-ID, **nu** de la scheme per variantă.
- **Prefix topic Kafka** — `poc.flink.<variant>.<table>` pentru job-urile Flink, `poc.kc.<variant>.<table>` pentru Kafka Connect, astfel încât ieșirile celor două motoare nu se ciocnesc
- **Căi checkpoint S3** — auto-namespace după `jobId`, bucket MinIO partajat, sigur

| Axă | Alocare |
|------|------------|
| MySQL server-ID | outbox=5600–5699, pipeline=5700–5709, sql-api=5800–5899, datastream=5900–5999, table-api=6000–6099; conectorii Kafka Connect folosesc 5500–5599 |
| Schema MySQL | partajată `poc_db` (`orders`, `customers`, `outbox_events`) — izolare prin server-ID, nu prin schemă |
| Prefix topic Kafka | Flink `poc.flink.<variant>.<table>` (ex. `poc.flink.datastream.orders`); Kafka Connect `poc.kc.<variant>.<table>` (ex. `poc.kc.datastream.orders`) |
| Căi checkpoint S3 | Auto-namespace după `jobId` — bucket MinIO partajat, sigur |

> **De ce intervale, nu ID-uri unice?** Flink alocă ID-uri pentru 'parallel readers'. Un singur identificator intră în coliziune la restart pentru că lease-ul anterior de binlog MySQL nu a expirat.

---

## Slide 13 — Monitorizare Centralizată: KC și Flink

| | Acum (KC / Debezium JMX) | Gap | Țintă (Flink, post-S1) |
|--|--------------------------|-----|------------------------|
| **Lag conector** | Debezium JMX `debezium.mysql:type=connector-metrics` → `MillisSinceLastEvent` | Fără echivalent direct Flink | Metrică backlog sursă Flink (investigație S1) |
| **Snapshot State** | Debezium JMX `snapshot.running` / `snapshot.aborted` | Fără echivalent încă (Spike S4) | Status job Flink + metrică personalizată via S1/S4 |
| **Poziție binlog** | Debezium JMX `source.pos` | Fără echivalent direct | Verificare poziție binlog din MySQL sau metrică offset Flink (S1) |
| **Restarts** | Restart-uri worker Kafka Connect | ✅ Disponibil — Flink `numRestarts` (Prometheus + Datadog) | Același |
| **Checkpoint Health** | N/A (KC stateless) | ✅ Îmbunătățire — Flink `lastCheckpointDuration`, `numberOfFailedCheckpoints` | Același |

**Monitoarele Datadog #4–#7** (lag conector, stare snapshot, poziție binlog, abort snapshot) nu pot fi mapate direct până când Spike S1 rezolvă echivalentele de metrici Flink.

**Atenuare interimară (pre-S1):**
- Monitorizare `numRestarts` ca proxy pentru lag (restart-uri repetate → poziție binlog stale)
- Din MySQL: interogare `SHOW MASTER STATUS` + comparare cu ultimul offset binlog Flink din metadatele checkpoint
- Alertă pe `records.consumed.rate` Flink sursă care scade la 0

**Starea țintă:** Un repo Terraform cu :
- un modul partajat pentru monitoarele Flink ('owned by Flink Platform Team') 
- câte un modul pentru fiecare echipă ('owned by' echipa) care instanțiază aceste monitoare și le configurează prin `config.tf` al fiecărei echipe.

---

## Slide 14 — Compromisuri si Riscuri
| Risc | Status / Atenuare | Unde este abordat |
|------|-------------------|-------------------|
| KC rămâne pentru 21 conectori SFTP/SingleStore — două sisteme de operat | De Acceptat; SFTP/SingleStore nu au echivalent Flink | Slide 3 (scop), Slide 15 (TCO) |
| Curbă de învățare — Flink Operator, checkpoint-uri, savepoint-uri | Atenuat pentru majoritatea echipelor prin modelul shared-job | Slide 10 (shared-job) |
| Secvențierea cutover — niciun plan de val, dual-run, gate paritate sau runbook de rollback încă | **Neatenuată** — S6 trebuie să livreze: plan de val, perioadă dual-run, gate paritate byte-for-byte, coordonare overlap server-ID binlog, runbook rollback | Slide 16, Spike S6 |
| Suprafață operațională nouă — Flink Operator, checkpoint-uri, savepoint-uri | Atenuat prin proprietatea Flink Platform Team asupra imaginii de bază și modulului de monitorizare | Slide 13, Spike S1 |
| Regresie observabilitate — metrici Debezium JMX (lag, stare snapshot, poziție binlog) nu au echivalent direct Flink | **Neatenuată** — interimar: metrici Flink restart/backlog + verificări binlog-position din MySQL ca proxy lag; rezoluție completă în așteptarea Spike S1 | Slide 13, Spike S1 |
| Evoluție schemă (ALTER TABLE) — comportamentul diferă per variantă; compatibilitatea schemei Kafka downstream nevalidată | **Neatenuată** — fără echivalent dbhistory.*; validare per echipă + politică compat schema-registry | Slide 16, Spike S8 (nou) |

---

## Slide 15 — Costul Total de Proprietate (Nivel Înalt)

**Starea actuală (Confluent KC):** o singură factură de cluster partajat acoperă toți cei 95 de conectori.

**Starea propusă (Flink):** factura Confluent redusă — doar 21 conectori rămân pe KC, deci clusterul poate rula pe mai puține noduri; Flink rulează pe K8s existent fără licențe suplimentare.

| Axă de cost | KC azi (95 conectori) | Propunere Flink (74 CDC → Flink; 21 KC rămân) |
|-------------|----------------------|------------------------------------------------|
| Licențiere Confluent | Factură completă pentru 95 conectori | ~22% conectori reținuți (21/95); mai puțini conectori → cluster mai mic → mai puține noduri de cluster facturabile — vezi mențiunea |
| Compute (CPU/RAM K8s) | KC gestionat de Confluent (inclus în licență) | O pereche JM + TM per conector; dimensionează per echipă față de rata de schimbare la vârf de sarcina (estimare POC: ~0,5 vCPU + 1 GB RAM la throughput binlog scăzut; dimensionarea pentru producție în așteptarea Spike S2) |
| Overhead operațional | Ops cluster partajat centralizat | Izolare per echipă; Flink Platform Team deține imaginea de bază |
| Cost migrare per echipă | Zero (status quo) | Livrabilele spike-urilor S5/S6 (automatizare cutover) |

> **Mențiune:** prețul Confluent per nod de cluster depinde de nivelul contractului.
> Economia direcțională (74 conectori mutați de pe clusterul KC, permițând rularea pe mai puține noduri) este certă;
> creșterea compute K8s trebuie dimensionată față de flota de workere KC existentă.

---

## Slide 16 — Spike-uri Deschise

| ID | Subiect | De Ce Contează | Faza | Timebox |
|----|-------|---------------|------|---------|
| S1 | Paritate metrici Flink — metrici Debezium JMX via Flink? | Determină designul modulului de monitorizare; blochează maparea monitoarelor KC #4–#7 | Faza 0 | 3 zile |
| S2 | Presiunea memoriei la snapshot inițial pe cea mai mare tabela | Previne surprizele în Faza 1/2 | Faza 0 | 2 zile |
| S3 | Rutare outbox multi-topic la scară mare (POC testează la 2) | Blocker go-live Faza 1 | Faza 0 | 2 zile |
| S4 | Echivalentul Flink pentru `snapshot.aborted`/`snapshot.running` | Migrarea outbox-connector (Faza 3) | Faza 0 | 2 zile |
| S5 | Testare în staging (RDS IAM, lease-uri binlog, rotație IRSA) | POC-ul nu le poate expune | Faza 1 | ≥7 zile testul de anduranță în staging |
| S6 | Automatizare cutover (KC → Flink): plan de val, perioadă dual-run, gate paritate byte-for-byte, coordonare overlap server-ID binlog, runbook rollback | Niciun plan de cutover nu există încă; switch-urile manuale nu vor scala la 26 de echipe | Faza 2 | ~5 zile ? |
| S7 | Instrumente Claude de migrare self-service pentru echipe | Echipele nu pot aștepta asistență de la Flink Platform Team | Faza 1 | 3 zile ? |
| S8 | Evoluție schemă — comportamentul ALTER TABLE per variantă Flink; fără echivalent dbhistory.*; politică compat schema-registry | 'blast radius' per echipă pentru schimbări de schemă; care e realitatea zilnică în producție ? | Faza 0 | 2 zile ? |
| S9 | Re-snapshot Client — savepoint + ștergere checkpoint S3 + re-rulare cu `--fromSavepoint` via Flink Operator; risc lease binlog | Calea oficială de upgrade cu stare; fără ea, orice restart re-snapshotează tot tabelul (FLINK_SAVEPOINT_RUNBOOK.md) | Faza 0 | 2 zile |

**Total Faza 0 (S1–S4, S8, S9): ~13 zile de inginerie — paralelizabil într-un singur sprint ?**

**Legenda fazelor:** 0 = spike-uri (pre-pilot) · 1 = go-live pilot prima echipă · 2 = extindere · 3 = cutover

---

## Referințe

- [Documentația Apache Flink 2.2.0](https://nightlies.apache.org/flink/flink-docs-release-2.2/)
- [Documentația Apache Flink CDC 3.6](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/)
- [Pagina principală a proiectului Apache Flink CDC (GitHub)](https://github.com/apache/flink-cdc)
- [Documentația Conector Debezium MySQL](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Prezentare generală Kafka Connect (documentație Apache Kafka)](https://kafka.apache.org/documentation/#connect)
- [Documentația Confluent Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
- [Confluent Debezium MySQL CDC Source Connector](https://docs.confluent.io/cloud/current/connectors/cc-mysql-cdc-source-debezium.html)
- [Documentația Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/overview/)
- [Documentația Flink Table API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/overview/)
- [Documentația Flink SQL API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/overview/)
- [Documentația Confluent Cloud](https://docs.confluent.io/cloud/current/overview.html)
- [Documentația Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
- [Documentația Apache Flink (stable)](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Documentația Flink Kubernetes Operator (stable)](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/)
- [Documentația Apache Flink CDC (stable)](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) ![Apache Flink CDC](images/flink-cdc-logo.png)
- [Documentația Conectorului Flink CDC MySQL](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/mysql-cdc/)
- `FLINK_CHECKPOINT_CONFIG.md` — semantici checkpoint, monitorizare, depanare
- `FLINK_SAVEPOINT_RUNBOOK.md` — fluxuri de upgrade sigure, recuperare stare
- `KAFKA_CONNECT.md` — variante KC CDC, SMT-uri, comparație Flink vs KC
- `HOW-TO-RUN-THIS-POC.md` - cum facem să rulăm acest POC
