
# Kafka Connect la Scară Largă: Cazul Migrării a 74 de Conectori

**Autor:** Adrian Balaban  
**Data:** 2026-06-26

În această sesiune vom explora Kafka Connect prin experiența reală a unui client, concentrându-ne pe un proof of concept și o migrare propusă a 74 de conectori din Confluent Kafka Cloud către Flink. Vom parcurge provocările actuale, îmbunătățirile pe care le urmărește noua abordare și compromisurile implicate. Prezentarea evidențiază, de asemenea, alternativele luate în considerare și raționamentul din spatele soluției propuse.

---

## Slide 0 — De ce este util acest talk (ce vei putea face după el)

> Nu doar o relatare a migrării unui client — un **playbook reutilizabil**. După
> acest talk poți reproduce CDC-ul cu Kafka Connect sau cu Flink la alt client.

Cinci lucruri pe care le iei de aici:

1. **Clientul** — cum s-a plecat acum câțiva ani de la o arhitectură **centrată pe DB** și s-a ajuns la una **event-driven** adăugând doar Kafka și o un număr de conectori Kafka Connect. Contextul care face rezultatul relevant.
2. **Problemele reale de producție cu KC** — rebalansare în cascadă între echipe fără legătură, lag fără reglaj per chipă, raza de impact partajată pe un   singur cluster, licențiere Confluent Cloud.
3. **Flink, conectorii Flink și Debezium pe scurt** — ce sunt, unde se  suprapun, unde diferă; Debezium ca parser de binlog reutilizat intern de Flink CDC (nu același conector KC).
4. **Flink este complet event-driven** — nu doar un conector CDC, ci un motor de stream processing cu stare, event-time și checkpoint-uri exactly-once, fiecare  job ca deployment K8s izolat.
5. **Informațiile + codul POC pentru a face CDC la un alt client** — 5 variante   rulând simultan, cod aproape de versiune de producție, infrastructură Podman Compose si K8s reproductibilă, component-tests care validează output-ul Kafka.

> Ținta: la final poți alege între KC și Flink cu argumente — și ai codul de la care pornești, nu de la zero.

---

## Slide 1 — Problema Într-o Propoziție

> Client real. Scară reală. 95 de conectori, 26 de echipe, un cluster partajat — și întrebarea dacă Flink este calea de ieșire corectă.

Migrarea propusă a **74 de conectori MySQL** de la Confluent Kafka Cloud la Flink, cu un proof of concept acoperind toate cele 5 variante.

Prezentarea este pregătită pentru **Comunitatea Java Cognizant România**.

---

## Slide 1b — Agendă (45 de minute)

*(Slide-urile 0–1d constituie cadrul de deschidere (~6 min); blocul de 45 de minute începe aici.)*

1. **Unde suntem** (2 min) — Contextul clientului + sfera migrării: 95 de conectori pe un singur cluster din care 74 conectori MySQL, 21 rămân pe KC → *Slide-urile 2, 5*
2. **De ce este 'painful' și ce cerem** (5 min) — Provocări + cele 3 cerințe pe care orice soluție trebuie să le indeplinească → *Slide 3*
3. **Ce este Flink și de ce este remedierea structurală** (4 min) — Flink într-un cadru; pool partajat vs. izolare per-job → *Slide 4*
4. **POC-ul + dovezi** (10 min) — 5 variante Flink rulând simultan; un snippet de cod; tabelul cu dovezi POC → *Slide-urile 6–8, 11*
5. **Soluția + îmbunătățiri** (5 min) — Modelul shared-job;îmbunătățiri concrete față de provocările de azi →*Slide-urile 9, 12*
6. **Arhitectură și evitarea coliziunilor** (8 min) — eployment K8s, intervale server-ID, monitorizare → *Slide-urile 10, 16*
7. **Compromisurile** (5 min) — Ce se schimbă, ce rămâne → *Slide 13*
8. **Costul schimbării** (2 min) — TCO: ce se reduce, ce  se adauga → *Slide 14*
9. **Întrebări deschise** (4 min) — 8 spike-uri → *Slide 15*

**Q&A: 15 minute**

*(total agendă: 45 min + 15 min Q&A; cadrul de deschidere Slide-urile 0–1d (~6 min: Slide 1c primer Kafka ~75 s, Slide 1d tiparele CDC-vs-Outbox ~3 min); capturile de ecran live Slide 11b se arată doar dacă timpul permite — niciuna nu e inclusă în cele 45 min.)*

---

## Slide 1c — Context în ~75 de secunde (pentru cei care nu au lucrat cu Kafka Connect & CDC)

```
MySQL binlog  →  Debezium  →  Kafka  →  consumatori
               (capturează     topicuri        (alte sisteme,
                schimbări)                       DB-uri)
```

| Termen | Ce este (o propoziție) |
|--------|------------------------|
| **MySQL binlog** | Jurnalul intern MySQL cu toate INSERT/UPDATE/DELETE — Debezium îl citește si il replică pe un Kafka topic |
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
## Tabela Outbox — Ce Este?

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

## Slide 1e — Ce Conține POC-ul (Privere de Ansamblu)

**POC-ul demonstrează 5 variante Flink CDC + 5 conectori Kafka Connect echivalenți, rulând simultan pe același stack local.**

| Flink Dashboard — 5/5 joburi RUNNING | Kafka Connect REST — 5 conectori |
|---|---|
| ![Flink Dashboard](images/slides/flink-dashboard.png) | ![Kafka Connect](images/slides/kafka-connect.png) |

> Ambele motoare rulează în paralel pentru comparație output-ului. Capturile complete și detaliile pe Slide 11b.

---

## Slide 2 — Contextul Clientului (Unde Suntem Azi)

```
26 de echipe  ─►  ┌───────────────────────────────┐
                  │  1 cluster KC partajat        │
                  │  95 conectori (74 MySQL + 21) │  blast radius = 1
                  │  1 grup de rebalansare        │
                  └───────────────────────────────┘
```

**Experiență reală cu un client: Confluent Kafka Cloud la scară**

- **95 de conectori** pe **un singur cluster Kafka Connect partajat** pentru **26 de echipe**
- Două familii de conectori astăzi:
  - **Debezium (Kafka Connect)** — citește binlog-ul MySQL via KC gestionat de Confluent, un eveniment per modificare per topic
  - Conectori sink/source SFTP + SingleStore
- Totul partajează un singur cluster: o configurație, un singur grup de rebalansare, o singură rază de impact

> Clusterul partajat era convenabil la 5 conectori. La 95 pe 26 de echipe — și
> în creștere — este cea mai mare sursă de incidente inter-echipe. Această
> presiune de scalare este motivul pentru care investigăm acum.

---

## Slide 3 — Ce este 'painful' astazi si cerem de la orice solutie


**Ce este 'painful' azi — și cât costă:**

| Problemă | Cine | Cât de des | Impact business |
|----------|------|-----------|-----------------|
| 'Rebalancing storms' — un conector defect destabilizează tot | Toate cele 26 de echipe | De mai multe ori/trimestru | Incidente inter-echipe; downtime consumatori |
| 'Blast radius' partajată — 95 de conectori, un cluster | Toate cele 26 de echipe | La fiecare incident | Fără izolare între echipe |
| 'Lag' recurent — fără instrumente 'per team' | Team + consumatori | Continuu | Risc SLA pe consumatorii downstream |
| Erori care se manifestă doar după deploy si doar în producție | Echipele cu conectori noi | După deploy conector nou cu erori | Erori ce ajung în prod. nedetectate |
| Licențiere Confluent Kafka Cloud | Organizația | Lunar | **Cost lunar de licențiere semnificativ** (este pe numar de noduri, vezi Slide 14 TCO) |
| Patch-uri de securitate Confluent doar la 3 luni | Echipa de mentenanță | La fiecare ciclu de release și la fiecare vulnerabilitate fixată | Risc de erori la patch-urile facute de Echipa de mentenanță |


**Ce cerem de la orice soluție** *(agnostic față de soluție — etalonul pentru opțiunile de pe matricea de decizie, Slide 7):* TODO verificat toate trimiterile la alte slide-uri

1. Imaginea de bază + patch-urile de securitate rămân **centralizate** — echipele nu dețin runtime-ul.
2. **Să ne îndepărtăm de Confluent Platform** — licențiere și lock-in.
3. **Impartirea in clustere per echipă, cu aceeasi solutie, nu rezolvă problemele**.


---

## Slide 4 — Ce Este Flink și De Ce Este Remedierea Propusa Structurală

**Apache Flink** este un motor de procesare a stream-urilor cu stare (stateful): un job continuu care citește evenimente, menține stare și scrie rezultate — cu  *checkpoint-uri exactly-once** (durabile, recuperabile). Fiecare job rulează ca **propriul deployment K8s izolat** (JobManager + TaskManager proprii) sub Flink Operator.

**Flink + MySQL Connector** cat si **Flink CDC** fac același lucru ca Debezium-on-Kafka-Connect — citește binlog-ul MySQL și emite evenimente CDC către Kafka.

Argumentul structural într-un singur cadru — aceasta este puntea de la "ce e 'painful'" la "de ce Flink remediază":

| | Kafka Connect azi | Flink (propus) |
|--|--|--|
| Deployment | 1 cluster partajat de workere | N joburi K8s izolate (prin Flink Operator) grupate pe team-uri |
| Blast radius | 1 — toate cele 95 de conectori | 1 per team — limitată |
| Rebalancing storms | un grup → cascadă pe 26 de echipe | niciuna |
| Licențiere | Confluent Cloud (cu plată) | Apache 2.0 (gratuit) |

TODO: e locul lui aici ? Se vede bine sau e de facut un slide separat ?
> **"CDC" înseamnă două lucruri — nu le confunda:** (1) **Flink CDC `MySqlSource`** — conectorul care citește binlog-ul MySQL prin algoritmul propriu de snapshot incremental Flink CDC (variantele 1–4: DataStream / Table API / SQL API / Outbox; reutilizează intern parserul de binlog Debezium, dar **nu** rulează pe conectorul Debezium Kafka Connect). (2) **Flink CDC YAML pipeline** — *framework-ul declarativ de pipeline YAML* deasupra aceleiași surse, fără Java (Varianta 5). Această prezentare acoperă ambele sensuri.

---

## Slide 5 — Scopul Migrării

**Ce migrăm:** 74 de conectori MySQL → Apache Flink MySQL CDC Connector

**Ce rămâne pe Kafka Connect:** 21 de conectori SFTP + SingleStore (Flink nu are echivalent)

![Pattern Migrare: Înainte și După](images/migration-before-after.svg)
TODO verificat cum se vad toate imaginile

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

## Slide 7 — Matricea de Decizie: Ce Variantă folosim pentru Ce Conector?

![Arbore de Decizie Conector: Ce Variantă pentru Ce Conector?](images/connector-decision-tree.svg)

| Tip Conector | Varianta Recomandată | De Ce |
|-------------------|--------------------|----|
| Outbox (tranzacțional, rutare per-rând) | DataStream | Table/SQL API nu pot face rutare per-rând |
| CDC cu îmbogățire/transformare personalizată | DataStream CDC | Acces Java la `CdcEventRouter` + `MapFunction` personalizat |
| CDC simplu (tabel → topic, fără transformare) | YAML Pipeline/SQL API | Zero Java |
| CDC cu join-uri/agregări SQL viitoare | Table API | Simplifica sistemul |

---

## Slide 8 — Perspectiva Programatorului Java: Comparație de Cod

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

## Slide 9 — Arhitectura Recomandată

**O imagine de bază per variantă. 74 de conectori MySQL. Fără fork Java per echipă.**

Flink Platform Team 'owns' imagini parametrizabile pentru cele 5 variante, actualizate pentru ultimele vulnerabilitati.
Fiecare echipă 'owns' si configureaza conectoarele ei, in repo-ul ei, doar prin suprascrierea valorilor Helm.

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
TODO de mutat in alt fisier, apoi renumerotat slide-urile
## Slide 10 — Modelul de Deployment K8s


### Evitarea Coliziunilor

Fiecare variantă POC primește propriul interval dedicat, fără suprapunere, pe 4 axe, astfel încât toate pot rula simultan fără coliziuni:

- **Interval MySQL server-ID** — pentru ca cititorii paraleli Flink CDC să nu își fure reciproc lease-ul de binlog
- **Schema MySQL** — pentru ca fiecare variantă să aibă propria bază de date sursă
- **Prefix topic Kafka** — pentru ca topicurile să nu se suprapună
- **Căi checkpoint S3** — pentru ca checkpoint-urile să nu se suprapună

| Axă | Alocare |
|------|------------|
| MySQL server-ID | outbox=5600–5699, pipeline=5700–5709, sql-api=5800–5899, cdc=5900–5999, table-api=6000–6099 |
| Schema MySQL | `cdc_db`, `sql_api_db`, `table_api_db`, `pipeline_db`, `outbox_db` |
| Prefix de topic Kafka | `shared-cdc.cdc-db.*`, `sql-api.sql-api-db.*`, `table-api.table-api-db.*`, `pipeline.pipeline-db.*`, `outbox.destination.*` |
| Căi checkpoint S3 | Auto-namespace după `jobId` — bucket partajat, sigur |

> **De ce intervale, nu ID-uri unice?** Flink alocă ID-uri pentru 'parallel readers'. Un singur identificator int intră în coliziune la restart pentru că
> lease-ul anterior de binlog MySQL nu a expirat.

---

## Slide 11 — Dovezi POC
TODO verde/verzi=>green/greens
| Verificare | Rezultat |
|-------------|--------|
| Teste unitare | 57/57 trecute |
| Toate cele 8 module compilează | Curat |
| Formatare (Spotless — Google Java Format) | Conformă |
| Flink CDC 3.6.0 pe Flink 2.2 | Verificat |
| Unit Tests pe fiecare variantă | Passed 100% coverage JaCoCo 100% mutations |
| Local integration test | Trecute (5 variante Flink + 5 KC) |
| Component Tests pe fiecare variantă | Passed (5 variante Flink + 5 KC) |
| StatementSet → 1 JobGraph | Verificat (doar SQL API; Table API folosește un singur INSERT, nu StatementSet) |
| Toate cele 5 variante ruleaza in paralel | Rulează la scară POC (localhost:8081; stare RocksDB incremental) |
| Teste de integrare locale (Flink MiniCluster) | Trecute (DataStreamCdc, OutboxRouter) |
| Checkpoint-uri persistate în S3/MinIO (ca în producție) | Verificat — bucket `flink-checkpoints`, aceeași configurație cod (interval 30 s, EXACTLY_ONCE) |

---

## Slide 11b — Dovezi POC: Capturi de Ecran Live

**Toate cele 5 variante Flink rulând simultan pe localhost — capturate în timpul POC-ului live.**
TODO mentionat ca url-urile sunt in ...

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

> 3 monitoare livrate (mirroring Datadog): Restart Loop, Durata Checkpoint, Eșecuri Checkpoint — toate cele 5 variante verzi. Monitoarele #4–#7 (lag conector, stare snapshot, poziție binlog) în așteptarea Spike S1.

---

## Slide 12 — Îmbunătățiri Adresate TODO de corectat, apoi mutat mai sus, apoi renumerotat slide-urile

| Provocare (Slide 3) | Îmbunătățire |
|---------------------|--------------|
| Furtuni de rebalansare — un conector defect destabilizează totul | **Raza de impact izolată** — jobul Flink al fiecărei echipe este izolat; eșecul rămâne per-echipă |
| Raza de impact partajată — 95 conectori, un singur cluster | **Proprietate clară** — echipa deține repo-ul și cadența de deploy a conectorului lor |
| Lag recurent — fără pârghie per echipă | **Lag-ul este gestionat pe echipă și per-job** |
| Eșecuri doar în producție | **Ciclu de viață Kubernetes nativ** — Flink Operator; testele component locale prind problemele înainte de deploy |
| Licențiere Confluent | **Economii parțiale de licențiere** — 74 conectori mutați de pe clusterul KC, deci poate fi redus la mai puține noduri de cluster facturabile; 21 conectori SFTP/SingleStore rămân pe KC |
| Upgrade-uri coordonate la nivel de flotă | **Upgrade-uri independente** — versionare per job; fără coordonare la nivel de flotă |

> **Notă:** Sink-ul exactly-once necesită tranzacții Kafka (`DeliveryGuarantee.EXACTLY_ONCE` + prefix ID tranzacțional în `KafkaSinkFactory`); broker-ul Kafka trebuie să aibă tranzacțiile activate.

---

## Slide 13 — Compromisuri si Riscuri
 TODO redenumire slide-uri referentiate
| Risc | Status / Atenuare | Unde este abordat |
|------|-------------------|-------------------|
| KC rămâne pentru 21 conectori SFTP/SingleStore — două sisteme de operat | De Acceptat; SFTP/SingleStore nu au echivalent Flink | Slide 5 (scop), Slide 14 (TCO) |
| Curbă de învățare — Flink Operator, checkpoint-uri, savepoint-uri | Atenuat pentru majoritatea echipelor prin modelul shared-job | Slide 7 (arbore de decizie), Slide 9 (shared-job) |
| Secvențierea cutover — niciun plan de val, dual-run, gate paritate sau runbook de rollback încă | **Neatenuată** — S6 trebuie să livreze: plan de val, perioadă dual-run, gate paritate byte-for-byte, coordonare overlap server-ID binlog, runbook rollback | Slide 15, Spike S6 |
| Suprafață operațională nouă — Flink Operator, checkpoint-uri, savepoint-uri | Atenuat prin proprietatea Flink Platform Team asupra imaginii de bază și modulului de monitorizare | Slide 16, Spike S1 |
| Regresie observabilitate — metrici Debezium JMX (lag, stare snapshot, poziție binlog) nu au echivalent direct Flink | **Neatenuată** — interimar: metrici Flink restart/backlog + verificări binlog-position din MySQL ca proxy lag; rezoluție completă în așteptarea Spike S1 | Slide 16, Spike S1 |
| Evoluție schemă (ALTER TABLE) — comportamentul diferă per variantă; compatibilitatea schemei Kafka downstream nevalidată | **Neatenuată** — fără echivalent dbhistory.*; validare per echipă + politică compat schema-registry | Slide 15, Spike S8 (nou) |

---

## Slide 14 — Costul Total de Proprietate (Nivel Înalt)

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

## Slide 15 — Spike-uri Deschise

| ID | Subiect | De Ce Contează | Faza | Timebox |
|----|-------|---------------|------|---------|
| S1 | Paritate metrici Flink — metrici Debezium JMX via Flink? | Determină designul modulului de monitorizare; blochează maparea monitoarelor KC #4–#7 | Faza 0 | 3 zile |
| S2 | Presiunea memoriei la snapshot inițial pe cea mai mare tabela | Previne surprizele în Faza 1/2 | Faza 0 | 2 zile |
| S3 | Rutare outbox multi-topic la scară mare (POC testează la 2) | Blocker go-live Faza 1 | Faza 0 | 2 zile |
| S4 | Echivalentul Flink pentru `snapshot.aborted`/`snapshot.running` | Migrarea outbox-connector (Faza 3) | Faza 0 | 2 zile |
| S5 | Testare in staging (RDS IAM, lease-uri binlog, rotație IRSA) | POC-ul nu le poate expune | Faza 1 | ≥7 zile testul de anduranță în staging |
| S6 | Automatizare cutover (KC → Flink): plan de val, perioadă dual-run, gate paritate byte-for-byte, coordonare overlap server-ID binlog, runbook rollback | Niciun plan de cutover nu există încă; switch-urile manuale nu vor scala la 26 de echipe | Faza 2 | ~5 zile ? |
| S7 | Instrumente Claude de migrare self-service pentru echipe | Echipele nu pot aștepta asistență de la Flink Platform Team | Faza 1 | 3 zile ? |
| S8 | Evoluție schemă — comportamentul ALTER TABLE per variantă Flink; fără echivalent dbhistory.*; politică compat schema-registry | 'blast radius' per echipă pentru schimbări de schemă; care e realitatea zilnică în producție ? | Faza 0 | 2 zile ? |
| S9 | Re-snapshot Client — savepoint + ștergere checkpoint S3 + re-rulare cu `--fromSavepoint` via Flink Operator; risc lease binlog | Calea oficială de upgrade cu stare; fără ea, orice restart re-snapshotează tot tabelul (FLINK_SAVEPOINT_RUNBOOK.md) | Faza 0 | 2 zile |

**Total Faza 0 (S1–S4, S8, S9): ~13 zile de inginerie — paralelizabil într-un singur sprint ?**

**Legenda fazelor:** 0 = spike-uri (pre-pilot) · 1 = go-live pilot prima echipă · 2 = extindere · 3 = cutover

---

## Slide 16 — Monitorizare Centralizată: KC și Flink

| | Acum (KC / Debezium JMX) | Gap | Țintă (Flink, post-S1) |
|--|--------------------------|-----|------------------------|
| **Lag conector** | Debezium JMX `debezium.mysql:type=connector-metrics` → `MillisSinceLastEvent` | Fără echivalent direct Flink | Metrică backlog sursă Flink (investigație S1) |
| **Stare snapshot** | Debezium JMX `snapshot.running` / `snapshot.aborted` | Fără echivalent încă (Spike S4) | Status job Flink + metrică personalizată via S1/S4 |
| **Poziție binlog** | Debezium JMX `source.pos` | Fără echivalent direct | Verificare poziție binlog din MySQL sau metrică offset Flink (S1) |
| **Restart-uri** | Restart-uri worker Kafka Connect | ✅ Disponibil — Flink `numRestarts` (Prometheus + Datadog) | Același |
| **Sănătate checkpoint** | N/A (KC stateless) | ✅ Îmbunătățire — Flink `lastCheckpointDuration`, `numberOfFailedCheckpoints` | Același |

**Monitoarele Datadog #4–#7** (lag conector, stare snapshot, poziție binlog, abort snapshot) nu pot fi mapate direct până când Spike S1 rezolvă echivalentele de metrici Flink.

**Atenuare interimară (pre-S1):**
- Monitorizare `numRestarts` ca proxy pentru lag (restart-uri repetate → poziție binlog stale)
- Din MySQL: interogare `SHOW MASTER STATUS` + comparare cu ultimul offset binlog Flink din metadatele checkpoint
- Alertă pe `records.consumed.rate` Flink sursă care scade la 0

**Starea țintă:** Un repo Terraform cu :
- un modul partajat pentru monitoarele Flink ('owned by Flink Platform Team') 
- cate un un modul pentru fiecare echipa ('owned by' echipa) care instantiaza aceste monitoare si le configureaza prin `config.tf` al fiecărei echipe.

---

## APPENDIX — Slide-uri de Rezervă (Nu Fac Parte din Prezentarea de 45 Minute)

> Cele trei liste de mai jos sunt materiale de referință doar pentru Q&A. Nu le prezentați live —
> sunt aici pentru a putea sări la un tabel specific dacă sunteți întrebați o întrebare detaliată de infrastructură.

---

## Referință Detaliată — Structura Modulelor POC

### Structura Modulelor POC (`flink-cdc-poc`)

```
flink-cdc-poc/
├── common/                             # JobConfig, CheckpointConfigurer, PocJsonDeserializationSchema, CdcEventRouter, OutboxRouter, KafkaSinkFactory, DdlValidator (~412 linii)
├── variant-flink-datastream-api-v1-cdc-job/   # DataStreamCdcJob.java  (63 de linii, server-ID 5900–5999)
├── variant-flink-table-api-cdc-job/           # TableApiCdcJob.java    (99 linii, server-ID 6000–6099)
├── variant-flink-sql-api-cdc-job/             # SqlApiCdcJob.java      (156 linii, server-ID 5800–5899)
├── variant-flink-datastream-api-v1-outbox-job/ # OutboxJob.java        (56 linii, server-ID 5600–5699)
├── variant-flink-cdc-yaml-pipeline-cdc-job/   # pipeline.yaml         (52 linii, canonical: src/main/resources/pipeline.yaml, server-ID 5700–5709)
├── component-tests/                    # 16 clase de test + 5 clase helper de bază:
│                                       #   variante Flink: DataStreamCdcTest, TableApiCdcTest, SqlApiCdcTest,
│                                       #     DataStreamOutboxTest, YamlPipelineCdcTest
│                                       #   KC: KafkaConnectVariantTest, KafkaConnectOutboxTest
│                                       #   invarianți/calitate: CdcOperationsTest, CdcParityTest, DataQualityTest,
│                                       #     DataStreamCdcMiniClusterTest, OutboxRouterMiniClusterTest,
│                                       #     ErrorScenarioTest, ExactlyOnceInvariantTest, JobHealthTest,
│                                       #     SchemaEvolutionTest
├── local-development-podman/           # Stack Podman Compose
│   ├── podman-compose.yml              # MySQL + Kafka + Flink JM/TM + KC + kafka-ui + flink-cdc-submitter
│   ├── flink-with-mysql/Dockerfile     # Flink 2.2 + mysql-connector-j
│   ├── flink-cdc-submitter/            # rulează flink-cdc.sh pentru varianta YAML Pipeline
│   ├── kafka-connect/                  # Debezium + SMT-uri personalizate; 5 configurații JSON conector
│   └── kafka-connect-smts/             # EnrichmentTransform + OutboxRoutingTransform (Java 11)
└── local-development-k8s/              # Stack Kubernetes (kind + Flink Operator + Strimzi)
    ├── deploy.sh / teardown.sh
    └── flink/  kafka/  kafka-connect/  mysql/  minio/  monitoring/
```

## Backup — Configurarea Checkpoint-urilor (pregătit pentru producție)

Toate cele cinci variante partajează un singur punct de extracție — `CheckpointConfigurer.applyExactlyOnce(env)` —
în loc să repete cele cinci apeluri de mai jos în fiecare clasă de intrare:

```java
// common/src/main/java/poc/common/checkpoint/CheckpointConfigurer.java
public static void applyExactlyOnce(StreamExecutionEnvironment env) {
    env.enableCheckpointing(30_000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setCheckpointTimeout(60_000);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
    env.getCheckpointConfig()
        .setExternalizedCheckpointRetention(
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    // State backend (RocksDB + incremental) configurat la nivel de cluster prin
    // FLINK_PROPERTIES: state.backend=rocksdb, state.backend.incremental=true
    // Astfel codul job-ului rămâne independent de alegerea backend-ului (flexibilitate operațională).
}
```

Starea checkpoint-urilor este persistată în stocare compatibilă S3 (MinIO local, AWS S3 în producție),
configurată în `flink-conf.yaml`:

```yaml
# POC local: state.backend: rocksdb (+ incremental) setat via FLINK_PROPERTIES — la fel ca producția
# Producție: state.backend: rocksdb via cluster config / FLINK_PROPERTIES
state.checkpoints.dir: s3://flink-checkpoints/checkpoints
state.savepoints.dir:  s3://flink-checkpoints/savepoints
s3.endpoint: http://minio:9000
s3.path.style.access: "true"
s3.access-key: minioadmin
s3.secret-key: minioadmin
```

**Dovadă POC — bucket-ul MinIO `flink-checkpoints` după rularea tuturor celor 5 variante:**

![MinIO bucket flink-checkpoints — foldere checkpoints și yaml-pipeline-checkpoints](images/slides/minio-checkpoints.png)

| Setare | Valoare | Motiv |
|--------|-------|--------|
| `enableCheckpointing` | 30.000 ms | Echilibrează durabilitatea vs. performanța |
| `CheckpointingMode` | EXACTLY_ONCE | Previne mesajele Kafka duplicate la recuperare |
| `MaxConcurrentCheckpoints` | 1 | Joburile CDC fac snapshot în timpul checkpoint-ului; unul câte unul |
| `CheckpointTimeout` | 60.000 ms | 2× intervalul; oferă marjă pentru joburi cu stare mare sub sarcină |
| `MinPauseBetweenCheckpoints` | 5.000 ms | Previne furtunile de checkpoint după finalizarea unuia |

![Checkpoint-uri persistate în MinIO/S3 — bucket `flink-checkpoints`](images/slides/minio-checkpoints.png)

> Captură din console-ul MinIO: checkpoint-urile Flink (un director per job) sunt persistate în bucket-ul
> `flink-checkpoints`, ca în producție pe S3 (aceleași setări cod: interval 30 s, EXACTLY_ONCE).

---

## Lista de Infrastructură 1 — Infrastructura Clientului (Producție)

### Kubernetes

- **Flink Operator** — gestionează CR-urile `FlinkDeployment`; capacitatea sloturilor TaskManager trebuie monitorizată
- **CR-uri `FlinkDeployment`** — câte unul per job/variantă; fiecare cu propriul JobManager + perechea de pod-uri TaskManager (Application Mode)
- **CR-uri `FlinkStateSnapshot`** — câte unul per job, gestionat de chart
- **Servicii ClusterIP** — `<jobName>-rest` per job, portul 8081
- **Chart Helm: `flink-base-chart`** — mapa `applicationJobs`, livrare init-container, topology spread, probe-uri, graceful shutdown, strategie restart
- **Izolare namespace**

### Apache Flink

- **Runtime Flink 2.2** — imagine de bază `flink-base-image` (Flink Platform Team)
- **Flink CDC 3.6.0** (sufixul `3.6.0-2.2`) — inclus în imaginile de variantă; versiunea trebuie să corespundă runtime-ului
- **Plugin-uri built-in** — `flink-s3-fs-presto-2.2.1.jar` (versionat, trebuie să corespundă imaginii de bază)
- **`mysql-connector-j`** — montat în ambele JobManager și TaskManager; pattern classloader parent-first necesar (`com.mysql.`)
- **Checkpointing** — `checkpointing.dir` unic per job; exactly-once; backed de S3

### MySQL / Baze de Date

- **Acces binlog MySQL** — Flink CDC citește binlog-ul direct; necesită `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`
- **Intervale `server-id` fără suprapunere**
- **Rotație token RDS IAM**
- **IRSA** — pentru permisiunile S3 ale checkpoint-ului
- **Privilegii MySQL** — `RELOAD` + `LOCK TABLES` necesare pentru snapshot-ul inițial

### Kafka

- **Topicuri Kafka** (prefixe per variantă)
- **Topicuri schema history** (KC/Debezium)
- **Topic semnal** (doar pre-migrare, abandonat după): `private.debezium.signal.<conector>.v1`
- **Kafka Connect (Confluent)** — păstrat pentru SFTP (20) și SingleStore (1); 74 de conectori Debezium MySQL migrați la Flink
- **Topic heartbeat** — monitor KC #1; echivalent Flink: Restart Loop + heartbeat TM

### Container Registry / Imagini

- `flink-base-image` — runtime Flink (existent)
- Imagini de bază noi propuse (Shared Job / Platform Architect):
  - `flink-cdc-base-image`
  - `flink-stream-api-base-image`
  - `flink-table-api-base-image`
  - `flink-sql-api-base-image`
- Imagini fat-jar per variantă: 4 fat-jar-uri Flink + 1 shadow JAR KC SMT; plugin Shadow

### Stocare Obiecte (S3)

- Bucket S3 partajat pentru checkpoint-uri/savepoint-uri
- Căile `checkpointing.dir` per job nu trebuie să se suprapună

### IAM / Securitate

- **IRSA** — acces S3 checkpoint; rotația trebuie testată
- **Token-uri RDS IAM**
- **Lease-uri binlog** 

---

## Lista de Infrastructură 2 — Infrastructura POC Local

### Versiuni Software

| Componentă | Versiune |
|-----------|---------|
| Apache Flink | 2.2.0 |
| Flink CDC | 3.6.0-2.2 |
| flink-connector-kafka | 5.0.0-2.2 |
| Kafka (Confluent) | Modul KRaft, cp-kafka 7.8.0 (broker upgrade 7.6.1→7.8.0, CVE-2024-27309 / CVE-2024-31141) |
| MySQL | 8.0 |
| Java (joburi Flink) | 17 |
| Java (Kafka Connect SMTs) | 11 (cp-kafka-connect 7.6.1 JDK) |
| mysql-connector-j | 8.0.33 |
| Gradle | 8.7 |
| Plugin Shadow | 8.1.1 |

### Servicii Podman-Compose

| Serviciu | Imagine | Port(uri) | Rol |
|---------|-------|---------|------|
| `mysql` | `mysql:8.0` | 3306 | Sursă CDC; `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`, `server-id=1` |
| `kafka` | `cp-kafka:7.8.0` | 9092 (ext), 29092 (int), 9093 (controller) | Broker KRaft + controller; `auto.create.topics.enable=true` |
| `flink-jobmanager` | personalizat (Flink 2.2 + mysql-connector-j) | 8081 (REST), 6123 (RPC) | JobManager; 8 sloturi task; `taskmanager.slot.timeout=60000` |
| `flink-taskmanager` | personalizat (aceeași imagine) | 8082, 6124 | TaskManager; 8 sloturi task |
| `flink-cdc-submitter` | personalizat | — | Rulează `flink-cdc.sh` pentru varianta YAML Pipeline la JM gata; `restart: on-failure` |
| `kafka-connect` | personalizat (Debezium + SMT JAR-uri) | 8083 | REST API KC; comparație alăturată; `restart: on-failure` |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 | Browser topicuri Kafka |
| `minio` | `minio/minio:latest` | 9000 (API), 9001 (consolă) | Stocare checkpoint compatibilă S3; bucket `flink-checkpoints` |
| `minio-init` | `minio/mc` | — | One-shot: creează bucket-ul `flink-checkpoints` la pornire |
| `prometheus` | `prom/prometheus:v2.52.0` | 9090 | Scraping metrici Flink JM/TM la 15 s; doar local |
| `grafana` | `grafana/grafana:10.4.3` | 3001 | Dashboard + reguli alertă (gestionate de Terraform); admin/admin |

### Module Gradle

| Modul | Rol |
|--------|------|
| `common` | `JobConfig`, `CheckpointConfigurer`, `PocJsonDeserializationSchema`, `CdcEventRouter`, `OutboxRouter`, `KafkaSinkFactory`, `DdlValidator` |
| `variant-flink-datastream-api-v1-cdc-job` | DataStream CDC; server-ID 5900–5999 |
| `variant-flink-table-api-cdc-job` | Table API CDC; server-ID 6000–6099 |
| `variant-flink-sql-api-cdc-job` | SQL API CDC; server-ID 5800–5899 |
| `variant-flink-datastream-api-v1-outbox-job` | Outbox; server-ID 5600–5699 |
| `variant-flink-cdc-yaml-pipeline-cdc-job` | YAML Pipeline; server-ID 5700–5709 |
| `component-tests` | End-to-end: submitere fat-jar-uri la JM REST; polling Kafka; acoperă toate cele 5 variante Flink + 5 KC |
| `kafka-connect-smts` | `EnrichmentTransform` + `OutboxRoutingTransform` (Java 11, shadow JAR) |

### Comenzi Build & Test

| Comandă | Ce Face |
|---------|-------------|
| `./gradlew shadowJar` | Construiește cele 4 fat-jar-uri Flink + shadow JAR KC SMT |
| `./gradlew :component-tests:test` | Rulează toate testele de componente (Flink + KC) |
| `./gradlew all` | Ciclu complet: build → restart podman-compose → așteptare servicii (180 s) → deploy conectori KC → rulare CT-uri |
| `podman-compose -f podman-compose.yml up -d` | Pornește stiva completă de 11 servicii |
| `podman exec flink-jm flink run /tmp/<jar>` | Submitere variantă la JM-ul rulant |

### Kafka Connect Alăturat (doar POC)

Cinci conectori KC oglindesc variantele Flink, folosind server-ID-uri în intervalul rezervat `5500–5599`:

| Conector KC | Server-ID | SMT |
|-------------|-----------|-----|
| `kc-datastream-cdc` | 5510 | `EnrichmentTransform` |
| `kc-table-api-cdc` | 5520 | `EnrichmentTransform` |
| `kc-sql-api-cdc` | 5530 | `EnrichmentTransform` |
| `kc-yaml-pipeline-cdc` | 5540 | `EnrichmentTransform` |
| `kc-outbox-cdc` | 5550 | `OutboxRoutingTransform` |

### Endpoint-uri de Monitorizare Locale

Stack-ul Podman leagă direct porturile pe host; stack-ul k8s nu leagă niciun port pe host — accesul se face via `kubectl port-forward` către porturi mari non-conflictuale (vezi [`K8S.md`](./K8S.md)). Ambele stack-uri rulează aceleași 5 job-uri Flink și conectori Kafka Connect; diferența este unitatea de deployment (Podman: un JM partajat; k8s: câte un JM per variantă, Application Mode).

| Serviciu | URL Podman | URL k8s (port-forward) | Captură de Ecran |
|----------|------------|------------------------|------------------|
| Flink Dashboard | `http://localhost:8081` (JM partajat; toate cele 5 job-uri) | `http://localhost:18081`–`18085` (JM per variantă: DataStream / Table API / SQL API / Outbox / YAML) | ![](images/slides/flink-dashboard.png) |
| Kafka UI | `http://localhost:8080` | — (nu este deployed în slice-ul k8s) | ![](images/slides/kafka-ui.png) |
| Kafka Connect REST | `http://localhost:8083` | `http://localhost:18086` | ![](images/slides/kafka-connect.png) |
| MySQL | `localhost:3306` (user: `flink`, parolă: `flink`, db: `poc_db`) | `localhost:13306` | — |
| Kafka (extern) | `localhost:9092` (topicuri: `poc.flink.*` pentru Flink, `poc.kc.*` pentru Kafka Connect) | `localhost:19092` (listener Strimzi extern nodeport; advertisedHost=localhost) | — |
| Prometheus | `http://localhost:9090` | `http://localhost:19090` | — |
| Grafana | `http://localhost:3001` (dashboard + alerte; user: `admin`, parolă: `admin`) | `http://localhost:13001` (user: `admin`, parolă: `admin`) | — |
| MinIO | `http://localhost:9001` (user: `minioadmin`, parolă: `minioadmin`, bucket: `flink-checkpoints`) | `http://localhost:9001` (user: `minioadmin`, parolă: `minioadmin`, bucket: `flink-checkpoints`) | ![](images/slides/minio-checkpoints.png) |

---

## Lista de Infrastructură 3 — Comparație: Client vs. POC Local
TODO de modificat ca sa fie luate info din k8s POC
| Zonă | Client (Producție) | POC Local (k8s) |
|------|--------------------|-----------------------------|
| **Orchestrare** | Kubernetes + Flink Operator + Helm (`flink-base-chart`) | La fel |
| **Unitate deployment Flink** | CR `FlinkDeployment` per job (Application Mode; JM+TM proprii) | La fel |
| **Versiune Flink** | 2.2 (via `flink-base-image`) | La fel |
| **Versiune Flink CDC** | 3.6.0-2.2 (inclus în imaginile de variantă) | La fel |
| **MySQL** | RDS (AWS); autentificare IAM; IRSA pentru S3; date de producție | K8s Container `mysql:8.0`; date seed via `init.sql` |
| **Server-ID binlog MySQL** | Intervale fără suprapunere 5600–6099 impuse prin lint CI + template imagine de bază | Aceleași intervale impuse prin `JobConfig`; KC folosește rezervat 5500–5599 |
| **Kafka** | Confluent Kafka Cloud (managed) | Container KRaft `cp-kafka:7.8.0`; broker unic; `localhost:9092` |
| **Kafka Connect** | Confluent managed KC pentru SFTP (20) + SingleStore (1); înlocuit pentru 74 conectori CDC | Container KC local + Debezium + SMT-uri personalizate; comparație alăturată doar |
| **Checkpointing** | Bucket S3 (per-job `checkpointing.dir`); permisiuni IRSA | Compatibil S3 (MinIO) via `s3://flink-checkpoints`; backend RocksDB incremental, checkpoint-uri persistate în MinIO; aceeași configurație cod (interval 30 s, EXACTLY_ONCE) |
| **CI/CD** | Jenkins (build imagine, ștergere `yq`, selectare variantă) + ArgoCD (deploy/restart) | `./gradlew all` (build → restart compose → deploy conectori → CT-uri) |
| **Monitorizare** | Datadog | Flink Dashboard `:8081` + Kafka UI `:8080` + KC REST `:8083` + Prometheus `:9090` + Grafana `:3001` |
| **Versiune Java** | 17 (joburi Flink) | 17 (joburi Flink); 11 (KC SMT-uri — constrângere cp-kafka-connect 7.6.1) |
| **IAM / Securitate** | Token-uri RDS IAM, IRSA, gestionare lease binlog | Fără IAM; credențiale plain `flink`/`flink`; testarea rotației nu este posibilă |
| **Re-snapshot** | Savepoint + ștergere checkpoint S3 + re-rulare cu `--fromSavepoint` via Flink Operator; risc lease binlog; cale oficială în FLINK_SAVEPOINT_RUNBOOK.md (Slide 15, Spike S9) | Anulare job, ștergere stare, re-submitere (`flink cancel <JOB_ID>` + `flink run`) |
| **Backend stare** | RocksDB (producție) | RocksDB incremental, memorie gestionată (la fel ca producția; setat via `FLINK_PROPERTIES` / `flinkConfiguration`, nu în codul job-ului) |
| **Nomenclatură topicuri Kafka** | `<echipă>.<schemă>.<tabel>` cu prefixe per variantă pentru toate cele 26 de echipe | `poc.flink.<variantă>.<tabel>` (Flink) / `poc.kc.<variantă>` (Kafka Connect); schemă unică `poc_db` |
| **Observabilitate** | Flink Platform Team / fiecare echipă (config.tf) | nu este necesar |
| **Scară** | 74 conectori CDC → 26 echipe | 1 schemă (`poc_db`), 3 tabele (`orders`, `customers`, `outbox_events`), 5 variante, 57 teste unitare + CT per variantă |
| **Submitere YAML Pipeline** | `flink-cdc.sh` via init-container sau `kubectl exec`; `FlinkDeployment` pornește cu JM gol până când este cablat | Containerul `flink-cdc-submitter` rulează `flink-cdc.sh` automat la JM gata |

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
- `HOW-TO-RUN-THIS-POC.md` - cum facem sa rulam acest POC
