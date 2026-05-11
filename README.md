# Università degli Studi Roma Tre - Corso di Big Data
## Secondo Progetto: Analisi comparativa di tecnologie Big Data

Questo repository contiene il codice sorgente e la documentazione relativi al secondo progetto del corso di Big Data.

### 1. Oggetto del Progetto
L'obiettivo del progetto è sperimentare l'uso comparativo di diverse tecnologie per l'analisi di big data su un dataset reale di grandi dimensioni. In particolare, il progetto si concentra su:
- preparazione e pulizia dei dati
- implementazione delle interrogazioni
- esecuzione ed analisi

Il dataset utilizzato è il **Flight Delay Dataset del 2024** (disponibile su Kaggle), che contiene informazioni relative a voli, ritardi, cancellazioni e altri attributi operativi.

### 2. Tecnologie Utilizzate
Le analisi sono state implementate e confrontate utilizzando una combinazione delle seguenti tecnologie:
- **Hive**
- **Spark Core**
- **Spark SQL**

### 3. Analisi Realizzate
Sono state implementate le seguenti analisi in base alle specifiche:

#### 3.1 Statistiche delle compagnie aeree
Generazione di statistiche per ciascuna compagnia aerea, includendo le tratte servite, il numero di voli, il ritardo minimo, massimo e medio in arrivo, il tasso di cancellazione e l'elenco dei mesi di operatività.

#### 3.2 Report dei ritardi per aeroporto e periodo temporale
Generazione di un report per ciascun aeroporto di partenza e mese, indicando il numero di voli per fasce di ritardo (basso: <15 min, medio: 15-60 min, alto: >60 min), il ritardo medio in partenza e in arrivo, e le cause più frequenti di cancellazione o ritardo.

#### 3.3 Ranking delle coppie compagnia-aeroporto con comportamento anomalo
Generazione di un report che confronta il comportamento di ciascuna compagnia in un determinato aeroporto con il comportamento medio di tutte le compagnie nello stesso aeroporto, includendo ritardi medi, tasso di cancellazione, differenza rispetto alla media dell'aeroporto e posizione in classifica.

### 4. Preparazione dei Dati
Il dataset è stato sottoposto a una fase di pulizia e preparazione prima dell'analisi, che include:
- Eliminazione di record errati, incompleti o non significativi.
- Normalizzazione dei valori degli attributi utilizzati.
- Selezione delle colonne rilevanti ai fini delle analisi.
- Trasformazione di attributi temporali e categorici.

Le specifiche dettagliate della preparazione dei dati sono descritte nel rapporto finale.

### 5. Esecuzione e Riproducibilità
Il repository contiene gli script necessari per l'esecuzione delle analisi sia in ambiente locale che su cluster.

**Configurazione e avvio:**
- `src/run_setup.sh`: Script per l'avvio in locale.
- `src/run_setup_cluster.sh`: Script per l'avvio e la configurazione dell'ambiente cluster (HDFS/YARN).
- Seguire le istruzioni aggiuntive fornite per eseguire i singoli job MapReduce, script Hive e job Spark (Core/SQL).

### 6. Struttura del Repository
- `src/`: Codice sorgente delle implementazioni suddivise per tecnologia (es. `spark_core`, `spark_sql`, `sql`, ecc.) e script di esecuzione.
- `results/`: Directory di output per i risultati in locale.
- `results_cluster/`: Directory di output per i risultati su cluster.
