## Projekt: Analiza tweetów w Apache Spark

Ten projekt jest przykładem wykorzystania **Apache Spark** (Spark SQL) w Scali do:
- **ładowania** danych o tweetach z kilku plików CSV,
- **czyszczenia i przygotowania** danych (typy, listy hashtagów),
- **wyszukiwania** tweetów po słowach kluczowych i lokalizacji,
- **analizy** (np. zliczanie źródeł tweetów, hashtagów).

Kod jest pisany z myślą o nauce podstaw Sparka – ma pokazywać typowe operacje na `DataFrame`.

---

## Wymagania

- **Java JDK 8+**
- **sbt** (Scala Build Tool)
- Spark wbudowany jako zależność w projekcie (`spark-core` i `spark-sql` 3.5.0 – patrz `build.sbt`)
- Dane wejściowe w katalogu `data/`:
  - `GRAMMYs_tweets.csv`
  - `financial.csv`
  - `covid19_tweets.csv`

Pliki powinny mieć nagłówki (czytane z `header = true`) i m.in. kolumny:
- `text`, `hashtags`, `is_retweet`, `source`
- `user_name`, `user_location`, `user_followers`, `user_friends`, `user_favourites`, `user_created`
- `date`

---

## Struktura projektu

- `src/main/scala/App.scala` – główny punkt wejścia aplikacji:
  - inicjuje `SparkSession`,
  - ładuje wszystkie tweety,
  - czyści dane,
  - filtruje tweety zawierające słowo **"Trump"** z lokalizacji **"United States"**,
  - liczy częstość występowania wartości w kolumnie `source` i wyświetla wynik.

- `src/main/scala/loaders/TweetsLoader.scala`  
  - `TweetsLoader` – ładowanie danych z trzech plików CSV, dodanie kolumny `category` i łączenie w jeden `DataFrame`.

- `src/main/scala/cleaners/TweetsCleaner.scala`  
  - `TweetsCleaner` – przygotowanie danych:
    - czyszczenie kolumny `hashtags` i zamiana na tablicę stringów,
    - rzutowanie dat (`date`, `user_created`) na `DateType`,
    - rzutowanie wybranych kolumn liczbowych użytkownika na `LongType`.

- `src/main/scala/analysers/TweetsAnalyzer.scala`  
  - `TweetsAnalyzer` – metody analityczne:
    - `calculateHashtagsFrequency`
    - `calculateIsRetweetCount`
    - `calculateSourceFrequency`
    - `calculateAvgUserFollowersPerLocation`

- `src/main/scala/analysers/TweetsSearch.scala`  
  - `TweetsSearch` – funkcje do wyszukiwania:
    - `searchByKeyWord(keyWord)`
    - `searchByKeyWords(keyWords: Seq[String])`
    - `onlyInLocation(location)`

- `build.sbt` – konfiguracja projektu i zależności (Scala 2.13.16, Spark 3.5.0).

---

## Jak uruchomić projekt

1. **Sklonuj / pobierz** repozytorium i przejdź do katalogu projektu:

```bash
cd fundamenty-sparka-projekt
```

2. **Upewnij się, że dane są w katalogu `data/`** (względem katalogu głównego projektu) i mają oczekiwane nazwy.

3. **Uruchom aplikację przy użyciu sbt**:

```bash
sbt run
```

Domyślna klasa startowa to `App` (Spark job uruchamiany lokalnie z `master("local")`).

---

## Dalsze eksperymenty

Kilka pomysłów na samodzielne rozszerzenia:
- zmiana kryteriów wyszukiwania (inne słowa kluczowe, inna lokalizacja),
- wykorzystanie pozostałych metod z `TweetsAnalyzer` w `App.scala`,
- zapis wyników do plików (np. `parquet`, `csv`) zamiast tylko `show()`,
- dodanie testów jednostkowych do kluczowych funkcji (np. logicznych filtrów w `TweetsSearch`).

