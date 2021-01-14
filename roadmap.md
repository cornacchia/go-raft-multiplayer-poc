1. engine che parla con ui che parla con nodi raft **conosciuti a priori**, giro completo
2. configuration change per aggiungere nuovi nodi dinamicamente
3. snapshot per evitare di usare infinita memoria
4. test vari
   1. semplice AI da far muovere in giro per il mondo di gioco (avviare il gioco in modalità AI)
   2. test di prestazioni (e.g. tempo intercorso tra invio di un comando e ricezione della risposta in base al numero di giocatori)
   3. test di funzionamento di raft
      1. asserzioni? https://github.com/sheremetat/assert
      2. test di integrazione che verificano che dopo un tot sono successe determinate cose
5. cose bizantine se avanza abbastanza tempo e in parallelo alla relazione


# Sicuri
* Configuration changes: per aggiungere, togliere nuovi nodi
* Go Engine: abbastanza semplice e veloce
* Connessione raft - go engine (single ui con cluster raft base dietro)
  * Nota: il cluster è composto di nodi che si conoscono a vicenda
  * Un client deve conoscere l'indirizzo di almeno un nodo a cui poter chiedere gli altri indirizzi
* Mostrare altri personaggi - from single to multiplayer

# Potenziali
* Snapshot per ridurre le slice di log (basterebbe l'ultimo stato di ogni giocatore)
* Test per assicurare il funzionamento del protocollo raft
* Evoluzione verso situazioni bizantine