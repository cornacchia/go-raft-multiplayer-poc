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