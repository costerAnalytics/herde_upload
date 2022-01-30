# Fuer Test: loeschet 1000 Spalten von jede Tabelle.

options(warn = 2)

library(RPostgres)
library(dotenv)
load_dot_env()

pg_schema <- paste0('test_herde_', DATABASE)

pgdb <- dbConnect(Postgres(), host = Sys.getenv('pg_host'),
	dbname = Sys.getenv('pg_dbname'),
	user = Sys.getenv('pg_user'), pass=Sys.getenv('pg_pwd'))

querpg <- function(...) dbGetQuery(pgdb, paste0(...))

tabellen <- c(
  'hw_bestand',
  'hw_ort',
  'hw_bewegung',
  'hw_kalbung',
  'hw_laktation',
  'hw_besamung',
  'hw_zstatus',
  'hw_schluessel',
  'hw_tu',
  'hw_techtag',
  'hw_gesundheit',
  'hw_schl_gesund')

for(tabel in tabellen){
	print(tabel)
	pg_tabel <- paste0(pg_schema, '.', tabel)
	max_id <- dbGetQuery(pgdb, paste0('select max(id) from ', pg_tabel))[1,1]
	dbExecute(pgdb, paste0('delete from ', pg_tabel, ' where id > ', max_id - 1000))
}
dbDisconnect(pgdb)
