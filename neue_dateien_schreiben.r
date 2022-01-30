options(warn = 2)

library(RODBC)
library(RPostgres)
library(dotenv)
library(dplyr)
load_dot_env()

for(variabele in c(
'herde_backup_folder',
'temp_herde_folder',
'herde_backup_db',
'herde_uid',
'herde_pwd',
'pg_host',
'pg_dbname',
'pg_user',
'pg_pwd', 
'pg_schema'
)){
	if(Sys.getenv(variabele) == '') stop(paste0('Es fehlt Systemvariabele ', variabele))
}

herde_backup_folder <- Sys.getenv('herde_backup_folder')
temp_herde_folder <- Sys.getenv('temp_herde_folder')

zip_file <- dir(path = herde_backup_folder, pattern = '.zip')[1]
zip_file <- paste0(herde_backup_folder, zip_file)
system(paste0(r"(c:\windows\system32\tar.exe -C )", herde_backup_folder, " -xf ", zip_file))
system(paste0(r"(c:\firebird\gbak -REP -c )", herde_backup_folder, "HerdeW.fbk ", temp_herde_folder, Sys.getenv('herde_backup_db'), ".fdb"))

odbc_herde <- odbcConnect(Sys.getenv('herde_backup_db'), 
	uid = Sys.getenv('herde_uid'), pwd = Sys.getenv('herde_pwd'), 
	case = 'toupper', DBMSencoding ='utf8')

pgdb <- dbConnect(Postgres(), host = Sys.getenv('pg_host'),
	dbname = Sys.getenv('pg_dbname'),
	user = Sys.getenv('pg_user'), pass=Sys.getenv('pg_pwd'))

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

querpg <- function(...) dbGetQuery(pgdb, paste0(...))
querherde <- function(...) sqlQuery(odbc_herde, paste0(...))

for(tabel in tabellen){
	cat(tabel)
	try({
		max_alt_id <- querpg('select max(id) from ', pg_schema, '.', tabel)[1, 1]
		if(is.na(max_alt_id)) max_alt_id <- 0
		max_neu_id <- querherde('select max(id) from ', tabel)[1, 1]
		if(is.na(max_neu_id) || max_neu_id == max_alt_id){
			cat(': keine neue Dateien\n')
			next # naechste Tabel, weil es keine neue Dateien gibt.
		}
		
		kolommen <- sqlColumns(odbc_herde, tabel)
		kolommen <- kolommen %>% select(kolom = COLUMN_NAME, type = TYPE_NAME)
		kolommen <- kolommen %>% filter(!grepl('BLOB', type))
		query <- paste(kolommen$kolom, collapse = ',')
		query <- paste('select ', query, ' from ', tabel, ' 
			where id > ', max_alt_id, ' order by id')
		dat <- querherde(query)
	  
		# Richtige Type fuer jede Zeile: 
		integers <- kolommen$kolom[kolommen$type %in% c('BIGINT','INTEGER', 'SMALLINT')]
		datums <- kolommen$kolom[kolommen$type == 'DATE']
		for(k in integers) dat[,k] <- as.integer(dat[,k])
		for(k in datums) dat[,k] <- as.Date(dat[,k])
		
		colnames(dat) <- tolower(colnames(dat))
		
		cat(paste0(': ', nrow(dat), ' neue Spalten schreiben... '))
		dbWriteTable(pgdb, name = DBI::SQL(paste0(pg_schema, ".", tabel)), 
			value = dat, row.names = FALSE, append = TRUE)
		cat('ok\n\n')
	})
}

odbcCloseAll()
dbDisconnect(pgdb)
