options(warn = 2)

library(lubridate)
library(RODBC)
library(RPostgres)
library(dotenv)
library(dplyr)
library(stringr)
library(readr)
load_dot_env()


log_to_file <- function(x){
  file <- paste0('log_', as.Date(Sys.time()), '.txt')
  cat(paste0(format(Sys.time(), '%H:%M:%S'),'\t', x, '\n'), file = file, append = TRUE)
}

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
	'pg_schema',
	'betrieb',
	'gbak'
)){
	if(Sys.getenv(variabele) == '') stop(paste0('Es fehlt Systemvariabele ', variabele))
}

herde_backup_folder <- Sys.getenv('herde_backup_folder')
temp_herde_folder <- Sys.getenv('temp_herde_folder')
pg_schema <- Sys.getenv('pg_schema')
betrieb <- Sys.getenv('betrieb')

zip_file <- dir(path = herde_backup_folder, pattern = '.zip')
zip_file <- zip_file[substr(zip_file, 1, 6) == 'herde_']
zip_file <- zip_file[length(zip_file)]
zip_file <- paste0(herde_backup_folder, zip_file)
unzip(zip_file, overwrite = TRUE, exdir = temp_herde_folder)
system(paste0(Sys.getenv('gbak'), ' -PAS ', Sys.getenv('herde_pwd'), ' -USER ', Sys.getenv('herde_uid'), ' -REP -c "', temp_herde_folder, '\\HerdeW.fbk" "', temp_herde_folder, '\\', Sys.getenv('herde_backup_db'), '.fdb"'))

odbc_herde <- odbcConnect(Sys.getenv('herde_backup_db'), 
	uid = Sys.getenv('herde_uid'), pwd = Sys.getenv('herde_pwd'), 
	case = 'toupper', DBMSencoding ='utf8')

pgdb <- dbConnect(Postgres(), host = Sys.getenv('pg_host'),
	dbname = Sys.getenv('pg_dbname'),
	user = Sys.getenv('pg_user'), pass=Sys.getenv('pg_pwd'))

tabellen <- sqlTables(herde)
tabellen <- tabellen[tabellen$TABLE_TYPE == 'TABLE',]
tabellen <- tabellen$TABLE_NAME
tabellen <- tolower(tabellen)
tabellen <- tabellen[!grepl("$", tabellen, fixed = TRUE)]

querpg <- function(...) dbGetQuery(pgdb, paste0(...))
querherde <- function(...) sqlQuery(odbc_herde, paste0(...))

monaten_neu <- NULL # data.frame mit (Jahr, Monat) die neu sind.

for(tabel in tabellen){
	cat(tabel)
	tryCatch({
		max_alt_id <- querpg('select max(id) from ', pg_schema, '.', tabel)[1, 1]
		if(is.na(max_alt_id)) max_alt_id <- 0
		max_neu_id <- querherde('select max(id) from ', tabel)[1, 1]
		if(is.na(max_neu_id) || max_neu_id == max_alt_id){
			cat(': keine neue Dateien\n')
		  log_to_file(paste0(tabel, ': 0 neue Spalten'))
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
		
		if(tabel != 'hw_besamung' && tabel != 'hw_tu' && !is.null(dat$datum)){
		  neue_monaten_aux <- as.data.frame(unique(cbind(year(dat$datum), month(dat$datum))))
		  colnames(neue_monaten_aux) <- c('jahr','monat')
		  monaten_neu <- rbind(monaten_neu, neue_monaten_aux)
		}
		
		cat(paste0(': ', nrow(dat), ' neue Spalten schreiben... '))
		dbWriteTable(pgdb, name = DBI::SQL(paste0(pg_schema, ".", tabel)), 
			value = dat, row.names = FALSE, append = TRUE)
		cat('ok\n\n')
		log_to_file(paste0(tabel, ': ', nrow(dat), ' neue Spalten'))
	}, error = \(e){
	  cat(e$message)
	  cat('\n')
	  log_to_file(paste0(tabel, ': ', e$message))
	})
}

# Work in progress: taeglisch Betriebsauswertung aktualisieren. Wird noch nicht ausgefuehrt.
if(FALSE && !is.null(monaten_neu)){
  monaten_neu <- monaten_neu |> 
    filter(!is.na(jahr)) |>
    distinct() |>
    arrange(jahr, monat)
  baus <- paste0('betriebsauswertung_', 
                 betrieb, '.betriebsauswertung_', betrieb)
  baus_min <- querpg(
    'select jahr, monat from ', baus, ' order by jahr, monat limit 1')
  monaten_neu <- monaten_neu |> 
    filter(jahr >= baus_min$jahr && (
      jahr > baus_min$jahr || monat > baus_min$monat))
}

if(FALSE && !is.null(monaten_neu) && nrow(monaten_neu) > 0){
  for(i in 1:nrow(monaten_neu)){
    jahr <- monaten_neu$jahr[i]
    monat <- monaten_neu$monat[i]
    
    bestehend <- querpg(
      'select count(*) from ', baus, 
      ' where jahr = ', jahr,
      ' and monat = ', monat)[1,1]
    bestehend <- as.logical(bestehend)
    
    if(!bestehend){
      log_to_file(str_glue('update betriebsauswertung_{betrieb}: ({jahr}, {monat})'))
      insert_betriebsauswertung(betrieb = betrieb, pg_schema = pg_schema, jahr = jahr, monat = monat, db = db)
    } else{
      log_to_file(str_glue('insert betriebsauswertung_{betrieb}: ({jahr}, {monat})'))
      update_betriebsauswertung(betrieb = betrieb, pg_schema = pg_schema, jahr = jahr, monat = monat, db = db)
    }
  }
}

odbcCloseAll()
dbDisconnect(pgdb)
