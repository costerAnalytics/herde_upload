update_betriebsauswertung <- function(betrieb, pg_schema, jahr, monat, db){
	update_query <- read_file('update betriebsauswertung.sql')
	betriebsauswertung_schema <- paste0('betriebsauswertung_', betrieb)
	betriebsauswertung_tabel <- paste0('betriebsauswertung_', betrieb)
	update_query <- str_glue(update_query)
	return(update_query)
	# dbExecute(db, update_query)
}
