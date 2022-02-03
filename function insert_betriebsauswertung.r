insert_betriebsauswertung <- function(betrieb, pg_schema, jahr, monat, db){
	insert_query <- read_file('insert betriebsauswertung.sql')
	betriebsauswertung_schema <- paste0('betriebsauswertung_', betrieb)
	betriebsauswertung_tabel <- paste0('betriebsauswertung_', betrieb)
	insert_query <- str_glue(insert_query)
	return(insert_query)
	# dbExecute(db, insert_query)
}
