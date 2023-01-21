package db

const (
	racesColumnsList = "columnsList"
	racesList        = "list"
)

func getRaceQueries() map[string]string {
	return map[string]string{
		racesColumnsList: `SELECT name FROM pragma_table_info('races')`,
		racesList: `
			SELECT 
				id, 
				meeting_id, 
				name, 
				number, 
				visible, 
				advertised_start_time 
			FROM races
		`,
	}
}
