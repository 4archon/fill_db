package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"log"
	"database/sql"
	"github.com/lib/pq"
)

func readCSV(fileName string) [][]string {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}

	return records[1:]
	// return records
}

func codeActive(str string) string {
	switch str {
	case "Деактивирована":
		return "d"
	case "Демонтирована":
		return "m"
	default:
		return "a"
	}
}

func fillPoints(db *sql.DB, points [][]string) {
	connection, err := db.Begin()
	if err != nil {
		log.Println(err)
	}

	for _, i := range points {
		pointID := i[0]
		active := codeActive(i[1])
		_, err = connection.Exec("insert into points(id, active) values($1, $2)", pointID, active)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		userIDs := pq.Array([]int{1})
		reportRow := connection.QueryRow(`insert into report(point_id, user_id) values($1, $2)
		returning id`, pointID, userIDs)
		var reportID int
		err = reportRow.Scan(&reportID)
		if err != nil {
			println(pointID)
			connection.Rollback()
			log.Println(err)
			return
		}

		var logRow *sql.Row
		if i[3] == "" {
			logRow = connection.QueryRow(`insert into change_points_log values(
				default, $1, $2, $3, $4, $5, $6, $7, $8, $9) returning id`,
				pointID, i[8], i[7], i[2], i[6], i[4], i[5], i[9], nil)	
		} else {
			logRow = connection.QueryRow(`insert into change_points_log values(
				default, $1, $2, $3, $4, $5, $6, $7, $8, $9) returning id`,
				pointID, i[8], i[7], i[2], i[6], i[4], i[5], i[9], i[3])
		}
		var logID int
		err = logRow.Scan(&logID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update report set change_point_id = $1, submission_date = $2, 
		sent_worker = $3, verified = $4 where id = $5`, logID, nil, true, false, reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update report set verified = $1, active = $2 where id = $3`,
		true, true, reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

	}
	err = connection.Commit()
	if err != nil {
		log.Println(err)
	}
}

func fillUsers(db *sql.DB, users [][]string) {
	connection, err := db.Begin()
	if err != nil {
		log.Println(err)
		return
	}

	for _, i := range users {
		_, err = connection.Exec(`insert into users(id, tg_id) values($1, $2)`, i[0], i[1])
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}
	}

	err = connection.Commit()
	if err != nil {
		log.Println(err)
		return
	}
}

func fillService(db *sql.DB, service [][]string) {
	connection, err := db.Begin()
	if err != nil {
		log.Println(err)
		return
	}

	for _, i := range service {
		pointID := i[0]
		userID := i[1]

		var reportRow *sql.Row
		if userID == "" {
			reportRow = connection.QueryRow(`insert into report(point_id, user_id) values($1, $2)
			returning id`, pointID, nil)
		} else {
			userIDs := pq.Array([]string{userID})
			reportRow = connection.QueryRow(`insert into report(point_id, user_id) values($1, $2)
			returning id`, pointID, userIDs)
		}
		var reportID int
		err = reportRow.Scan(&reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		serviceLogRow := connection.QueryRow(`insert into service_log values(default,
		$1, $2) returning id`, pointID, i[2])
		var serviceLogID int
		err = serviceLogRow.Scan(&serviceLogID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		if i[5] == "" {
			_, err = connection.Exec(`insert into service_log_data values(default,
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			serviceLogID, i[3], i[4], nil, i[6], i[7], i[8], i[9], i[10], i[11], i[12])
		} else {
			_, err = connection.Exec(`insert into service_log_data values(default,
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			serviceLogID, i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12])
		}
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update report set service_log_id = $1, submission_date = $2, 
		sent_worker = $3, verified = $4 where id = $5`, serviceLogID, i[2], true, false, reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update report set verified = $1, active = $2 where id = $3`,
		true, true, reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}
	}

	err = connection.Commit()
	if err != nil {
		log.Println(err)
		return
	}
}

func fillInspection(db *sql.DB, inspections [][]string) {
	connection, err := db.Begin()
	if err != nil {
		log.Println(err)
		return
	}

	for _, i := range inspections {
		pointID := i[0]
		userID := i[1]

		var reportRow *sql.Row
		if userID == "" {
			reportRow = connection.QueryRow(`insert into report(point_id, user_id) values($1, $2)
			returning id`, pointID, nil)
		} else {
			userIDs := pq.Array([]string{userID})
			reportRow = connection.QueryRow(`insert into report(point_id, user_id) values($1, $2)
			returning id`, pointID, userIDs)
		}
		var reportID int
		err = reportRow.Scan(&reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		inspectionLogRow := connection.QueryRow(`insert into inspection_log values(default,
		$1, $2) returning id`, pointID, i[2])
		var inspectionLogID int
		err = inspectionLogRow.Scan(&inspectionLogID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`insert into inspection_log_data values(default,
			$1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			inspectionLogID, i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10])
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update report set inspection_log_id = $1, submission_date = $2, 
		sent_worker = $3, verified = $4 where id = $5`, inspectionLogID, i[2], true, false, reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update report set verified = $1, active = $2 where id = $3`,
		true, true, reportID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}
	}

	err = connection.Commit()
	if err != nil {
		log.Println(err)
		return
	}
}

func main() {
	service := readCSV("serv_log.csv")
	inspec := readCSV("ins_log.csv")
	points := readCSV("points.csv")
	users := readCSV("users.csv")
	connectionString := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=%s default_transaction_isolation=%s",
	"kn17", "a1", "maps", "disable", "serializable")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Println(err.Error())
		return
	}

	fillPoints(db, points)
	fillUsers(db, users)
	fillService(db, service)
	fillInspection(db, inspec)
}
