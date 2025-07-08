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

func codeActive(str string) bool {
	switch str {
	case "Деактивирована":
		return false
	case "Демонтирована":
		return false
	default:
		return true
	}
}

func fillPoints(db *sql.DB, points [][]string) {
	connection, err := db.Begin()
	if err != nil {
		log.Println(err)
	}

	for _, i := range points {
		pointID := i[10]
		active := codeActive(i[1])

		_, err = connection.Exec(`insert into points values($1, $2, 
		$3, $4, $5, $6, $7, $8, $9,
		$10,
		$11, $12, $13, $14, $15)`,
		pointID, active,
		i[9], i[8], i[2], i[6], i[4], i[5], i[7],
		func(s *string) *string {if *s == "" {return nil} else {return s}}(&i[3]),
		nil, nil, i[11], i[12], i[0])
		
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

func fillMarking(db *sql.DB, points [][]string) {
	connection, err := db.Begin()
	if err != nil {
		log.Println(err)
	}

	for _, i := range points {
		pointID := i[11]
		active := codeActive(i[1])

		_, err = connection.Exec(`insert into points values($1, $2, 
		$3, $4, $5, $6, $7, $8, $9,
		$10,
		$11, $12, $13, $14, $15)`,
		pointID, active,
		i[10], i[9], i[2], i[7], i[4], i[6], i[8],
		func(s *string) *string {if *s == "" {return nil} else {return s}}(&i[3]),
		nil, nil, i[12], i[13], i[0])
		
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`insert into markings values(default,
		$1, $2, $3)`, pointID, i[5], "Термопластик")
		
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
		typeOfWork := i[12]
		if i[5] == "" {
			i[5] = "1"
		}

		var serviceRow *sql.Row
		if userID == "" {
			serviceRow = connection.QueryRow(`insert into service
			(point_id, user_id, appointment_date, execution_date, sent, sent_by, without_task)
			values($1, $2, $3, $4, $5, $6, $7) returning id`,
			pointID, nil, i[2], i[2], true, nil, false)
		} else {
			userIDs := pq.Array([]string{userID})
			serviceRow = connection.QueryRow(`insert into service
			(point_id, user_id, appointment_date, execution_date, sent, sent_by, without_task)
			values($1, $2, $3, $4, $5, $6, $7) returning id`,
			pointID, userIDs, i[2], i[2], true, userID, false)
		}
		var serviceID int
		err = serviceRow.Scan(&serviceID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		status := "Точка доступна"
		work_type := ""
		work := ""
		task_type := ""

		if typeOfWork == "service" {
			work_type = "done"
			task_type = "Произвести сервис"

			if i[3] == "Ремонт" {
				switch i[4] {
				case "Покраска комплекс":
					work = "Покраска"
				case "Покраска":
					work = "Покраска"
				case "Перенос":
					work = "Демонтаж-монтаж"
					task_type = "Перенос точки"
				case "Монтаж старой точки":
					work = "Монтаж"
					task_type = "Монтаж старой точки"
				case "Демонтаж-монтаж":
					work = "Демонтаж-монтаж"
				}
			} else if i[3] == "Монтаж новой точки" {
				switch i[4] {
				case "Монтаж новой точки":
					work = "Монтаж"
					task_type = "Монтаж новой точки"
				}
			} else if i[3] == "Демонтаж" {
				switch i[4] {
				case "Частичный демонтаж":
					work = "Демонтаж"
					task_type = "Частичный демонтаж"
				case "Демонтаж временный":
					work = "Демонтаж"
					task_type = "Временный демонтаж по разным причинам"
					status = "Временно демонтирована"
				case "Демонтаж НЕ временный":
					work = "Демонтаж"
					task_type = "Снятие всех дуг"
					// возможно надо поменять статус
				}
			}
		} else if typeOfWork == "inspection" {
			work_type = "required"
			task_type = "Проинспектировать"

			if i[3] == "Точка требует ремонта" {
				switch i[4] {
				case "частичное нанесение":
					work = "Частичное нанесение"
				case "Покраска":
					work = "Покраска"
				case "Монтаж старой точки":
					work = "Монтаж"
				case "Замена на алюминиевую дугу":
					work = "Демонтаж-монтаж"
				case "Забрать дуги срочно":
					work = "Демонтаж"
				case "Демонтаж-монтаж + Покраска":
					work = "Демонтаж-монтаж"
					_, err = connection.Exec(`insert into service_works(service_id, type, work, arc)
							values($1, $2, $3, $4)`, serviceID, work_type, "Покраска", i[5])
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				case "Демонтаж-монтаж":
					work = "Демонтаж-монтаж"
				case "Временный демонтаж":
					work = "Демонтаж"
				}
			} else if i[3] == "Точка не требует ремонта" {
				switch i[4] {
				case "Уточнений не требуется":
					work = "Работа не требуется"
				case "Уточнение не требуется":
					work = "Работа не требуется"
				case "Идет благоустройство":
					work = "Работа не требуется"
					status = "Идет блогоустройство"
				}
			} else if i[3] == "Ремонт" {
				switch i[4] {
				case "Монтаж старой точки":
					work = "Монтаж"
				}
			} else if i[3] == "Установка новой точки" {
				switch i[4] {
				case "Монтаж новой точки":
					work = "Монтаж"
				}
			}
		}

		workRow := connection.QueryRow(`insert into service_works(service_id, type, work, arc)
				values($1, $2, $3, $4) returning id`, serviceID, work_type, work, i[5])
		var workID int
		err = workRow.Scan(&workID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`insert into tasks(point_id, type, service_id, entry_date, done)
				values($1, $2, $3, $4, $5)`, pointID, task_type, serviceID, i[2], true)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update service set status = $1 where id = $2`,
		status, serviceID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec(`update points set status = $1 where id = $2`,
		status, pointID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		if typeOfWork == "service" {
			_, err = connection.Exec(`update tasks set service_id = $1, done = $2
			where point_id = $3 and done is null`, serviceID, true, pointID)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		} else if typeOfWork == "inspection" {
			addTaskAfterInspection(i[3], i[4], connection, pointID, i[2])
		}

		if i[6] != "" {
			createMedia(serviceID, "jpeg", "b" + i[6], connection)
		}
		if i[7] != "" {
			createMedia(serviceID, "jpeg", "l" + i[7], connection)
		}
		if i[8] != "" {
			createMedia(serviceID, "jpeg", "r" + i[8], connection)
		}
		if i[9] != "" {
			createMedia(serviceID, "jpeg", "f" + i[9], connection)
		}
		if i[11] != "" {
			createMedia(serviceID, "jpeg", "e" + i[11], connection)
		}
		if i[10] != "" {
			createMedia(serviceID, "mov", "v" + i[10], connection)
		}
	}

	err = connection.Commit()
	if err != nil {
		log.Println(err)
		return
	}
}

func addTaskAfterInspection(service_type string, subtype string, connection *sql.Tx,
	pointID string, entry_date string) {
	if service_type == "Точка требует ремонта" {
		switch subtype {
		case "частичное нанесение":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Частичное нанесение", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		case "Покраска":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Произвести сервис", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		case "Монтаж старой точки":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Монтаж старой точки", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		case "Замена на алюминиевую дугу":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Замена дуги на алюминиевую", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		case "Забрать дуги срочно":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Благоустройство - Временный демонтаж", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		case "Демонтаж-монтаж + Покраска":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Произвести сервис", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		case "Демонтаж-монтаж":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Произвести сервис", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		case "Временный демонтаж":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Временный демонтаж по разным причинам", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		}
	} else if service_type == "Ремонт" {
		switch subtype {
		case "Монтаж старой точки":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Монтаж старой точки", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		}
	} else if service_type == "Установка новой точки" {
		switch subtype {
		case "Монтаж новой точки":
			_, err := connection.Exec(`insert into tasks(point_id, type, entry_date)
					values($1, $2, $3)`, pointID, "Монтаж новой точки", entry_date)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		}
	}
}

func createMedia(workID int, mediaType string, name string, connection *sql.Tx) {
	_, err := connection.Exec(`insert into media(service_id, media_type, media_name)
	values($1, $2, $3)`, workID, mediaType, name)
	if err != nil {
		connection.Rollback()
		log.Println(err)
		return
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile);
	service := readCSV("serv_log.csv")
	points := readCSV("points.csv")
	users := readCSV("users.csv")
	marking := readCSV("marking.csv")
	connectionString := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=%s default_transaction_isolation=%s",
	"kn17", "a1", "maps", "disable", "serializable")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Println(err.Error())
		return
	}

	fillPoints(db, points)
	fillMarking(db, marking)
	fillUsers(db, users)
	fillService(db, service)
}
