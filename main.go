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
		pointID := i[0]
		active := codeActive(i[1])
		_, err = connection.Exec("insert into points(id, active) values($1, $2)",
			pointID, active)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		var logRow *sql.Row
		if i[3] == "" {
			logRow = connection.QueryRow(`insert into points_log values(
				default, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning id`,
				pointID, i[8], i[7], i[2], i[6], i[4], i[5], i[9], nil, nil)	
		} else {
			logRow = connection.QueryRow(`insert into points_log values(
				default, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning id`,
				pointID, i[8], i[7], i[2], i[6], i[4], i[5], i[9], i[3], nil)
		}
		var logID int
		err = logRow.Scan(&logID)
		if err != nil {
			connection.Rollback()
			log.Println(err)
			return
		}

		_, err = connection.Exec("update points set change_id = $1 where id = $2",
			logID, pointID)
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
		typeOfWork := i[13]
		if i[5] == "" {
			i[5] = "1"
		}

		var serviceRow *sql.Row
		if userID == "" {
			serviceRow = connection.QueryRow(`insert into service
			(point_id, user_id, appointment_date, execution_date, comment)
			values($1, $2, $3, $4, $5) returning id`, pointID, nil, i[2], i[2], i[11])
		} else {
			userIDs := pq.Array([]string{userID})
			serviceRow = connection.QueryRow(`insert into service
			(point_id, user_id, appointment_date, execution_date, comment)
			values($1, $2, $3, $4, $5) returning id`, pointID, userIDs, i[2], i[2], i[11])
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
				if i[4] == "Монтаж старой точки" {
					work = "Монтаж"
					task_type = "Монтаж старой точки"
				} else if i[4] == "Покраска комплекс" {
					work = "Покраска комплекс"
				} else if i[4] == "Демонтаж-монтаж" {
					work = "Демонтаж-монтаж"
				} else if i[4] == "Покраска" {
					work = "Покраска"
				} else if i[4] == "Перенос" {
					work = "Демонтаж-монтаж"
					task_type = "Перенос точки"
				}
			} else if i[3] == "Демонтаж" {
				if i[4] == "Демонтаж НЕ временный" {
					work = "Демонтаж"
					task_type = "Деактивация точки"
				} else if i[4] == "Демонтаж временный" {
					work = "Демонтаж"
					status = "Временно демонтирована"
					task_type = "Временный демонтаж по разным причинам"
				} else if i[4] == "Частичный демонтаж" {
					work = "Демонтаж"
					task_type = "Частичный демонтаж"
				}
			} else if i[3] == "Монтаж новой точки" {
				if i[4] == "Монтаж новой точки" {
					work = "Монтаж"
					task_type = "Монтаж новой точки"
				}
			}
		} else if typeOfWork == "inspection" {
			work_type = "required"
			task_type = "Проинспектировать"

			if i[3] == "Точка требует ремонта" {
				if i[4] == "Монтаж старой точки" {
					work = "Монтаж"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Монтаж старой точки")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} else if i[4] == "Покраска" {
					work = "Покраска"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Произвести сервис")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} else if i[4] == "Демонтаж-монтаж + Покраска" {
					work = "Демонтаж-монтаж"
					_, err = connection.Exec(`insert into service_works(service_id, type, work, arc)
							values($1, $2, $3, $4)`, serviceID, work_type, "Покраска", i[5])
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Произвести сервис")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} else if i[4] == "Демонтаж-монтаж" {
					work = "Демонтаж-монтаж"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Произвести сервис")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} else if i[4] == "Временный демонтаж" {
					work = "Демонтаж"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Временный демонтаж по разным причинам")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} else if i[4] == "Замена на алюминиевую дугу" {
					work = "Демонтаж-монтаж"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Замена на алюминиевую дугу")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} else if i[4] == "Забрать дуги срочно" {
					work = "Демонтаж"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Благоустройство - Временный демонтаж")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} else if i[4] == "частичное нанесение" {
					work = "Частичное нанесение"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Частичное нанесение")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				} 
			} else if i[3] == "Ремонт" {
				if i[4] == "Монтаж старой точки" {
					work = "Монтаж"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Монтаж старой точки")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
				}
			} else if i[3] == "Точка не требует ремонта" {
				if i[4] == "Уточнение не требуется" {
					work = "Работа не требуется"
				} else if i[4] == "Уточнений не требуется" {
					work = "Работа не требуется"
				} else if i[4] == "Идет благоустройство" {
					work = "Работа не требуется"
					status = "Идет блогоустройство"
				}
			} else if i[3] == "Установка новой точки" {
				if i[4] == "Монтаж новой точки" {
					work = "Монтаж"
					_, err = connection.Exec(`insert into tasks(point_id, type)
							values($1, $2)`, pointID, "Монтаж новой точки")
					if err != nil {
						connection.Rollback()
						log.Println(err)
						return
					}
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

		_, err = connection.Exec(`insert into tasks(point_id, type, service_id)
				values($1, $2, $3)`, pointID, task_type, serviceID)
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

		if typeOfWork == "service" {
			_, err = connection.Exec(`update tasks set service_id = $1
			where point_id = $2 and service_id is null`, serviceID, pointID)
			if err != nil {
				connection.Rollback()
				log.Println(err)
				return
			}
		}

		if i[6] != "" {
			createMedia(workID, "jpeg", "b" + i[6], connection)
		}
		if i[7] != "" {
			createMedia(workID, "jpeg", "l" + i[7], connection)
		}
		if i[8] != "" {
			createMedia(workID, "jpeg", "r" + i[8], connection)
		}
		if i[9] != "" {
			createMedia(workID, "jpeg", "f" + i[9], connection)
		}
		if i[12] != "" {
			createMedia(workID, "jpeg", "e" + i[12], connection)
		}
		if i[10] != "" {
			createMedia(workID, "mov", "v" + i[10], connection)
		}
	}

	err = connection.Commit()
	if err != nil {
		log.Println(err)
		return
	}
}

func createMedia(workID int, mediaType string, name string, connection *sql.Tx) {
	_, err := connection.Exec(`insert into media(work_id, media_type, media_name)
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
}
