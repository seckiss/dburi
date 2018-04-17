package dburi

import (
	"database/sql"
	"fmt"
	"os/exec"

	"github.com/lib/pq"
)

type DbUri struct {
	Host string
	Port string
	User string
	Pass string
	Name string
}

// if password is empty take password from env var
func New(host, port, name, user, pass string) (dbUri *DbUri) {
	return &DbUri{host, port, user, pass, name}
}

// stringify to dbname URI
func (dbUri *DbUri) String() string {
	uri := "postgresql://" + dbUri.User + ":" + dbUri.Pass + "@" + dbUri.Host + ":" + dbUri.Port + "/" + dbUri.Name + "?"
	if dbUri.Host == "127.0.0.1" || dbUri.Host == "localhost" {
		uri += "sslmode=disable&"
	} else {
		uri += "sslmode=require&"
	}
	return uri
}

func (dbUri *DbUri) Dsn() (dsn string, err error) {
	return pq.ParseURL(dbUri.String())
}

func (dbUri *DbUri) Open() (*sql.DB, error) {
	db, err := sql.Open("postgres", dbUri.String())
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (dbUri *DbUri) OpenMaintenanceDb() (*sql.DB, error) {
	mnt := New(dbUri.Host, dbUri.Port, "postgres", dbUri.User, dbUri.Pass)
	return mnt.Open()
}

func (dbUri *DbUri) CreateDb(dbname string) (err error) {
	mntDb, err := dbUri.OpenMaintenanceDb()
	if err != nil {
		return err
	}
	defer mntDb.Close()
	_, err = mntDb.Exec("create database " + dbname)
	return err
}

func (dbUri *DbUri) DropDb(dbname string) (err error) {
	mntDb, err := dbUri.OpenMaintenanceDb()
	if err != nil {
		return err
	}
	defer mntDb.Close()
	_, err = mntDb.Exec("drop database if exists " + dbname)
	return err
}

func (dbUri *DbUri) KillPglogicalBackends() (err error) {
	mntDb, err := dbUri.OpenMaintenanceDb()
	if err != nil {
		return err
	}
	defer mntDb.Close()
	cmd := "SELECT pg_terminate_backend(pid) FROM pg_stat_activity where application_name like 'pglogical%';"
	_, err = mntDb.Exec(cmd)
	return err
}

// only public schema
func (dbUri *DbUri) PgDumpSchema() (out string, err error) {
	b, err := exec.Command("pg_dump", "-s", "--dbname="+dbUri.String(), "--schema=public").Output()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Generic DB functions
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// Generic DB String
////////////////////////////////////////////////////////////////////////////////

func GetStringRows(db *sql.DB, query string, args ...interface{}) ([][]string, error) {
	var res [][]string
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	colNames, err := rows.Columns()
	for rows.Next() {
		//see http://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
		//why do I have to fight with the type system again?
		readCols := make([]interface{}, len(colNames))
		writeCols := make([]string, len(colNames))
		for i, _ := range writeCols {
			readCols[i] = &writeCols[i]
		}
		if err := rows.Scan(readCols...); err != nil {
			return nil, err
		}
		res = append(res, writeCols)
	}
	return res, nil
}

func GetStringRow(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	ss, err := GetStringRows(db, query, args...)
	if err != nil {
		return nil, err
	}
	if len(ss) == 0 {
		return []string{}, nil
	} else if len(ss) == 1 {
		return ss[0], nil
	} else {
		return nil, fmt.Errorf("GetStringRow error: fetched %d results, expected 0 or 1", len(ss))
	}
}

func GetStringColumn(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	ss, err := GetStringRows(db, query, args...)
	if err != nil {
		return nil, err
	}
	if len(ss) == 0 {
		return []string{}, nil
	} else if len(ss[0]) != 1 {
		return nil, fmt.Errorf("GetColumn error: query returns %d columns, expected 1", len(ss[0]))
	} else {
		var res []string
		for _, row := range ss {
			res = append(res, row[0])
		}
		return res, nil
	}
}

func GetStringValue(db *sql.DB, query string, args ...interface{}) (string, error) {
	s, err := GetStringRow(db, query, args...)
	if err != nil {
		return "", err
	}
	if len(s) != 1 {
		return "", fmt.Errorf("GetValue error: query returned %d items, expected 1", len(s))
	}
	return s[0], nil
}

////////////////////////////////////////////////////////////////////////////////
// Generic DB Int
////////////////////////////////////////////////////////////////////////////////

func GetIntRows(db *sql.DB, query string, args ...interface{}) ([][]int, error) {
	var res [][]int
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	colNames, err := rows.Columns()
	for rows.Next() {
		//see http://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
		//why do I have to fight with the type system again?
		readCols := make([]interface{}, len(colNames))
		writeCols := make([]int, len(colNames))
		for i, _ := range writeCols {
			readCols[i] = &writeCols[i]
		}
		if err := rows.Scan(readCols...); err != nil {
			return nil, err
		}
		res = append(res, writeCols)
	}
	return res, nil
}

func GetIntRow(db *sql.DB, query string, args ...interface{}) ([]int, error) {
	ss, err := GetIntRows(db, query, args...)
	if err != nil {
		return nil, err
	}
	if len(ss) == 0 {
		return []int{}, nil
	} else if len(ss) == 1 {
		return ss[0], nil
	} else {
		return nil, fmt.Errorf("GetIntRow error: fetched %d results, expected 0 or 1", len(ss))
	}
}

func GetIntColumn(db *sql.DB, query string, args ...interface{}) ([]int, error) {
	ss, err := GetIntRows(db, query, args...)
	if err != nil {
		return nil, err
	}
	if len(ss) == 0 {
		return []int{}, nil
	} else if len(ss[0]) != 1 {
		return nil, fmt.Errorf("GetColumn error: query returns %d columns, expected 1", len(ss[0]))
	} else {
		var res []int
		for _, row := range ss {
			res = append(res, row[0])
		}
		return res, nil
	}
}

func GetIntValue(db *sql.DB, query string, args ...interface{}) (int, error) {
	s, err := GetIntRow(db, query, args...)
	if err != nil {
		return 0, err
	}
	if len(s) != 1 {
		return 0, fmt.Errorf("GetValue error: query returned %d items, expected 1", len(s))
	}
	return s[0], nil
}
