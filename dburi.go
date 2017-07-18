package dburi

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"

	"github.com/lib/pq"
)

type DbUri struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
}

// if password is empty take password from env var
func NewDbUri(host, port, name, user, password string) (dbUri *DbUri, err error) {
	if password == "" {
		password = os.Getenv("PGPASSWORD")
		if password == "" {
			return nil, fmt.Errorf("PGPASSWORD not present in env vars")
		}
	}
	return &DbUri{host, port, user, password, name}, nil
}

// stringify to dbname URI
func (dbUri *DbUri) String() string {
	uri := "postgresql://" + dbUri.User + ":" + dbUri.Password + "@" + dbUri.Host + ":" + dbUri.Port + "/" + dbUri.Name + "?"
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
	mnt, err := NewDbUri(dbUri.Host, dbUri.Port, "postgres", dbUri.User, "")
	if err != nil {
		return nil, err
	}
	return mnt.Open()
}

func (dbUri *DbUri) CreateDb(dbname string) (err error) {
	mntDb, err := dbUri.OpenMaintenanceDb()
	defer mntDb.Close()
	if err != nil {
		return err
	}
	_, err = mntDb.Exec("create database " + dbname)
	return err
}

func (dbUri *DbUri) DropDb(dbname string) (err error) {
	mntDb, err := dbUri.OpenMaintenanceDb()
	defer mntDb.Close()
	if err != nil {
		return err
	}
	_, err = mntDb.Exec("drop database if exists " + dbname)
	return err
}

func (dbUri *DbUri) KillPglogicalBackends() (err error) {
	mntDb, err := dbUri.OpenMaintenanceDb()
	defer mntDb.Close()
	if err != nil {
		return err
	}
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
