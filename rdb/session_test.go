package rdb

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"gopkg.in/go-playground/assert.v1"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type MockFactory struct {
	client *DBClient
}

func (f MockFactory) NewSession() Session {
	return f.client.NewSession()
}

type MockTable struct {
	ID   int `gorm:"primaryKey"`
	Name string
	Age  int
}

func TestSession(t *testing.T) {
	var err error
	db, mock, err := sqlmock.New()
	assert.Equal(t, err, nil)
	// mock statement
	// mock sql "select version()"
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `mock_tables`").WillReturnResult(sqlmock.NewResult(10, 1))
	mock.ExpectExec("UPDATE `mock_tables`").WithArgs("bob", 20, 10).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	// mock end statement
	gormdb, err := gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.Equal(t, err, nil)

	client := &DBClient{}
	client = client.WithDB(gormdb)
	factory := MockFactory{
		client: client,
	}
	tx, maker := NewSessionMaker(nil, factory)
	defer maker.Close(&err)
	newdata := &MockTable{
		Name: "alice",
		Age:  18,
	}
	tx = tx.Create(newdata)
	err = tx.Error
	assert.Equal(t, err, nil)
	updateitem := &MockTable{
		Name: "bob",
		Age:  20,
	}
	tx = tx.Model(&MockTable{}).Where(&MockTable{ID: newdata.ID}).Updates(updateitem)
	err = tx.Error
	assert.Equal(t, err, nil)
	tx.Commit()
}
