package rdb

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"gopkg.in/go-playground/assert.v1"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type MockTable struct {
	ID   int `gorm:"primaryKey"`
	Name string
	Age  int
}

func TestSession(t *testing.T) {
	var err error
	db, mock, err := sqlmock.New()
	assert.Equal(t, err, nil)
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))

	// mock end statement
	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.Equal(t, err, nil)
	// mock sql "select version()"

	client := &DBClient{}
	client = client.WithDB(gormDB)

	// mock statement
	// insert success
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `mock_tables`").WillReturnResult(sqlmock.NewResult(10, 1))
	mock.ExpectCommit()

	tx1, err := InsertMockData(nil, client)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, tx1.ConnPool, nil)

	// update success
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `mock_tables`").WithArgs("bob", 20, 10).WillReturnResult(sqlmock.NewResult(0, 1))
	// insert fail
	mock.ExpectExec("INSERT INTO `mock_tables`").WillReturnError(gorm.ErrDuplicatedKey)
	mock.ExpectCommit()

	tx := client.db.Begin()
	tx, maker := NewTxMaker(tx, client)
	defer maker.Close(&err)

	tx2, err := UpdateMockData(tx, client)
	assert.Equal(t, err, nil)
	tx3, err := InsertMockData(tx, client)
	assert.Equal(t, err, gorm.ErrDuplicatedKey)
	assert.Equal(t, &tx2.ConnPool, &tx3.ConnPool)

	txf := ForUpdate(tx)
	assert.NotEqual(t, txf.ConnPool, tx.ConnPool)
	tx = tx.Commit()

	// Check if tx is committed
	err = tx.Error

	assert.Equal(t, err, nil)

	// Attempt to execute a DB operation after commit
	err = tx.Create(&MockTable{Name: "test", Age: 25}).Error
	assert.Equal(t, err, sql.ErrTxDone) // Should return an error as the transaction is already committed
}

func InsertMockData(tx Tx, client *DBClient) (Tx, error) {
	var err error
	tx, maker := NewTxMaker(tx, client)
	defer maker.Close(&err)

	data := &MockTable{
		Name: "alice",
		Age:  18,
	}

	err = tx.Create(data).Error
	if err != nil {
		return tx, err
	}
	return tx, nil
}

func UpdateMockData(tx Tx, client *DBClient) (Tx, error) {
	var err error
	tx, maker := NewTxMaker(tx, client)
	defer maker.Close(&err)

	id := 10
	updatedata := &MockTable{
		Name: "bob",
		Age:  20,
	}

	tx = tx.Model(&MockTable{}).Where(&MockTable{ID: id}).Updates(updatedata)

	err = tx.Error
	if err != nil {
		return tx, err
	}
	return tx, nil
}
