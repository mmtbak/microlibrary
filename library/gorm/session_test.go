package gorm

import (
	"fmt"
	"testing"
)

type MockFactory struct{}

func (f MockFactory) NewSession() Session {
	return nil
}

func CreateError() (int, error) {
	return 10, fmt.Errorf("create error ")
}

func TestSession(t *testing.T) {

	var err error
	factory := MockFactory{}
	newss, maker := NewSessionMaker(nil, factory)
	defer maker.Close(&err)
	err = fmt.Errorf("err1")
	fmt.Printf("err : %s,  p: %p \n", err, &err)
	fmt.Println(newss)
	err = fmt.Errorf("err2 ")
	fmt.Printf("err : %s,  p: %p \n", err, &err)
	id, err := CreateError()
	fmt.Printf("id : %d , err : %s  p : %p \n", id, err, &err)
	// err = fmt.Errorf("err3 ")
	// fmt.Printf("err : %s,  p: %p \n", err, &err)
}

func TestSessionNil(t *testing.T) {

	// var err error
	factory := MockFactory{}
	_, maker := NewSessionMaker(nil, factory)
	// defer maker.Close(&err)
	defer maker.Close(nil)
}
