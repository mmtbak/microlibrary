package rdb

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Tx session rename.
type Tx = *gorm.DB

// ForUpdate create session with for update lock.
func ForUpdate(tx Tx) Tx {
	tx = tx.Clauses(clause.Locking{Strength: "UPDATE"})
	return tx
}

// TxMaker ....
type TxMaker struct {
	origintx Tx
	factory  TxFactory
	newtx    Tx
}

// TxFactory a factory can create new transaction
type TxFactory interface {
	// NewTx create a new transaction
	NewTx() Tx
}

// NewTxMaker new transaction maker.
/*
if ori is nil, create a new transaction by factory
if ori is not nil, use the ori transaction
function return the new transaction and the txmaker

txmaker is used to manage the transaction lifecycle
when the function ends, txmaker.Close(*errp) will be called, the transaction will be committed or rolled back
if errp is nil, the transaction will be committed
if errp is not nil, the transaction will be rolled back

you can use TxMaker to pass the transaction between functions.

*/
func NewTxMaker(ori Tx, factory TxFactory) (Tx, *TxMaker) {
	var newtx Tx
	if ori != nil {
		newtx = ori
	} else if factory != nil {
		newtx = factory.NewTx()
		newtx.Begin()
	} else {
		newtx = nil
	}
	return newtx, &TxMaker{
		origintx: ori,
		factory:  factory,
		newtx:    newtx,
	}
}

// Close close transaction
/*
if origin tx != nil, it means the tx is used by upper function, so we don't need to do anything
if origin tx is nil, it means the tx is a new tx created by factory, we need to handle it.
if errp is nil, the transaction will be committed
if errp is not nil, the transaction will be rolled back
*/
func (maker *TxMaker) Close(errp *error) error {
	// if origin tx is not nil, it means the tx is used by upper function, so we don't need to do anything
	if maker.origintx != nil {
		return nil
	}
	// if newtx.ConnPool == nil, it means the tx is not a valid tx, we don't need to do anything
	if maker.newtx.ConnPool == nil {
		return nil
	}
	// if err is nil, the transaction will be committed
	if errp != nil && *errp == nil {
		return maker.newtx.Commit().Error
	}
	// if errp is not nil, the transaction will be rolled back
	return maker.newtx.Rollback().Error
}
