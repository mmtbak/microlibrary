package rdb

import "gorm.io/gorm"

// IsRecordNotFound ...
// @Description: 判断err是否是"没有该记录"
func IsErrRecordNotFound(err error) bool {
	return err == gorm.ErrRecordNotFound
}

// IsErrDuplicatedKey return true if the error is a duplicate key error
// @Description: 判断err是否是"主键冲突"
func IsErrDuplicatedKey(err error) bool {
	return err == gorm.ErrDuplicatedKey
}
