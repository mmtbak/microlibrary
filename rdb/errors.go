package rdb

import "gorm.io/gorm"

// IsRecordNotFound ...
// @Description: 判断err是否是"没有该记录"
func IsRecordNotFound(err error) bool {
	return err == gorm.ErrRecordNotFound
}
