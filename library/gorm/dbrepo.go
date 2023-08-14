package gorm

import (
	"gorm.io/gorm"
)

// DBBaseRepository  releated database base repository
type DBBaseRepository struct {
	dbclient *DBClient
	tables   []interface{}
}

// NewDBBaseRepository new repo
func NewDBBaseRepository(dbclient *DBClient, tables []interface{}) DBBaseRepository {
	return DBBaseRepository{
		dbclient: dbclient,
		tables:   tables,
	}
}

// SyncTables sync tables to db
func (repo *DBBaseRepository) SyncTables() error {
	return repo.dbclient.DB().Set("gorm:table_options",
		"CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci").AutoMigrate(repo.tables...)
}

// DropTables drop  tables from  db
func (repo *DBBaseRepository) DropTables() error {
	return repo.dbclient.DB().Migrator().DropTable(repo.tables...)
}

// NewSessionMaker proxy session maker
func (repo *DBBaseRepository) NewSessionMaker(session Session,
) (Session, *SesionMaker) {
	return NewSessionMaker(session, repo.dbclient)
}

// IsRecordNotFound @Description: 判断err是否是"没有该记录"
func (repo *DBBaseRepository) IsRecordNotFound(err error) bool {
	return err == gorm.ErrRecordNotFound
}
