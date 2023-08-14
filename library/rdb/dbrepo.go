package rdb

import (
	"gorm.io/gorm"
)

// DBRepository  releated database base repository
type DBRepository struct {
	Client *DBClient
	tables []interface{}
}

// NewDBBaseRepository new repo
func NewDBBaseRepository(dbclient *DBClient, tables []interface{}) DBRepository {
	return DBRepository{
		Client: dbclient,
		tables: tables,
	}
}

// SyncTables sync tables to db
func (repo *DBRepository) SyncTables() error {
	return repo.Client.SyncTables(repo.tables)
}

// DropTables drop  tables from  db
func (repo *DBRepository) DropTables() error {
	return repo.Client.DB().Migrator().DropTable(repo.tables...)
}

// NewSessionMaker proxy session maker
func (repo *DBRepository) NewSessionMaker(session Session,
) (Session, *SesionMaker) {
	return NewSessionMaker(session, repo.Client)
}

// IsRecordNotFound @Description: 判断err是否是"没有该记录"
func (repo *DBRepository) IsRecordNotFound(err error) bool {
	return err == gorm.ErrRecordNotFound
}

func (repo *DBRepository) GetClient() *DBClient {
	return repo.Client
}
