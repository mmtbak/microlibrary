package rdb

import "gorm.io/gorm"

// Session session rename
type Session = *gorm.DB

// SesionMaker ....
type SesionMaker struct {
	originsession Session
	factory       SessionFactory
	newsession    Session
}

// SessionFactory ....
type SessionFactory interface {
	NewSession() Session
}

// NewSessionMaker new session maker
func NewSessionMaker(ori Session, factory SessionFactory) (Session, *SesionMaker) {

	var newsession Session
	if ori == nil {
		newsession = factory.NewSession()
		newsession.Begin()
	} else {
		newsession = ori
	}
	return newsession, &SesionMaker{
		originsession: ori,
		factory:       factory,
		newsession:    newsession,
	}

}

// Close 关闭session
/*
如果origin session != nil, 说明是使用上游session, 不管执行异常还是什么， 不处理
如果origin session 为nil ,说明是当前新创建的session， 需要对其做后续处理。
	如果执行正常， 需要commit
	如果执行异常，需要rollback最后close
*/
func (s *SesionMaker) Close(errp *error) error {

	// 如果是上游传入的， 不做处理，直接返回
	if s.originsession != nil {
		return nil
	}
	// 如果是本地新创建的，根据执行结果判断
	var err2 error
	// 如果执行异常，rollback，如果执行正常，commit
	if errp != nil && *errp != nil {
		err2 = s.newsession.Rollback().Error
	} else {
		err2 = s.newsession.Commit().Error
	}
	if err2 != nil {
		return err2
	}
	// s.newsession.Close()

	return nil
}
