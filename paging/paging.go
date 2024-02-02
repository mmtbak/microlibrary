package paging

import (
	"math"
)

// PageRequest 分页请求
type PageRequest struct {
	// PageSize 请求的分页上限
	PageSize int `json:"page_size,omitempty" validate:"gt=0"`
	// PageNumber 请求的分页页号
	PageNumber int `json:"page_number,omitempty" validate:"gt=0"`
}

// DefaultPageRequest 默认请求分页大小
var DefaultPageRequest = PageRequest{
	PageSize:   50,
	PageNumber: 1,
}

// PageResponse 分页返回
type PageResponse struct {
	// Count 条目总数
	Count int64 `json:"count"`
	// 每页数量上限
	PageSize int `json:"page_size"`
	// 当前页号
	PageNumber int `json:"page_number"`
	// 分页总页数
	PageCount int `json:"page_count"`
}

// Limit  根据分页请求计算分页查询的起始偏移量
// limit 单页限制数量
// offset 起始偏移量
func (req PageRequest) Limit() (limit int, offset int) {
	limit = req.PageSize
	offset = (req.PageNumber - 1) * req.PageSize
	return limit, offset
}

// Count  根据分页请求计算 总数
func (req PageRequest) Count() int64 {
	return int64(req.PageNumber * req.PageSize)
}

// Response 根据总数计算出response分页信息
func (req PageRequest) Response(count int64) PageResponse {
	PageCount := int(math.Ceil(float64(count) / float64(req.PageSize)))
	resp := PageResponse{
		PageNumber: req.PageNumber,
		PageSize:   req.PageSize,
		PageCount:  PageCount,
		Count:      count,
	}
	return resp
}
