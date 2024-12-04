package paging

import (
	"fmt"
	"testing"
)

func TestPageing(t *testing.T) {
	var testcases = []struct {
		req   PageRequest
		count int64
		want  PageResponse
	}{
		{
			req:   PageRequest{PageSize: 10, PageNumber: 1},
			count: 100,
			want:  PageResponse{PageNumber: 1, PageSize: 10, PageCount: 10, Count: 100},
		},
		{
			req:   PageRequest{PageSize: 10, PageNumber: 2},
			count: 100,
			want:  PageResponse{PageNumber: 2, PageSize: 10, PageCount: 10, Count: 100},
		},
		{
			req:   PageRequest{PageSize: 10, PageNumber: 2},
			count: 95,
			want:  PageResponse{PageNumber: 2, PageSize: 10, PageCount: 10, Count: 95},
		},
	}

	for _, tt := range testcases {
		t.Run(fmt.Sprintf("PageSize:%d, PageNumber:%d", tt.req.PageSize, tt.req.PageNumber), func(t *testing.T) {
			resp := tt.req.Response(tt.count)
			if resp != tt.want {
				t.Errorf("got %v, want %v", resp, tt.want)
			}
		})
	}
}

func ExamplePageRequest_Limit() {
	req := PageRequest{
		PageSize:   10,
		PageNumber: 3,
	}
	limit, offset := req.Limit()
	count := 95
	resp := req.Response(int64(count))
	// Output: 10 20
	// 10 95 10 3
	fmt.Println(limit, offset)
	fmt.Println(resp.PageCount, resp.Count, resp.PageSize, resp.PageNumber)

}
