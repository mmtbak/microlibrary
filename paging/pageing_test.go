package paging

import (
	"fmt"
	"testing"
)

func TestPageing(t *testing.T) {

	req := DefaultPageRequest
	resp := req.Response(103)
	fmt.Println(resp)

}
