package gateway

import "errors"

var (
	errProviderExists  = errors.New("厂商编号已存在，请使用编辑操作")
	errProviderMissing = errors.New("厂商不存在，请刷新列表")
)
