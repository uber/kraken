package blobserver

type contextKey int

const (
	ctxKeyDigest contextKey = iota
	ctxKeyUploadUUID
	ctxKeyBlobReader
	ctxKeyLocalStore
	ctxKeyStartByte
	ctxKeyEndByte
	ctxKeyHashConfig
	ctxKeyHashState
)
