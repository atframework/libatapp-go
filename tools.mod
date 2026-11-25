module github.com/atframework/libatapp-go/tools

go 1.27.4

tool (
	github.com/atframework/atframe-utils-go/buildkit/cmd/atbuild
	github.com/atframework/atframe-utils-go/protoc-gen-mutable
	google.golang.org/protobuf/cmd/protoc-gen-go
)

require (
	github.com/atframework/atframe-utils-go v1.0.5-0.20260416024202-66c04636f055
	github.com/atframework/atframe-utils-go/buildkit v0.1.0
	google.golang.org/protobuf v1.36.11
)
