RELEASE ?= schema-v1.24.1
ACP_RELEASE_BASE_URL ?= https://github.com/agentclientprotocol/agent-client-protocol/releases/download

SCHEMA_DIR := schema
SCHEMA_JSON := $(SCHEMA_DIR)/schema.json
META_JSON := $(SCHEMA_DIR)/meta.json
GENERATED_SCHEMA := $(SCHEMA_DIR)/generated.go
SCHEMA_URL := $(ACP_RELEASE_BASE_URL)/$(RELEASE)/schema.unstable.json
META_URL := $(ACP_RELEASE_BASE_URL)/$(RELEASE)/meta.unstable.json

.PHONY: schema schema-download schema-generate schema-local test vet check

schema: schema-download schema-generate

schema-download:
	mkdir -p $(SCHEMA_DIR)
	curl -fsSL -o $(SCHEMA_JSON) $(SCHEMA_URL)
	curl -fsSL -o $(META_JSON) $(META_URL)

schema-generate:
	go run ./cmd/acp-schema-gen \
		-schema $(SCHEMA_JSON) \
		-meta $(META_JSON) \
		-out $(GENERATED_SCHEMA)

schema-local: schema-generate

test:
	go test ./...

vet:
	go vet ./...

check: test vet
