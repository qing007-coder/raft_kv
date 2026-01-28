start cmd /c "go run cmd/server/main.go -addr :8081 -config ./config/node1.yaml"
start cmd /c "go run cmd/server/main.go -addr :8082 -config ./config/node2.yaml"
start cmd /c "go run cmd/server/main.go -addr :8083 -config ./config/node3.yaml"