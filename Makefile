.PHONY: up down clean logs ps load test tidy status urls

up:            ## build and start both regions + router + Prometheus + Grafana
	docker compose up -d --build
	@$(MAKE) --no-print-directory urls

down:          ## stop everything (keeps data)
	docker compose --profile load down

clean:         ## stop everything and delete all volumes
	docker compose --profile load down -v --remove-orphans

logs:
	docker compose logs -f service-a service-b

ps status:
	@./chaos/chaos.sh status

load:          ## continuous k6 load (WRITE_RATE, READ_RATE, VERIFY_RATE, DURATION are overridable)
	docker compose --profile load run --rm k6

test:
	go test -race ./...

tidy:          ## resolve go.mod / go.sum (run once, needs network)
	go mod tidy

urls:
	@echo "Grafana    http://localhost:3000   (dashboard 'Multi-Region · Customers')"
	@echo "Router     http://localhost:8080   active-active | http://localhost:8090 failover (A primary, B backup)"
	@echo "HAProxy    http://localhost:8404/stats"
	@echo "Region A   http://localhost:18081  | Region B http://localhost:18082   (direct, bypass the router)"
	@echo "Prometheus http://localhost:9090"
