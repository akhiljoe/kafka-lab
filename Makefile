PY := .venv/bin/python
UVICORN := .venv/bin/uvicorn

.PHONY: up down setup lab stop-lab status shop inspector worker logs topics groups describe-group tail-orders reset-group wipe stop-broker start-broker

up:            ## start kafka, kafka-ui, exporter, prometheus, grafana
	docker compose up -d

down:
	docker compose down

setup:         ## install python deps and create topics
	$(PY) -m pip install -r requirements.txt
	$(PY) scripts/create_topics.py

lab:           ## start shop + inspector in the background, auto-restart on crash, logs in .run/
	@mkdir -p .run
	@$(MAKE) -s stop-lab
	@nohup sh -c 'while true; do $(UVICORN) services.shop.app:app --port 8000; echo "shop exited, restarting in 2s"; sleep 2; done' > .run/shop.log 2>&1 &
	@nohup sh -c 'while true; do $(UVICORN) services.inspector.app:app --port 8080; echo "inspector exited, restarting in 2s"; sleep 2; done' > .run/inspector.log 2>&1 &
	@sleep 4; $(MAKE) -s status

stop-lab:      ## stop the background shop + inspector
	@pkill -f 'services.shop.app:app' 2>/dev/null || true
	@pkill -f 'services.inspector.app:app' 2>/dev/null || true

status:        ## is everything up?
	@docker compose ps --format '  {{.Name}}: {{.Status}}' 2>/dev/null || echo "  docker: not running"
	@curl -s -o /dev/null -w '  Lab        http://localhost:8000  %{http_code}\n' http://localhost:8000/ || true
	@curl -s -o /dev/null -w '  Inspector  http://localhost:8080  %{http_code}\n' http://localhost:8080/ || true

shop:          ## shop in the foreground (one terminal), restarts itself if it crashes
	while true; do $(UVICORN) services.shop.app:app --port 8000; echo "shop exited, restarting in 2s"; sleep 2; done

inspector:     ## inspector in the foreground (one terminal), restarts itself if it crashes
	while true; do $(UVICORN) services.inspector.app:app --port 8080; echo "inspector exited, restarting in 2s"; sleep 2; done

worker:        ## one processor from the terminal: make worker NAME=worker-a DELAY=0.2 ARGS="--session-timeout-ms 10000"
	$(PY) -m services.processor.worker --name $(or $(NAME),worker-cli) --delay $(or $(DELAY),0.2) $(ARGS)

wipe:          ## start fresh: stop workers, delete and recreate topics so offsets begin at 0 (shop must be running)
	curl -s -X POST localhost:8000/api/wipe && echo

stop-broker:   ## make stop-broker N=2  (then watch leaders move in the Inspector)
	docker stop kafka-$(or $(N),2)

start-broker:  ## make start-broker N=2
	docker start kafka-$(or $(N),2)

logs:
	docker compose logs -f --tail=100 kafka-1 kafka-2 kafka-3

# ---- the CLI an engineer reaches for in production -------------------------
topics:
	docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe

groups:
	docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --list

describe-group: ## make describe-group GROUP=order-processor
	docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --describe --group $(or $(GROUP),order-processor)

tail-orders:
	docker exec kafka-1 kafka-console-consumer --bootstrap-server kafka-1:29092 --topic orders --property print.key=true --property print.partition=true --property print.offset=true

reset-group:   ## rewind a group to the beginning (all workers in it must be stopped)
	docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --group $(or $(GROUP),order-processor) --topic orders --reset-offsets --to-earliest --execute
