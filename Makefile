.PHONY: up down api worker test logs clean

up:
	docker compose up --build -d

down:
	docker compose down

api:
	uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

worker:
	WORKER_ID=worker-local python worker.py

test:
	pytest tests/ -v -s

test-exactly-once:
	pytest tests/test_exactly_once.py -v -s -k "test_exactly_once_single_run"

test-all-runs:
	pytest tests/test_exactly_once.py -v -s

logs:
	docker compose logs -f --tail=50

clean:
	docker compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# Quick demo: schedule a webhook job
demo-webhook:
	curl -s -X POST http://localhost:8000/jobs \
	  -H "Content-Type: application/json" \
	  -d '{"name":"demo-webhook","job_type":"webhook","payload":{"url":"https://httpbin.org/post","body":{"hello":"aether"}},"delay_seconds":2}' | python -m json.tool

# Schedule an LLM task
demo-llm:
	curl -s -X POST http://localhost:8000/jobs \
	  -H "Content-Type: application/json" \
	  -d '{"name":"demo-llm","job_type":"llm_task","payload":{"prompt":"Summarize distributed systems in 2 sentences.","callback_url":"https://httpbin.org/post"},"delay_seconds":1}' | python -m json.tool

# Schedule a chain job
demo-chain:
	curl -s -X POST http://localhost:8000/jobs \
	  -H "Content-Type: application/json" \
	  -d '{"name":"demo-chain","job_type":"chain","payload":{"steps":[{"job_type":"llm_task","payload":{"prompt":"Generate 3 keywords about distributed systems.","callback_url":"https://httpbin.org/post"}},{"job_type":"webhook","payload":{"url":"https://httpbin.org/post","body":{}}}],"callback_url":"https://httpbin.org/post"},"delay_seconds":1}' | python -m json.tool

metrics:
	curl -s http://localhost:8000/metrics | python -m json.tool

health:
	curl -s http://localhost:8000/health | python -m json.tool

# Load tests
load-test:
	python scripts/load_test.py --jobs 100 --concurrency 10 --type webhook

load-test-heavy:
	python scripts/load_test.py --jobs 500 --concurrency 20 --type webhook

load-test-llm:
	python scripts/load_test.py --jobs 50 --concurrency 5 --type llm_task
