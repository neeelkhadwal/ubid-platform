.PHONY: install setup generate run-pipeline serve clean test

install:
	pip install -r requirements.txt

setup: install
	cp -n .env.example .env || true
	python scripts/run_pipeline.py --setup-only

generate:
	python scripts/run_pipeline.py --generate-only

run-pipeline:
	python scripts/run_pipeline.py

serve:
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

demo: run-pipeline serve

docker-up:
	docker compose up -d db
	sleep 3
	DATABASE_URL=postgresql://ubid:ubid_secret@localhost:5432/ubid_db python scripts/run_pipeline.py
	docker compose up api

clean:
	rm -f ubid.db
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

reset: clean
	$(MAKE) run-pipeline
