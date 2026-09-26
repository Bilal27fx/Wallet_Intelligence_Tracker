COMPOSE := docker compose
EXEC := $(COMPOSE) exec web

.PHONY: up down logs shell migrate makemigrations test lint fmt

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f $(s)

shell:
	$(EXEC) python manage.py shell

migrate:
	$(EXEC) python manage.py migrate

makemigrations:
	$(EXEC) python manage.py makemigrations

test:
	$(EXEC) pytest $(args)

lint:
	$(EXEC) ruff check .
	$(EXEC) ruff format --check .

fmt:
	$(EXEC) ruff check --fix .
	$(EXEC) ruff format .
