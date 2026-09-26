COMPOSE := docker compose
EXEC := $(COMPOSE) exec web

.PHONY: up down logs shell migrate makemigrations test lint fmt startapp

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

startapp:
	@test -n "$(name)" || (echo "Usage: make startapp name=<app>" && exit 1)
	$(EXEC) sh -c "mkdir -p apps/$(name) && python manage.py startapp --template config/app_template $(name) apps/$(name)"
	@echo "Ensuite : ajouter \"apps.$(name)\" à INSTALLED_APPS et path(\"api/$(name)/\", include(\"apps.$(name).urls\")) à config/urls.py"
