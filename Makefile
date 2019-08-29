DOCKER = docker-compose -f docker-compose.yml

.PHONY: build push pull down run test local-dev

build:
	$(DOCKER) build

push:
	$(DOCKER) push

pull:
	$(DOCKER) pull

down:
	$(DOCKER) down

run:
	$(DOCKER) up -d

