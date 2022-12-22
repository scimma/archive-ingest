##
## The "push" target assumes that AWS credentials 
## have been configured.
##
CNT_NAME := scimma/housekeeping
REGION   := us-west-2
AWSREG   := 585193511743.dkr.ecr.us-west-2.amazonaws.com

TAG      := $(shell git log -1 --pretty=%H || echo MISSING )
CNT_IMG  := $(CNT_NAME):$(TAG)
CNT_LTST := $(CNT_NAME):latest
SHELL    += -x
.PHONY: set-release-tags push test clean all container

all: container

print-%  : ; @echo $* = $($*)

container: Dockerfile
#	#@if [ ! -z "$$(git status --porcelain)" ]; then echo "Directory is not clean. Commit your changes."; exit 1; fi
#	DOCKER_BUILDKIT=1 docker build --platform linux/amd64 --secret id=aws,src=/Users/donaldp/.aws/credentials -f $< -t $(CNT_IMG) .
	docker build --platform linux/amd64  -f $< -t $(CNT_IMG) .  
#	docker build                        -f $< -t $(CNT_IMG) .  
	docker tag $(CNT_IMG) $(CNT_LTST)

set-release-tags:
	@$(eval RELEASE_TAG := $(shell echo $(GITHUB_REF) | awk -F- '{print $$2}'))
	@echo RELEASE_TAG =  $(RELEASE_TAG)
	@$(eval MAJOR_TAG   := $(shell echo $(RELEASE_TAG) | awk -F. '{print $$1}'))
	@echo MAJOR_TAG = $(MAJOR_TAG)
	@$(eval MINOR_TAG   := $(shell echo $(RELEASE_TAG) | awk -F. '{print $$2}'))
	@echo MINOR_TAG = $(MINOR_TAG)
#       tags of the form "number.number.number"
	@(echo $(RELEASE_TAG) | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$$' > /dev/null ) || (echo Bad release tag: $(RELEASE_TAG) && exit 1)

push-aws: set-release-tags push-local
	/usr/local/bin/aws ecr get-login-password | docker login --username AWS --password-stdin $(AWSREG)
	docker push $(AWSREG)/$(CNT_NAME):$(RELEASE_TAG)
	docker push $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG)
	docker push $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG).$(MINOR_TAG)
#	@eval "echo $$DPASS" | docker login --username $(DBUILDER) --password-stdin
#	docker tag $(CNT_IMG) $(CNT_NAME):$(RELEASE_TAG)
#	docker tag $(CNT_IMG) $(CNT_NAME):$(MAJOR_TAG)
#	docker tag $(CNT_IMG) $(CNT_NAME):$(MAJOR_TAG).$(MINOR_TAG)
#	docker push $(CNT_NAME):$(RELEASE_TAG)
#	docker push $(CNT_NAME):$(MAJOR_TAG)
#	docker push $(CNT_NAME):$(MAJOR_TAG).$(MINOR_TAG)
#	rm -f $(HOME)/.docker/config.json
test:

push-local: set-release-tags

	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(RELEASE_TAG)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG).$(MINOR_TAG)