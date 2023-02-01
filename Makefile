##
##  usege
##   make container TAG=<n.n.n>
##   make push      TAG=<n.n.n>
## Make
##   version expects a semantic version (e.g number.number.number.
##     following a pattern I cannot justify, continers tagged with
##     just the major, just the major.mono and full verions are pushed
##     to AWS.  Terrafrom is used to select the version look there to
##     figure out what's to be run
##
##   devel pushes a tag named 0.0.0 
##
##  this fle assumees AWS credentials have been configured.
##
CNT_NAME := scimma/archive_ingest
REGION   := us-west-2
AWSREG   := 585193511743.dkr.ecr.us-west-2.amazonaws.com
#TAG      := $(shell git log -1 --pretty=%H || echo MISSING )
TAG      := "please supply TAG on command line of the form n.n.n"
RELEASE_TAG := $(TAG)
MAJOR_TAG   := $(shell V=$(TAG) ; echo $${V%%.*} )
MINOR_TAG   := $(shell V=$(TAG) ; echo $${V%.*} )

ifeq ($(TAG),0.0.0)
   HOP   := --build-arg  HOP=hop-devel
   STORE := --build-arg  STORE=S3-dev
   DB    := --build-arg  DB=aws-dev-db
else
   HOP   := --build-arg  HOP=hop-prod
   STORE := --build-arg  STORE=S3-prod
   DB    := --build-arg  DB=aws-prod-db
endif

CNT_IMG  := $(CNT_NAME)$(TAG)
CNT_LTST := $(CNT_NAME):latest
SHELL    += -x
.PHONY: push all container

all: container  push

print-%  : ; @echo $* = $($*)

container: Dockerfile
	@(echo $(RELEASE_TAG) | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$$' > /dev/null ) || (echo Bad release tag: $(RELEASE_TAG) && exit 1)
	docker build $(DB) $(STORE) $(HOP) --platform linux/amd64  -f $< -t $(CNT_IMG) .  
	docker tag $(CNT_IMG) $(CNT_LTST)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(RELEASE_TAG)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(MINOR_TAG)

push: 
	@(echo $(RELEASE_TAG) | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$$' > /dev/null ) || (echo Bad release tag: $(RELEASE_TAG) && exit 1)
	/usr/local/bin/aws ecr get-login-password | docker login --username AWS --password-stdin $(AWSREG)
	docker push $(AWSREG)/$(CNT_NAME):$(RELEASE_TAG)
	docker push $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG)
	docker push $(AWSREG)/$(CNT_NAME):$(MINOR_TAG)

